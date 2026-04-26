"""
job_scraper.py
==============
Production-grade Incremental Web Scraper for Job Market Intelligence.
Targets: Wuzzuf.net + Forasna.com (fallback, more scraper-friendly)

Fixes over original:
  1. More robust Cloudflare bypass with realistic browser fingerprint
  2. Retry with exponential back-off + jitter  
  3. Wider CSS selectors with multiple fallbacks per field
  4. Session warm-up: visit homepage first before hitting deep URLs
  5. Rotating User-Agents to reduce fingerprinting
  6. XML namespace-safe sitemap parsing
  7. Optional proxy support via SCRAPER_PROXY env var
  8. Graceful keyboard interrupt (saves partial progress)
  9. Forasna.com support as an easier alternative

Author  : Academic Research Crawler
License : MIT (research use only)
Python  : 3.9+

Install:
    pip install requests beautifulsoup4 lxml cloudscraper fake-useragent
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import re
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import cloudscraper
import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("JobScraper")


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# ── Wuzzuf ──
# Primary sitemap (confirmed from wuzzuf.net/sitemap-https.xml in Google index)
WUZZUF_SITEMAP     = "https://wuzzuf.net/sitemap-https.xml"
# Fallback: scrape job URLs directly from search/listing pages (no sitemap needed)
WUZZUF_SEARCH_URL  = "https://wuzzuf.net/search/jobs?q=&l=egypt&start={page}"
WUZZUF_HOME        = "https://wuzzuf.net"
WUZZUF_URL_PATTERN = re.compile(r"https://wuzzuf\.net/jobs/p/[^<\s\"']+")

# ── Forasna (easier alternative, less anti-bot protection) ──
FORASNA_SITEMAP     = "https://www.forasna.com/sitemap.xml"
FORASNA_HOME        = "https://www.forasna.com"
FORASNA_URL_PATTERN = re.compile(r"https://www\.forasna\.com/jobs/[^<\s]+")

SLEEP_MIN       : float = 2.5   # ethical rate-limit floor
SLEEP_MAX       : float = 5.0   # ethical rate-limit ceiling
REQUEST_TIMEOUT : int   = 20
MAX_RETRIES     : int   = 4

DATASET_FILE = Path("dataset_jobs.json")
LOG_FILE     = Path("scraped_jobs_log.json")

# Rotating user-agents — reduces fingerprinting
USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
        "Gecko/20100101 Firefox/124.0"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
]

BASE_HEADERS = {
    "Accept"         : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection"     : "keep-alive",
    "DNT"            : "1",
}


# ─────────────────────────────────────────────────────────────────────────────
# Data Schema
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class JobRecord:
    job_id          : str
    source_url      : str
    job_title       : str
    company_name    : str
    location        : str
    job_description : str
    skills_or_tags  : list[str]
    scraped_at      : str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return asdict(self)


# ─────────────────────────────────────────────────────────────────────────────
# State Manager
# ─────────────────────────────────────────────────────────────────────────────
class StateManager:
    def __init__(self, log_path: Path = LOG_FILE) -> None:
        self.log_path = log_path
        self._seen: dict[str, str] = self._load()

    def _load(self) -> dict[str, str]:
        if self.log_path.exists():
            try:
                with self.log_path.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
                logger.info("State loaded — %d previously scraped URLs.", len(data))
                return data
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("State log unreadable: %s — starting fresh.", exc)
        return {}

    def is_seen(self, url: str) -> bool:
        return url in self._seen

    def mark_seen(self, url: str, job_id: str) -> None:
        self._seen[url] = job_id
        self._flush()

    def _flush(self) -> None:
        tmp = self.log_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._seen, fh, indent=2, ensure_ascii=False)
        tmp.replace(self.log_path)

    @property
    def seen_count(self) -> int:
        return len(self._seen)


# ─────────────────────────────────────────────────────────────────────────────
# Dataset Writer
# ─────────────────────────────────────────────────────────────────────────────
class DatasetWriter:
    def __init__(self, dataset_path: Path = DATASET_FILE) -> None:
        self.dataset_path = dataset_path
        self._records: list[dict] = self._load_existing()

    def _load_existing(self) -> list[dict]:
        if self.dataset_path.exists():
            try:
                with self.dataset_path.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
                logger.info("Dataset loaded — %d existing records.", len(data))
                return data
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("Dataset unreadable: %s — starting fresh.", exc)
        return []

    def append(self, record: JobRecord) -> None:
        self._records.append(record.to_dict())
        self._flush()
        logger.info(
            "✓ Saved  id=%-16s  title='%s'  company='%s'",
            record.job_id, record.job_title, record.company_name,
        )

    def _flush(self) -> None:
        tmp = self.dataset_path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(self._records, fh, indent=2, ensure_ascii=False)
        tmp.replace(self.dataset_path)

    @property
    def record_count(self) -> int:
        return len(self._records)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP Client — improved Cloudflare bypass
# ─────────────────────────────────────────────────────────────────────────────
class HttpClient:
    """
    Uses cloudscraper with:
      • rotating User-Agents
      • session warm-up (visit homepage before scraping deep URLs)
      • exponential back-off with jitter
      • optional proxy via SCRAPER_PROXY env var
    """

    def __init__(self, home_url: Optional[str] = None) -> None:
        self._home_url = home_url
        self._warmed   = False
        self.session   = self._new_session()

    # ── Session factory ──────────────────────────────────────────────────────
    def _new_session(self) -> cloudscraper.CloudScraper:
        scraper = cloudscraper.create_scraper(
            browser={
                "browser" : "chrome",
                "platform": "windows",
                "mobile"  : False,
                "desktop" : True,
            },
            delay=3,
        )
        scraper.headers.update({
            **BASE_HEADERS,
            "User-Agent": random.choice(USER_AGENTS),
        })

        # Optional proxy  (e.g. export SCRAPER_PROXY=http://user:pass@host:port)
        proxy = os.getenv("SCRAPER_PROXY")
        if proxy:
            scraper.proxies = {"http": proxy, "https": proxy}
            logger.info("Using proxy: %s", proxy)

        return scraper

    # ── Warm-up: visit homepage first so cookies are set ────────────────────
    def warm_up(self) -> bool:
        if self._warmed or not self._home_url:
            return True
        try:
            logger.info("Warming up session via %s …", self._home_url)
            r = self.session.get(self._home_url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                self._warmed = True
                logger.info("Session warmed up (status 200).")
                self.polite_sleep()
                return True
            logger.warning("Warm-up got status %d.", r.status_code)
        except Exception as exc:
            logger.warning("Warm-up failed: %s", exc)
        return False

    # ── GET with retry ────────────────────────────────────────────────────────
    def get(self, url: str, is_xml: bool = False) -> Optional[str]:
        headers: dict[str, str] = {}
        if is_xml:
            headers["Accept"] = "application/xml,text/xml;q=0.9,*/*;q=0.8"

        # rotate UA every request
        self.session.headers.update({"User-Agent": random.choice(USER_AGENTS)})

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.session.get(
                    url,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                )

                # Detect Cloudflare challenge page
                if "Just a moment" in r.text or (
                    "Cloudflare" in r.text and r.status_code in (403, 503)
                ):
                    logger.warning(
                        "Cloudflare challenge on attempt %d/%d for %s",
                        attempt, MAX_RETRIES, url,
                    )
                    # rebuild session + sleep before retry
                    self.session = self._new_session()
                    self._warmed = False
                    time.sleep(random.uniform(5, 10) * attempt)
                    continue

                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 60)) + random.randint(5, 15)
                    logger.warning("Rate-limited (429) — sleeping %ds.", wait)
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                return r.text

            except Exception as exc:
                back_off = (2 ** attempt) + random.uniform(0, 2)
                logger.warning(
                    "HTTP error (attempt %d/%d) %s — %s — retry in %.1fs",
                    attempt, MAX_RETRIES, url, exc, back_off,
                )
                time.sleep(back_off)

        logger.error("All %d attempts failed for %s", MAX_RETRIES, url)
        return None

    @staticmethod
    def polite_sleep() -> None:
        delay = random.uniform(SLEEP_MIN, SLEEP_MAX)
        logger.debug("Rate-limiting: sleeping %.2fs", delay)
        time.sleep(delay)

    def close(self) -> None:
        self.session.close()


# ─────────────────────────────────────────────────────────────────────────────
# Sitemap Fetcher
# ─────────────────────────────────────────────────────────────────────────────
class SitemapFetcher:
    def __init__(self, http_client: HttpClient, url_pattern: re.Pattern) -> None:
        self.http        = http_client
        self.url_pattern = url_pattern

    def _parse_locs(self, xml_text: str) -> list[str]:
        """Extract all <loc> values regardless of XML namespace."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.error("XML parse error: %s", exc)
            # fallback: regex extraction (handles malformed XML)
            return re.findall(r"<loc>\s*(https?://[^<\s]+)\s*</loc>", xml_text)

        ns = re.compile(r"\{[^}]+\}")
        return [
            ns.sub("", el.text or "").strip()
            for el in root.iter()
            if ns.sub("", el.tag) == "loc" and el.text
        ]

    def fetch_job_urls(self, sitemap_url: str, max_urls: Optional[int] = None) -> list[str]:
        logger.info("Fetching sitemap: %s", sitemap_url)
        xml_text = self.http.get(sitemap_url, is_xml=True)

        if not xml_text:
            logger.warning("Sitemap unavailable — falling back to search-page discovery.")
            return self._discover_via_search(
                "https://wuzzuf.net/search/jobs?q=&l=egypt&start={page}",
                max_urls=max_urls,
            )

        all_locs = self._parse_locs(xml_text)
        logger.info("Found %d <loc> entries in sitemap.", len(all_locs))

        job_urls: list[str] = []

        for loc in all_locs:
            if loc.endswith(".xml"):
                logger.info("Resolving nested sitemap: %s", loc)
                nested = self.http.get(loc, is_xml=True)
                self.http.polite_sleep()
                if nested:
                    job_urls.extend(
                        u for u in self._parse_locs(nested)
                        if self.url_pattern.match(u)
                    )
            elif self.url_pattern.match(loc):
                job_urls.append(loc)

        # deduplicate, preserve order
        seen: set[str] = set()
        unique = [u for u in job_urls if not (u in seen or seen.add(u))]  # type: ignore[func-returns-value]
        logger.info("Identified %d unique job URLs.", len(unique))

        if max_urls:
            unique = unique[:max_urls]
        return unique

    def _discover_via_search(
        self,
        search_url_template: str,
        max_urls: Optional[int] = None,
        max_pages: int = 20,
    ) -> list[str]:
        """
        Fallback URL discovery: paginate through wuzzuf search results
        and extract job links from the HTML.
        Used when the sitemap is unreachable.
        """
        logger.info("Search-page discovery mode (up to %d pages).", max_pages)
        job_urls: list[str] = []
        seen: set[str] = set()

        for page_num in range(max_pages):
            start = page_num * 10
            url = search_url_template.format(page=start)
            logger.info("  Scraping search page %d: %s", page_num + 1, url)

            html = self.http.get(url)
            if not html:
                logger.warning("  Search page %d failed — stopping.", page_num + 1)
                break

            soup = BeautifulSoup(html, "lxml")

            # Job links appear as <a href="/jobs/p/..."> in listing cards
            found = [
                "https://wuzzuf.net" + a["href"]
                if a["href"].startswith("/") else a["href"]
                for a in soup.select("a[href*='/jobs/p/']")
                if a.get("href") and self.url_pattern.search(
                    ("https://wuzzuf.net" if a["href"].startswith("/") else "") + a["href"]
                )
            ]

            new_found = [u for u in found if u not in seen]
            seen.update(new_found)
            job_urls.extend(new_found)
            logger.info("  Found %d new job URLs on page %d (total: %d)", len(new_found), page_num + 1, len(job_urls))

            if not new_found:
                logger.info("  No new URLs on page %d — end of results.", page_num + 1)
                break

            if max_urls and len(job_urls) >= max_urls:
                break

            self.http.polite_sleep()

        if max_urls:
            job_urls = job_urls[:max_urls]

        logger.info("Search-page discovery: %d total job URLs found.", len(job_urls))
        return job_urls


# ─────────────────────────────────────────────────────────────────────────────
# HTML Parsers — one per site
# ─────────────────────────────────────────────────────────────────────────────
class BaseParser:
    @staticmethod
    def _job_id(url: str) -> str:
        return hashlib.sha256(urlparse(url).path.encode()).hexdigest()[:16]

    @staticmethod
    def _first_text(soup: BeautifulSoup, selectors: list[str]) -> str:
        """Try selectors in order, return first non-empty text found."""
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(strip=True)
                if text:
                    return text
        return ""

    def parse(self, html: str, url: str) -> Optional[JobRecord]:
        raise NotImplementedError


class WuzzufParser(BaseParser):
    """
    Parser for wuzzuf.net job pages.
    Uses multiple CSS-selector fallbacks per field to handle site redesigns.
    """

    def parse(self, html: str, url: str) -> Optional[JobRecord]:
        try:
            soup = BeautifulSoup(html, "lxml")

            # ── Job Title ──────────────────────────────────────────────────
            job_title = self._first_text(soup, [
                "h1",
                "h1[class*='title']",
                "h1[class*='job']",
                ".css-f9rdnr",
            ])

            # ── Company Name ───────────────────────────────────────────────
            company_name = self._first_text(soup, [
                "a[href*='/jobs/careers/']",
                "a[href*='/companies/']",
                "a[data-analytics*='company']",
                ".css-17s97q8",
            ]).replace("-", "").strip() or "Unknown"

            # ── Location ───────────────────────────────────────────────────
            location = self._first_text(soup, [
                "span[class*='css-5wys0k']",
                "span[class*='location']",
                # generic: find <strong>Cairo</strong>-like pattern
                "strong + span",
                ".css-blbtif span",
            ]) or "Not specified"

            # ── Description ────────────────────────────────────────────────
            # Strategy: find largest text block containing "Description"
            desc_text = ""
            for tag in soup.find_all(["div", "section"]):
                text = tag.get_text(separator="\n", strip=True)
                if len(text) > len(desc_text) and (
                    "Description" in text or "Requirements" in text or "Responsibilities" in text
                ):
                    desc_text = text

            if not desc_text:
                desc_text = self._first_text(soup, [
                    "div[class*='css-g5wqkn']",
                    "section[class*='details']",
                    "div[class*='description']",
                    "main",
                ])

            # ── Skills / Tags ──────────────────────────────────────────────
            tag_links = soup.select("a[href*='/a/jobs-in'], a[href*='/tag/'], a[href*='/jobs/search']")
            skills = list(dict.fromkeys(
                t.get_text(strip=True).replace("·", "").strip()
                for t in tag_links if t.get_text(strip=True)
            ))

            if not job_title:
                logger.error("No job title found — skipping %s", url)
                Path("error_page.html").write_text(html, encoding="utf-8")
                return None

            return JobRecord(
                job_id=self._job_id(url),
                source_url=url,
                job_title=job_title,
                company_name=company_name,
                location=location,
                job_description=desc_text[:5000],  # cap at 5k chars
                skills_or_tags=skills,
            )

        except Exception as exc:
            logger.error("WuzzufParser crashed on %s: %s", url, exc)
            return None


class ForasnaParser(BaseParser):
    """
    Parser for forasna.com — Arabic/English job portal, far less anti-bot.
    Tends to use semantic HTML so selectors are more stable.
    """

    def parse(self, html: str, url: str) -> Optional[JobRecord]:
        try:
            soup = BeautifulSoup(html, "lxml")

            job_title = self._first_text(soup, [
                "h1.job-title",
                "h1",
                ".position-title",
            ])

            company_name = self._first_text(soup, [
                "a.company-name",
                ".company h2",
                "span.employer",
                "a[href*='/company/']",
            ]) or "Unknown"

            location = self._first_text(soup, [
                ".location",
                "span[class*='city']",
                "li[class*='location']",
            ]) or "Not specified"

            desc_el = soup.select_one(
                ".job-description, #job-description, article, .details"
            )
            desc_text = desc_el.get_text(separator="\n", strip=True) if desc_el else ""

            # Forasna uses <span class="tag"> for skills
            skills = [
                t.get_text(strip=True)
                for t in soup.select("span.tag, a.tag, .skills a, .requirements li")
                if t.get_text(strip=True)
            ]

            if not job_title:
                logger.error("No job title on %s — skipping.", url)
                return None

            return JobRecord(
                job_id=self._job_id(url),
                source_url=url,
                job_title=job_title,
                company_name=company_name,
                location=location,
                job_description=desc_text[:5000],
                skills_or_tags=skills,
            )

        except Exception as exc:
            logger.error("ForasnaParser crashed on %s: %s", url, exc)
            return None


# ─────────────────────────────────────────────────────────────────────────────
# Main Orchestrator
# ─────────────────────────────────────────────────────────────────────────────
class JobScraper:
    """
    Unified scraper supporting Wuzzuf and Forasna.

    Usage::

        # Wuzzuf (harder — needs good proxy or residential IP)
        with JobScraper(site="wuzzuf", max_jobs=100) as s:
            s.run()

        # Forasna (easier)
        with JobScraper(site="forasna", max_jobs=200) as s:
            s.run()
    """

    SITES = {
        "wuzzuf" : {
            "sitemap" : WUZZUF_SITEMAP,
            "home"    : WUZZUF_HOME,
            "pattern" : WUZZUF_URL_PATTERN,
            "parser"  : WuzzufParser,
        },
        "forasna": {
            "sitemap" : FORASNA_SITEMAP,
            "home"    : FORASNA_HOME,
            "pattern" : FORASNA_URL_PATTERN,
            "parser"  : ForasnaParser,
        },
    }

    def __init__(
        self,
        site       : str = "wuzzuf",
        max_jobs   : Optional[int] = None,
        dataset_path: Path = DATASET_FILE,
        log_path   : Path = LOG_FILE,
    ) -> None:
        if site not in self.SITES:
            raise ValueError(f"site must be one of {list(self.SITES)}")

        cfg = self.SITES[site]
        self.sitemap_url = cfg["sitemap"]
        self.parser      = cfg["parser"]()

        self.max_jobs       = max_jobs
        self.http_client    = HttpClient(home_url=cfg["home"])
        self.state_manager  = StateManager(log_path=log_path)
        self.dataset_writer = DatasetWriter(dataset_path=dataset_path)
        self.sitemap_fetcher = SitemapFetcher(self.http_client, cfg["pattern"])

        self._new_count     = 0
        self._skipped_count = 0
        self._error_count   = 0

        # Graceful interrupt handler — saves progress on Ctrl+C
        signal.signal(signal.SIGINT, self._handle_interrupt)

    def _handle_interrupt(self, *_) -> None:
        logger.info("Interrupted! Saving progress …")
        logger.info("Summary so far: %s", self._summary())
        sys.exit(0)

    def _should_stop(self) -> bool:
        return self.max_jobs is not None and self._new_count >= self.max_jobs

    def _scrape_one(self, url: str) -> bool:
        html = self.http_client.get(url)
        if html is None:
            self._error_count += 1
            return False

        record = self.parser.parse(html, url)
        if record is None:
            self._error_count += 1
            return False

        self.dataset_writer.append(record)
        self.state_manager.mark_seen(url, record.job_id)
        self._new_count += 1
        return True

    def run(self) -> dict[str, int]:
        logger.info("=" * 65)
        logger.info("JobScraper started")
        logger.info(
            "State: %d seen | Dataset: %d records",
            self.state_manager.seen_count,
            self.dataset_writer.record_count,
        )

        # Warm up session (visit homepage to get cookies)
        self.http_client.warm_up()

        # Discover URLs via sitemap
        job_urls = self.sitemap_fetcher.fetch_job_urls(self.sitemap_url)
        if not job_urls:
            logger.warning("No job URLs found — exiting.")
            return self._summary()

        # Incremental scrape loop
        total = len(job_urls)
        for idx, url in enumerate(job_urls, 1):

            if self._should_stop():
                logger.info("Reached max_jobs=%d — stopping.", self.max_jobs)
                break

            logger.info("[%d/%d] %s", idx, total, url)

            if self.state_manager.is_seen(url):
                logger.info("  → SKIP (already scraped)")
                self._skipped_count += 1
                continue

            ok = self._scrape_one(url)
            if ok:
                logger.info("  → OK  (dataset now has %d records)", self.dataset_writer.record_count)
            else:
                logger.warning("  → FAIL")

            self.http_client.polite_sleep()

        summary = self._summary()
        logger.info("=" * 65)
        logger.info("DONE: %s", summary)
        logger.info("=" * 65)

        # Write run report
        report = {
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "summary"      : summary,
        }
        Path("run_report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        return summary

    def _summary(self) -> dict[str, int]:
        return {
            "new_jobs_scraped"        : self._new_count,
            "urls_skipped_duplicates" : self._skipped_count,
            "errors"                  : self._error_count,
            "total_jobs_in_dataset"   : self.dataset_writer.record_count,
        }

    def __enter__(self) -> "JobScraper":
        return self

    def __exit__(self, *_) -> None:
        self.http_client.close()
        logger.info("HTTP session closed.")


# ─────────────────────────────────────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    """
    Environment variables:
        SCRAPER_SITE     = wuzzuf | forasna   (default: wuzzuf)
        SCRAPER_MAX_JOBS = N                   (default: 10 for test run)
        SCRAPER_PROXY    = http://user:pass@host:port
    """
    site     = os.getenv("SCRAPER_SITE", "wuzzuf")
    max_jobs = int(os.getenv("SCRAPER_MAX_JOBS", "10"))

    with JobScraper(site=site, max_jobs=max_jobs) as scraper:
        scraper.run()


if __name__ == "__main__":
    main()
