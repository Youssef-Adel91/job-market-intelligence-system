import json
from collections import Counter
from datetime import datetime

def generate_dashboard():
    try:
        with open('dataset_jobs.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        return "No data found yet."

    total_jobs = len(data)
    companies = Counter([job.get('company_name', 'Unknown') for job in data])
    locations = Counter([job.get('location', 'Not Specified') for job in data])
    
    # تحضير نص الداشبورد بصيغة Markdown
    stats_md = f"""
# 🚀 Job Market Intelligence Dashboard
*Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

### 📊 Quick Stats
- **Total Jobs Scraped:** {total_jobs}
- **Unique Companies:** {len(companies)}

### 🏢 Top Hiring Companies
| Company | Job Count |
| :--- | :--- |
"""
    for company, count in companies.most_common(5):
        stats_md += f"| {company} | {count} |\n"

    stats_md += "\n### 📍 Top Locations\n"
    for loc, count in locations.most_common(5):
        stats_md += f"- **{loc}:** {count} jobs\n"

    # تحديث ملف README.md
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(stats_md)
    print("Dashboard updated successfully!")

if __name__ == "__main__":
    generate_dashboard()