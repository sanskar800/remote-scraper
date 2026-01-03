import os
import json
import csv
import time
from dotenv import load_dotenv
from firecrawl import FirecrawlApp

load_dotenv()

# Load API keys
API_KEYS = [os.getenv(f'FIRECRAWL_API_KEY{"" if i == 0 else f"_{i}"}') for i in range(1, 6)]
API_KEYS = [key for key in API_KEYS if key]

current_key_index = 0
app = FirecrawlApp(api_key=API_KEYS[current_key_index])


def rotate_key():
    global current_key_index, app
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    app = FirecrawlApp(api_key=API_KEYS[current_key_index])


JOB_SCHEMA = {
    "type": "object",
    "properties": {
        "jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "company": {"type": "string"},
                    "location": {"type": "string"},
                    "job_type": {"type": "string"},
                    "apply_url": {"type": "string"}
                }
            }
        },
        "next_page_url": {
            "type": "string",
            "description": "URL of the next page button or link"
        }
    }
}


def scrape_site(url, max_pages=2):
    all_jobs = []
    current_url = url
    page_count = 0
    
    while current_url and page_count < max_pages:
        page_count += 1
        try:
            result = app.extract(urls=[current_url], schema=JOB_SCHEMA)
            
            if not result or not result.data:
                break
            
            data = result.data[0] if isinstance(result.data, list) else result.data
            jobs = data.get('jobs', [])
            if jobs:
                all_jobs.extend(jobs)
            
            next_url = data.get('next_page_url')
            if not next_url or next_url == current_url:
                break
            
            current_url = next_url
            time.sleep(2)
            
        except Exception as e:
            if 'Payment Required' in str(e) or 'Insufficient credits' in str(e):
                rotate_key()
                time.sleep(2)
                try:
                    result = app.extract(urls=[current_url], schema=JOB_SCHEMA)
                    if result and result.data:
                        data = result.data[0] if isinstance(result.data, list) else result.data
                        jobs = data.get('jobs', [])
                        if jobs:
                            all_jobs.extend(jobs)
                        next_url = data.get('next_page_url')
                        if next_url and next_url != current_url:
                            current_url = next_url
                        else:
                            break
                except:
                    break
            else:
                break
    
    return all_jobs


def main():
    sites = [
        "https://jobs.workable.com/search?location=Pātan%2C+Nepal",
        "https://dynamitejobs.com/remote-jobs",
        "https://remotive.com/remote-jobs",
        "https://work.mercor.com/explore",
        "https://remote.co/remote-jobs/developer",
        "https://remote.co/remote-jobs/design",
        "https://remote.co/remote-jobs/marketing"
    ]
    
    all_jobs = []
    for url in sites:
        jobs = scrape_site(url, max_pages=2)
        all_jobs.extend(jobs)
        time.sleep(3)
    
    # Deduplicate
    seen = set()
    unique = []
    for job in all_jobs:
        url = job.get('apply_url', '')
        if url and url not in seen:
            seen.add(url)
            unique.append(job)
    
    # Save
    with open('jobs.json', 'w', encoding='utf-8') as f:
        json.dump(unique, f, indent=2, ensure_ascii=False)
    
    with open('jobs.csv', 'w', newline='', encoding='utf-8') as f:
        if unique:
            writer = csv.DictWriter(f, fieldnames=['title', 'company', 'location', 'job_type', 'apply_url'])
            writer.writeheader()
            writer.writerows(unique)

if __name__ == "__main__":
    main()