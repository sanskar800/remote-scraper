import os
import json
import csv
import re
import time
from typing import List, Dict, Optional
from dotenv import load_dotenv
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup

# Configuration
load_dotenv()

# API Key Management
API_KEYS = [
    os.getenv(f'FIRECRAWL_API_KEY{"" if i == 0 else f"_{i}"}')
    for i in range(1, 6)
]
API_KEYS = [key for key in API_KEYS if key]

if not API_KEYS:
    raise ValueError("No API keys found. Please configure FIRECRAWL_API_KEY in .env file")

current_key_index = 0
app = FirecrawlApp(api_key=API_KEYS[current_key_index])


def rotate_api_key() -> None:
    #Rotate to the next available API key
    global current_key_index, app
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    app = FirecrawlApp(api_key=API_KEYS[current_key_index])


def scrape_with_retry(url: str, formats: List[str], max_retries: int = 3) -> Optional[object]:
    #Scrape a URL with automatic retry and API key rotation on rate limits
    for attempt in range(max_retries * len(API_KEYS)):
        try:
            return app.scrape(url, formats=formats)
        except Exception as e:
            if 'rate limit' in str(e).lower() or '429' in str(e):
                rotate_api_key()
                continue
            return None
    return None


def extract_job_data(job_data: Dict, source: str) -> Optional[Dict]:
    #Validate and format job data
    if not job_data.get('title') or not job_data.get('apply_url'):
        return None
    
    return {
        'title': job_data['title'].strip(),
        'company': job_data.get('company', 'N/A').strip(),
        'location': job_data.get('location', 'Remote').strip(),
        'job_type': job_data.get('job_type', 'Full-time').strip(),
        'apply_url': job_data['apply_url'].strip(),
        'source': source
    }


def scrape_workable() -> List[Dict]:
    #Scrape jobs from Workable
    jobs = []
    url = "https://jobs.workable.com/search?location=Pātan%2C+Nepal"
    
    result = scrape_with_retry(url, formats=['html'])
    if not result or not hasattr(result, 'html'):
        return jobs
    
    soup = BeautifulSoup(result.html, 'html.parser')
    job_links = soup.find_all('a', href=re.compile(r'/view/'))
    
    for link in job_links[:150]:
        title = link.get_text(strip=True)
        if not title or len(title) < 5:
            continue
        
        job_url = link.get('href', '')
        if job_url and not job_url.startswith('http'):
            job_url = f"https://jobs.workable.com{job_url}"
        
        # Extract company from URL pattern
        company = "N/A"
        if '-at-' in job_url:
            parts = job_url.split('/')[-1].split('-at-')
            if len(parts) > 1:
                company = parts[-1].replace('-', ' ').title()
        
        job = extract_job_data({
            'title': title,
            'company': company,
            'location': 'Remote',
            'job_type': 'Full-time',
            'apply_url': job_url
        }, 'Workable')
        
        if job:
            jobs.append(job)
    
    return jobs


def scrape_dynamitejobs() -> List[Dict]:
    #Scrape jobs from DynamiteJobs
    jobs = []
    
    for page in range(1, 51):
        url = f"https://dynamitejobs.com/remote-jobs{'?page=' + str(page) if page > 1 else ''}"
        
        result = scrape_with_retry(url, formats=['html'])
        if not result or not hasattr(result, 'html'):
            continue
        
        soup = BeautifulSoup(result.html, 'html.parser')
        h2_elements = [h2 for h2 in soup.find_all('h2') if h2.get('href') and '/remote-job/' in h2.get('href')]
        
        for h2 in h2_elements:
            title = h2.get_text(strip=True)
            if not title or len(title) < 5:
                continue
            
            job_url = h2.get('href', '')
            if job_url and not job_url.startswith('http'):
                job_url = f"https://dynamitejobs.com{job_url}"
            
            # Extract company from next sibling
            company = "N/A"
            next_p = h2.find_next_sibling('p')
            if next_p:
                company = next_p.get_text(strip=True)
            
            job = extract_job_data({
                'title': title,
                'company': company,
                'location': 'Remote',
                'job_type': 'Full-time',
                'apply_url': job_url
            }, 'DynamiteJobs')
            
            if job:
                jobs.append(job)
        
        time.sleep(2)
        
        if len(jobs) >= 200:
            break
    
    return jobs


def scrape_remotive() -> List[Dict]:
    #Scrape jobs from Remotive
    jobs = []
    url = "https://remotive.com/remote-jobs"
    
    result = scrape_with_retry(url, formats=['html'])
    if not result or not hasattr(result, 'html'):
        return jobs
    
    soup = BeautifulSoup(result.html, 'html.parser')
    job_links = soup.find_all('a', href=re.compile(r'/remote-jobs/[^/]+/[^/]+-\d+$'))
    
    for link in job_links[:150]:
        title_text = link.get_text(strip=True)
        title = title_text
        company = "N/A"
        
        # Extract company from title (format: "Title • Company")
        if '•' in title_text:
            parts = title_text.split('•')
            title = parts[0].strip()
            if len(parts) > 1:
                company = parts[1].strip()
        
        job_url = link.get('href', '')
        if job_url and not job_url.startswith('http'):
            job_url = f"https://remotive.com{job_url}"
        
        if job_url.count('/') <= 4:
            continue
        
        job = extract_job_data({
            'title': title,
            'company': company,
            'location': 'Remote',
            'job_type': 'Full-time',
            'apply_url': job_url
        }, 'Remotive')
        
        if job:
            jobs.append(job)
    
    return jobs[:150]


def scrape_mercor() -> List[Dict]:
    #Scrape jobs from Mercor
    jobs = []
    url = "https://work.mercor.com/explore"
    
    result = scrape_with_retry(url, formats=['html'])
    if not result or not hasattr(result, 'html'):
        return jobs
    
    soup = BeautifulSoup(result.html, 'html.parser')
    job_cards = soup.find_all('a', href=re.compile(r'listingId='))
    
    for card in job_cards[:150]:
        title_elem = card.find('h2')
        if not title_elem:
            continue
        
        title = title_elem.get_text(strip=True)
        if not title or len(title) < 5:
            continue
        
        job_url = card.get('href', '')
        if job_url and not job_url.startswith('http'):
            job_url = f"https://work.mercor.com{job_url}"
        
        # Extract metadata
        location = "Remote"
        job_type = "Contract"
        
        metadata_divs = card.find_all('div', class_=re.compile(r'flex.*items-center.*gap-1.*text-sm'))
        for div in metadata_divs:
            text = div.get_text(strip=True)
            if any(kw in text for kw in ['Remote', 'Worldwide']):
                location = text
            elif 'full-time' in text.lower():
                job_type = "Full-time"
        
        job = extract_job_data({
            'title': title,
            'company': 'Mercor',
            'location': location,
            'job_type': job_type,
            'apply_url': job_url
        }, 'Mercor')
        
        if job:
            jobs.append(job)
    
    return jobs[:150]


def scrape_remoteco() -> List[Dict]:
    #Scrape jobs from Remote.co
    jobs = []
    categories = [
        "accounting", "customer-service", "design", "developer", "online-data-entry",
        "online-editing", "entry-level", "freelance", "healthcare", "human-resources",
        "insurance", "legal", "marketing", "medical-coding", "non-profit",
        "project-management", "recruiter", "sales", "software", "teaching", "writing"
    ]
    
    for category in categories:
        url = f"https://remote.co/remote-jobs/{category}"
        
        result = scrape_with_retry(url, formats=['html'])
        if not result or not hasattr(result, 'html'):
            continue
        
        soup = BeautifulSoup(result.html, 'html.parser')
        job_links = soup.find_all('a', id=re.compile(r'^job-name-'))
        
        for link in job_links:
            title = link.get_text(strip=True)
            title = re.sub(r'\s*(New!|Today)\s*', '', title).strip()
            
            if not title or len(title) < 5:
                continue
            
            job_url = link.get('href', '')
            if job_url and not job_url.startswith('http'):
                job_url = f"https://remote.co{job_url}"
            
            # Extract company from card
            parent_card = link.find_parent(['div', 'article', 'li'])
            company = "N/A"
            
            if parent_card:
                # Try image alt text first
                company_img = parent_card.find('img', alt=True)
                if company_img:
                    alt = company_img.get('alt', '').strip()
                    if alt and len(alt) > 2 and alt.lower() not in ['logo', 'image', 'icon']:
                        company = alt
            
            job = extract_job_data({
                'title': title,
                'company': company,
                'location': 'Remote',
                'job_type': 'Full-time',
                'apply_url': job_url
            }, 'Remote.co')
            
            if job:
                jobs.append(job)
        
        time.sleep(4)
    
    return jobs


def deduplicate_jobs(jobs: List[Dict]) -> List[Dict]:
    #Remove duplicate job listings based on URL
    seen_urls = set()
    unique_jobs = []
    
    for job in jobs:
        url = job.get('apply_url', '')
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)
    
    return unique_jobs


def export_to_csv(jobs: List[Dict], filename: str = "jobs.csv") -> None:
    #Export jobs to CSV file
    if not jobs:
        return
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'company', 'location', 'job_type', 'apply_url', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs)


def export_to_json(jobs: List[Dict], filename: str = "jobs.json") -> None:
    #Export jobs to JSON file
    with open(filename, 'w', encoding='utf-8') as jsonfile:
        json.dump(jobs, jsonfile, indent=2, ensure_ascii=False)


def main():
    #Main execution function
    print(f"Initializing scraper with {len(API_KEYS)} API key(s)...")
    
    all_jobs = []
    scrapers = [
        ('Workable', scrape_workable),
        ('DynamiteJobs', scrape_dynamitejobs),
        ('Remotive', scrape_remotive),
        ('Mercor', scrape_mercor),
        ('Remote.co', scrape_remoteco)
    ]
    
    for name, scraper in scrapers:
        print(f"Scraping {name}...")
        jobs = scraper()
        all_jobs.extend(jobs)
        print(f"Collected {len(jobs)} jobs from {name}")
    
    print(f"\nTotal jobs collected: {len(all_jobs)}")
    
    # Deduplicate
    unique_jobs = deduplicate_jobs(all_jobs)
    duplicates_removed = len(all_jobs) - len(unique_jobs)
    print(f"Removed {duplicates_removed} duplicates")
    print(f"Final unique jobs: {len(unique_jobs)}")
    
    # Export
    export_to_json(unique_jobs)
    export_to_csv(unique_jobs)
    print(f"\nExported to jobs.json and jobs.csv")
    print("Scraping complete.")


if __name__ == "__main__":
    main()
