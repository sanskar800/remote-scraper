"""
Job Scraper using Firecrawl API
Collects 500+ job listings from multiple remote job websites

Based on browser analysis:
- Remote.co: 21 categories, company names in text nodes
- DynamiteJobs: h2 elements have href attributes
- Mercor: 15 jobs/page, company is "Mercor"
- Workable: Company in URL pattern
- Remotive: Working well
"""

import os
import json
import csv
import re
from typing import List, Dict, Optional
from dotenv import load_dotenv
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup
import time

# Load environment variables
load_dotenv()

# Initialize Firecrawl client with API key rotation
api_keys = [
    os.getenv('FIRECRAWL_API_KEY'),
    os.getenv('FIRECRAWL_API_KEY_2'),
    os.getenv('FIRECRAWL_API_KEY_3'),
    os.getenv('FIRECRAWL_API_KEY_4'),
    os.getenv('FIRECRAWL_API_KEY_5')
]

# Filter out None values
api_keys = [key for key in api_keys if key]

if not api_keys:
    raise ValueError("No FIRECRAWL_API_KEY found in .env file")

print(f"\n[INIT] Loaded {len(api_keys)} API key(s) for rotation")

# Global variables for API key rotation
current_key_index = 0
app = FirecrawlApp(api_key=api_keys[current_key_index])


def rotate_api_key():
    """Rotate to the next API key when rate limit is hit"""
    global current_key_index, app
    
    if len(api_keys) <= 1:
        print("[ROTATION] No additional API keys available")
        return False
    
    current_key_index = (current_key_index + 1) % len(api_keys)
    app = FirecrawlApp(api_key=api_keys[current_key_index])
    print(f"[ROTATION] Switched to API key #{current_key_index + 1}")
    return True


def scrape_with_retry(url, formats=['html'], max_retries=3):
    """Scrape URL with automatic API key rotation on rate limit"""
    for attempt in range(max_retries):
        try:
            result = app.scrape(url, formats=formats)
            return result
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a rate limit error
            if 'Rate Limit Exceeded' in error_msg or 'rate limit' in error_msg.lower():
                print(f"[RATE LIMIT] Hit rate limit on API key #{current_key_index + 1}")
                
                # Try to rotate to next API key
                if rotate_api_key():
                    print(f"[RETRY] Retrying with API key #{current_key_index + 1}")
                    time.sleep(2)  # Brief pause before retry
                    continue
                else:
                    # No more API keys to try
                    if attempt < max_retries - 1:
                        wait_time = 30
                        print(f"[WAIT] All API keys exhausted. Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                    else:
                        raise
            else:
                # Not a rate limit error, raise it
                raise
    
    raise Exception(f"Failed to scrape {url} after {max_retries} attempts")


def extract_job_data(raw_data: Dict, source: str) -> Optional[Dict]:
    """Extract and normalize job data to match the required schema"""
    try:
        job = {
            "title": raw_data.get("title", "").strip(),
            "company": raw_data.get("company", "").strip(),
            "location": raw_data.get("location", "").strip(),
            "job_type": raw_data.get("job_type", "").strip(),
            "apply_url": raw_data.get("apply_url", "").strip(),
            "source": source
        }
        
        # Validate required fields
        if job["title"] and job["apply_url"] and len(job["title"]) > 3:
            return job
        return None
    except Exception as e:
        return None


def scrape_workable() -> List[Dict]:
    """
    Scrape jobs from Workable - Single-step for API efficiency
    Company names extracted from URL pattern: .../at-company
    """
    print("\n[WORKABLE] Starting scrape...")
    jobs = []
    
    try:
        url = "https://jobs.workable.com/search?location=P%C4%81tan%2C+Nepal"
        print(f"[WORKABLE] Scraping: {url}")
        
        result = scrape_with_retry(url, formats=['html'])
        
        if result and hasattr(result, 'html'):
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Find all job links
            job_links = soup.find_all('a', href=re.compile(r'/view/'))
            
            print(f"[WORKABLE] Found {len(job_links)} job links")
            
            for link in job_links[:150]:
                try:
                    title_text = link.get_text(strip=True)
                    
                    if not title_text or len(title_text) < 5:
                        continue
                    
                    job_url = link.get('href', '')
                    if job_url and not job_url.startswith('http'):
                        if job_url.startswith('/'):
                            job_url = f"https://jobs.workable.com{job_url}"
                        else:
                            job_url = f"https://jobs.workable.com/{job_url}"
                    
                    # Extract company from URL pattern
                    company = "N/A"
                    url_parts = job_url.split('/')
                    if len(url_parts) > 4:
                        last_part = url_parts[-1]
                        if '-at-' in last_part:
                            company = last_part.split('-at-')[-1].replace('-', ' ').title()
                    
                    job_data = {
                        "title": title_text,
                        "company": company,
                        "location": "Remote",
                        "job_type": "Full-time",
                        "apply_url": job_url,
                        "source": "Workable"
                    }
                    
                    job = extract_job_data(job_data, "Workable")
                    if job:
                        jobs.append(job)
                        
                except Exception as e:
                    continue
        
        print(f"[WORKABLE] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[WORKABLE] Error: {e}")
    
    return jobs


def scrape_mercor() -> List[Dict]:
    """
    Scrape jobs from Mercor
    Extract location and job type from metadata divs on job cards
    """
    print("\n[MERCOR] Starting scrape...")
    jobs = []
    
    try:
        # Mercor uses client-side pagination, so we can only scrape page 1 with Firecrawl
        url = "https://work.mercor.com/explore"
        print(f"[MERCOR] Scraping: {url}")
        
        result = scrape_with_retry(url, formats=['html'])
        
        if result and hasattr(result, 'html'):
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Find job cards: a tags with listingId in href
            job_cards = soup.find_all('a', href=re.compile(r'listingId='))
            
            print(f"[MERCOR] Found {len(job_cards)} job cards")
            
            for card in job_cards[:150]:
                try:
                    # Get title from h2
                    title_elem = card.find('h2')
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    
                    if not title or len(title) < 5:
                        continue
                    
                    # Get URL
                    job_url = card.get('href', '')
                    if job_url and not job_url.startswith('http'):
                        if job_url.startswith('/'):
                            job_url = f"https://work.mercor.com{job_url}"
                        else:
                            job_url = f"https://work.mercor.com/{job_url}"
                    
                    # Company is always Mercor (they're a recruiting platform)
                    company = "Mercor"
                    
                    # Extract location and job type from metadata divs
                    location = "Remote"
                    job_type = "Contract"
                    
                    # Find metadata divs with specific class
                    metadata_divs = card.find_all('div', class_=re.compile(r'flex.*items-center.*gap-1.*text-sm'))
                    
                    for div in metadata_divs:
                        text = div.get_text(strip=True)
                        
                        # Check for location keywords
                        if any(keyword in text for keyword in ['Remote', 'Worldwide', 'Location']):
                            if text and len(text) < 50:
                                location = text
                        
                        # Check for job type keywords
                        elif any(keyword in text for keyword in ['contract', 'Full-time', 'Part-time', 'Hourly']):
                            if text and len(text) < 50:
                                # Normalize job type
                                if 'contract' in text.lower() or 'hourly' in text.lower():
                                    job_type = "Contract"
                                elif 'full-time' in text.lower():
                                    job_type = "Full-time"
                                elif 'part-time' in text.lower():
                                    job_type = "Part-time"
                    
                    job_data = {
                        "title": title,
                        "company": company,
                        "location": location,
                        "job_type": job_type,
                        "apply_url": job_url,
                        "source": "Mercor"
                    }
                    
                    job = extract_job_data(job_data, "Mercor")
                    if job:
                        jobs.append(job)
                        
                except Exception as e:
                    continue
        
        jobs = jobs[:150]
        print(f"[MERCOR] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[MERCOR] Error: {e}")
    
    return jobs


def scrape_remotive() -> List[Dict]:
    """
    Scrape jobs from Remotive - Single-step for API efficiency
    """
    print("\n[REMOTIVE] Starting scrape...")
    jobs = []
    
    try:
        url = "https://remotive.com/remote-jobs"
        print(f"[REMOTIVE] Scraping: {url}")
        
        result = scrape_with_retry(url, formats=['html'])
        
        if result and hasattr(result, 'html'):
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Find job links
            job_links = soup.find_all('a', href=re.compile(r'/remote-jobs/[^/]+/[^/]+-\d+$'))
            
            print(f"[REMOTIVE] Found {len(job_links)} job links")
            
            for link in job_links[:150]:
                try:
                    title_text = link.get_text(strip=True)
                    
                    title = title_text
                    company = "N/A"
                    
                    if '•' in title_text:
                        parts = title_text.split('•')
                        title = parts[0].strip()
                        if len(parts) > 1:
                            company_text = parts[1].strip()
                            # Remove duplication
                            words = company_text.split()
                            if len(words) >= 2 and words[0] == words[-1]:
                                company = words[0]
                            else:
                                company = company_text
                    
                    job_url = link.get('href', '')
                    if job_url and not job_url.startswith('http'):
                        job_url = f"https://remotive.com{job_url}"
                    
                    # Skip category pages
                    if job_url.count('/') <= 4:
                        continue
                    
                    job_data = {
                        "title": title,
                        "company": company,
                        "location": "Remote",
                        "job_type": "Full-time",
                        "apply_url": job_url,
                        "source": "Remotive"
                    }
                    
                    job = extract_job_data(job_data, "Remotive")
                    if job:
                        jobs.append(job)
                        
                except Exception as e:
                    continue
        
        jobs = jobs[:150]
        print(f"[REMOTIVE] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[REMOTIVE] Error: {e}")
    
    return jobs


def scrape_dynamitejobs() -> List[Dict]:
    """
    Scrape jobs from DynamiteJobs - Single-step for API efficiency
    Extracts company names from listing page, uses defaults for location/job type
    """
    print("\n[DYNAMITEJOBS] Starting scrape...")
    jobs = []
    
    try:
        # Scrape up to 50 pages
        for page in range(1, 51):
            url = f"https://dynamitejobs.com/remote-jobs?page={page}" if page > 1 else "https://dynamitejobs.com/remote-jobs"
            print(f"[DYNAMITEJOBS] Scraping page {page}: {url}")
            
            try:
                result = scrape_with_retry(url, formats=['html'])
                
                if result and hasattr(result, 'html'):
                    soup = BeautifulSoup(result.html, 'html.parser')
                    
                    # Find job links - h2 elements with href attribute
                    h2_elements = soup.find_all('h2')
                    job_links = []
                    
                    for h2 in h2_elements:
                        href = h2.get('href')
                        if href and '/remote-job/' in href:
                            job_links.append(h2)
                    
                    print(f"[DYNAMITEJOBS] Page {page}: Found {len(job_links)} jobs")
                    
                    for h2 in job_links:
                        try:
                            # Get title
                            title = h2.get_text(strip=True)
                            
                            if not title or len(title) < 5:
                                continue
                            
                            # Get job URL
                            job_url = h2.get('href', '')
                            if job_url and not job_url.startswith('http'):
                                job_url = f"https://dynamitejobs.com{job_url}"
                            
                            if not job_url:
                                continue
                            
                            # Get company from next sibling p tag (on listing page)
                            company = "N/A"
                            next_p = h2.find_next_sibling('p')
                            if next_p:
                                company = next_p.get_text(strip=True)
                            
                            # Fallback: extract from URL
                            if company == "N/A" and job_url:
                                url_parts = job_url.split('/')
                                if len(url_parts) > 4:
                                    company = url_parts[4].replace('-', ' ').title()
                            
                            job_data = {
                                "title": title,
                                "company": company,
                                "location": "Remote",
                                "job_type": "Full-time",
                                "apply_url": job_url,
                                "source": "DynamiteJobs"
                            }
                            
                            job = extract_job_data(job_data, "DynamiteJobs")
                            if job:
                                jobs.append(job)
                                
                        except Exception as e:
                            continue
            
            except Exception as page_error:
                print(f"[DYNAMITEJOBS] Error on page {page}: {page_error}")
                continue
            
            time.sleep(2)  # Delay between pages
            
            if len(jobs) >= 200:  # Collect up to 200 jobs
                break
        
        print(f"[DYNAMITEJOBS] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[DYNAMITEJOBS] Error: {e}")
    
    return jobs
    """
    Scrape jobs from DynamiteJobs
    Key finding: h2 elements have href attribute with job URL
    Company name is in the nextElementSibling (p tag)
    """
    print("\n[DYNAMITEJOBS] Starting scrape...")
    jobs = []
    
    try:
        # Scrape up to 50 pages (will use API key rotation)
        for page in range(1, 51):
            url = f"https://dynamitejobs.com/remote-jobs?page={page}" if page > 1 else "https://dynamitejobs.com/remote-jobs"
            print(f"[DYNAMITEJOBS] Scraping page {page}: {url}")
            
            result = app.scrape(url, formats=['html'])
            
            if result and hasattr(result, 'html'):
                soup = BeautifulSoup(result.html, 'html.parser')
                
                # Find the job list container
                container = soup.find('div', class_=re.compile(r'overflow-y-auto.*border-gray-200'))
                
                if container:
                    # Find all h2 elements (job titles)
                    h2_elements = container.find_all('h2')
                    
                    print(f"[DYNAMITEJOBS] Page {page}: Found {len(h2_elements)} jobs")
                    
                    for h2 in h2_elements:
                        try:
                            title = h2.get_text(strip=True)
                            
                            if not title or len(title) < 5:
                                continue
                            
                            # Get href from h2 element (non-standard but that's how they do it)
                            job_url = h2.get('href', '')
                            if job_url and not job_url.startswith('http'):
                                job_url = f"https://dynamitejobs.com{job_url}"
                            
                            # Get company from next sibling (p tag)
                            company = "N/A"
                            next_elem = h2.find_next_sibling('p')
                            if next_elem:
                                company = next_elem.get_text(strip=True)
                            
                            # Fallback: extract from URL
                            if company == "N/A" and job_url:
                                url_parts = job_url.split('/')
                                if len(url_parts) > 4:
                                    company = url_parts[4].replace('-', ' ').title()
                            
                            job_data = {
                                "title": title,
                                "company": company,
                                "location": "Remote",
                                "job_type": "Remote",
                                "apply_url": job_url,
                                "source": "DynamiteJobs"
                            }
                            
                            job = extract_job_data(job_data, "DynamiteJobs")
                            if job:
                                jobs.append(job)
                                
                        except Exception as e:
                            continue
            
            time.sleep(3)  # Increased delay to avoid rate limits
            
            if len(jobs) >= 80:  # Reduced target to stay within limits
                break
        
        # Return all jobs collected (no limit)
        print(f"[DYNAMITEJOBS] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[DYNAMITEJOBS] Error: {e}")
    
    return jobs


def scrape_mercor() -> List[Dict]:
    """
    Scrape jobs from Mercor
    Extract location and job type from metadata divs on job cards
    """
    print("\n[MERCOR] Starting scrape...")
    jobs = []
    
    try:
        # Mercor uses client-side pagination, so we can only scrape page 1 with Firecrawl
        url = "https://work.mercor.com/explore"
        print(f"[MERCOR] Scraping: {url}")
        
        result = scrape_with_retry(url, formats=['html'])
        
        if result and hasattr(result, 'html'):
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Find job cards: a tags with listingId in href
            job_cards = soup.find_all('a', href=re.compile(r'listingId='))
            
            print(f"[MERCOR] Found {len(job_cards)} job cards")
            
            for card in job_cards[:150]:
                try:
                    # Get title from h2
                    title_elem = card.find('h2')
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    
                    if not title or len(title) < 5:
                        continue
                    
                    # Get URL
                    job_url = card.get('href', '')
                    if job_url and not job_url.startswith('http'):
                        if job_url.startswith('/'):
                            job_url = f"https://work.mercor.com{job_url}"
                        else:
                            job_url = f"https://work.mercor.com/{job_url}"
                    
                    # Company is always Mercor (they're a recruiting platform)
                    company = "Mercor"
                    
                    # Extract location and job type from metadata divs
                    location = "Remote"
                    job_type = "Contract"
                    
                    # Find metadata divs with specific class
                    metadata_divs = card.find_all('div', class_=re.compile(r'flex.*items-center.*gap-1.*text-sm'))
                    
                    for div in metadata_divs:
                        text = div.get_text(strip=True)
                        
                        # Check for location keywords
                        if any(keyword in text for keyword in ['Remote', 'Worldwide', 'Location']):
                            if text and len(text) < 50:
                                location = text
                        
                        # Check for job type keywords
                        elif any(keyword in text for keyword in ['contract', 'Full-time', 'Part-time', 'Hourly']):
                            if text and len(text) < 50:
                                # Normalize job type
                                if 'contract' in text.lower() or 'hourly' in text.lower():
                                    job_type = "Contract"
                                elif 'full-time' in text.lower():
                                    job_type = "Full-time"
                                elif 'part-time' in text.lower():
                                    job_type = "Part-time"
                    
                    job_data = {
                        "title": title,
                        "company": company,
                        "location": location,
                        "job_type": job_type,
                        "apply_url": job_url,
                        "source": "Mercor"
                    }
                    
                    job = extract_job_data(job_data, "Mercor")
                    if job:
                        jobs.append(job)
                        
                except Exception as e:
                    continue
        
        jobs = jobs[:150]
        print(f"[MERCOR] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[MERCOR] Error: {e}")
    
    return jobs




def scrape_remotive() -> List[Dict]:
    """
    Scrape jobs from Remotive - Single-step for API efficiency
    """
    print("\n[REMOTIVE] Starting scrape...")
    jobs = []
    
    try:
        url = "https://remotive.com/remote-jobs"
        print(f"[REMOTIVE] Scraping: {url}")
        
        result = scrape_with_retry(url, formats=['html'])
        
        if result and hasattr(result, 'html'):
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Find job links
            job_links = soup.find_all('a', href=re.compile(r'/remote-jobs/[^/]+/[^/]+-\d+$'))
            
            print(f"[REMOTIVE] Found {len(job_links)} job links")
            
            for link in job_links[:150]:
                try:
                    title_text = link.get_text(strip=True)
                    
                    title = title_text
                    company = "N/A"
                    
                    if '•' in title_text:
                        parts = title_text.split('•')
                        title = parts[0].strip()
                        if len(parts) > 1:
                            company_text = parts[1].strip()
                            # Remove duplication
                            words = company_text.split()
                            if len(words) >= 2 and words[0] == words[-1]:
                                company = words[0]
                            else:
                                company = company_text
                    
                    job_url = link.get('href', '')
                    if job_url and not job_url.startswith('http'):
                        job_url = f"https://remotive.com{job_url}"
                    
                    # Skip category pages
                    if job_url.count('/') <= 4:
                        continue
                    
                    job_data = {
                        "title": title,
                        "company": company,
                        "location": "Remote",
                        "job_type": "Full-time",
                        "apply_url": job_url,
                        "source": "Remotive"
                    }
                    
                    job = extract_job_data(job_data, "Remotive")
                    if job:
                        jobs.append(job)
                        
                except Exception as e:
                    continue
        
        jobs = jobs[:150]
        print(f"[REMOTIVE] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[REMOTIVE] Error: {e}")
    
    return jobs
    """
    Scrape jobs from Remotive
    Working well - keeping existing logic
    """
    print("\n[REMOTIVE] Starting scrape...")
    jobs = []
    
    try:
        url = "https://remotive.com/remote-jobs"
        print(f"[REMOTIVE] Scraping: {url}")
        
        result = app.scrape(url, formats=['html'])
        
        if result and hasattr(result, 'html'):
            soup = BeautifulSoup(result.html, 'html.parser')
            
            # Find job links
            job_links = soup.find_all('a', href=re.compile(r'/remote-jobs/[^/]+/[^/]+-\d+$'))
            
            print(f"[REMOTIVE] Found {len(job_links)} job links")
            
            for link in job_links[:150]:
                try:
                    title_text = link.get_text(strip=True)
                    
                    title = title_text
                    company = "N/A"
                    
                    if '•' in title_text:
                        parts = title_text.split('•')
                        title = parts[0].strip()
                        if len(parts) > 1:
                            company_text = parts[1].strip()
                            # Remove duplication
                            words = company_text.split()
                            if len(words) >= 2 and words[0] == words[-1]:
                                company = words[0]
                            else:
                                company = company_text
                    
                    job_url = link.get('href', '')
                    if job_url and not job_url.startswith('http'):
                        job_url = f"https://remotive.com{job_url}"
                    
                    # Skip category pages
                    if job_url.count('/') <= 4:
                        continue
                    
                    job_data = {
                        "title": title,
                        "company": company,
                        "location": "Remote",
                        "job_type": "Full-time",
                        "apply_url": job_url,
                        "source": "Remotive"
                    }
                    
                    job = extract_job_data(job_data, "Remotive")
                    if job:
                        jobs.append(job)
                        
                except Exception as e:
                    continue
        
        jobs = jobs[:150]
        print(f"[REMOTIVE] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[REMOTIVE] Error: {e}")
    
    return jobs



def scrape_remoteco() -> List[Dict]:
    """
    Scrape jobs from Remote.co
    Key finding: 21 categories on main page
    Company names are text nodes after the "Save Job" link
    Some jobs are member-only (company hidden)
    """
    print("\n[REMOTE.CO] Starting scrape...")
    jobs = []
    
    try:
        # All 21 categories with API key rotation
        categories = [
            "accounting", "customer-service", "design", "developer", "online-data-entry",
            "online-editing", "entry-level", "freelance", "healthcare", "human-resources",
            "insurance", "legal", "marketing", "medical-coding", "non-profit",
            "project-management", "recruiter", "sales", "software", "teaching", "writing"
        ]
        
        for category in categories:
                
            url = f"https://remote.co/remote-jobs/{category}"
            print(f"[REMOTE.CO] Scraping category: {category}")
            
            try:
                result = scrape_with_retry(url, formats=['html'])
                
                if result and hasattr(result, 'html'):
                    soup = BeautifulSoup(result.html, 'html.parser')
                    
                    # Find job links (a tags with id starting with "job-name-")
                    job_links = soup.find_all('a', id=re.compile(r'^job-name-'))
                    
                    print(f"[REMOTE.CO] {category}: Found {len(job_links)} jobs")
                    
                    for link in job_links:
                        try:
                            # Get title and clean it (remove "New!" and "Today")
                            title = link.get_text(strip=True)
                            title = re.sub(r'\s*(New!|Today)\s*', '', title).strip()
                            
                            if not title or len(title) < 5:
                                continue
                            
                            # Get URL
                            job_url = link.get('href', '')
                            if job_url and not job_url.startswith('http'):
                                job_url = f"https://remote.co{job_url}"
                            
                            # Find the parent card container
                            parent_card = link.find_parent(['div', 'article', 'li'])
                            if not parent_card:
                                parent_card = link.find_parent()
                            
                            # Get all text from the card, split by lines
                            card_text_raw = parent_card.get_text(separator='\n', strip=True) if parent_card else ''
                            card_text_lines = [line.strip() for line in card_text_raw.split('\n') if line.strip()]
                            
                            # Initialize fields
                            company = "N/A"
                            location = "Remote"
                            job_type = "Full-time"
                            
                            # === COMPANY NAME EXTRACTION ===
                            # First try: image alt text (most reliable)
                            if parent_card:
                                company_img = parent_card.find('img', alt=True)
                                if company_img:
                                    alt = company_img.get('alt', '').strip()
                                    if alt and len(alt) > 2 and alt.lower() not in ['logo', 'image', 'icon']:
                                        company = alt
                            
                            # Second try: parse card text lines
                            if company == "N/A":
                                for text in card_text_lines:
                                    # Skip if it's the job title or common non-company text
                                    if (text and len(text) > 2 and len(text) < 100 and
                                        text != title and
                                        text not in ['New!', 'Today', 'Save Job'] and
                                        not text.startswith('100%') and
                                        not text.startswith('Full-Time') and
                                        not text.startswith('Full-time') and
                                        not text.startswith('Part-Time') and
                                        not text.startswith('Part-time') and
                                        not text.startswith('Contract') and
                                        not text.startswith('Freelance') and
                                        not text.startswith('Hybrid') and
                                        not text.startswith('Remote in') and
                                        not text.startswith('$') and
                                        not re.match(r'^\d+', text)):  # Skip numbers
                                        company = text
                                        break
                            
                            # === LOCATION EXTRACTION ===
                            for text in card_text_lines:
                                # Look for location patterns
                                if text.startswith('Remote in') or text.startswith('Hybrid in'):
                                    location = text
                                    break
                                elif 'Remote' in text and len(text) < 50 and ',' in text:
                                    # Likely a location like "Remote, US" or "Remote in Norfolk, VA"
                                    location = text
                                    break
                            
                            # === JOB TYPE EXTRACTION ===
                            # Look for job type in card text
                            for text in card_text_lines:
                                text_lower = text.lower()
                                if text in ['Full-time', 'Part-time', 'Contract', 'Freelance']:
                                    job_type = text
                                    break
                                elif 'full-time' in text_lower or 'full time' in text_lower:
                                    job_type = "Full-time"
                                    break
                                elif 'part-time' in text_lower or 'part time' in text_lower:
                                    job_type = "Part-time"
                                    break
                                elif 'contract' in text_lower:
                                    job_type = "Contract"
                                    break
                                elif 'freelance' in text_lower:
                                    job_type = "Freelance"
                                    break
                            
                            job_data = {
                                "title": title,
                                "company": company,
                                "location": "Remote",
                                "job_type": "Full-time",
                                "apply_url": job_url,
                                "source": "Remote.co"
                            }
                            
                            job = extract_job_data(job_data, "Remote.co")
                            if job:
                                jobs.append(job)
                                
                        except Exception as e:
                            continue
                
                time.sleep(4)  # Longer delay between categories
                
            except Exception as e:
                print(f"[REMOTE.CO] Error on {category}: {e}")
                continue
        
        # Return all jobs collected (no limit)
        print(f"[REMOTE.CO] Collected {len(jobs)} jobs")
        
    except Exception as e:
        print(f"[REMOTE.CO] Error: {e}")
    
    return jobs


def deduplicate_jobs(jobs: List[Dict]) -> List[Dict]:
    """Remove duplicate jobs based on apply_url"""
    seen_urls = set()
    unique_jobs = []
    
    for job in jobs:
        url = job.get("apply_url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)
    
    duplicates_removed = len(jobs) - len(unique_jobs)
    if duplicates_removed > 0:
        print(f"\n[DEDUPLICATION] Removed {duplicates_removed} duplicate jobs")
    
    return unique_jobs


def export_to_csv(jobs: List[Dict], filename: str = "jobs.csv"):
    """Export jobs to CSV file"""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ["title", "company", "location", "job_type", "apply_url", "source"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for job in jobs:
                writer.writerow(job)
        
        print(f"\n[EXPORT] Successfully exported {len(jobs)} jobs to {filename}")
    except Exception as e:
        print(f"\n[EXPORT] Error exporting to CSV: {e}")


def export_to_json(jobs: List[Dict], filename: str = "jobs.json"):
    """Export jobs to JSON file"""
    try:
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(jobs, jsonfile, indent=2, ensure_ascii=False)
        
        print(f"[EXPORT] Successfully exported {len(jobs)} jobs to {filename}")
    except Exception as e:
        print(f"[EXPORT] Error exporting to JSON: {e}")


def main():
    """Main scraper execution"""
    print("=" * 60)
    print("JOB SCRAPER - Starting collection from 5 websites")
    print("=" * 60)
    
    all_jobs = []
    
    # Scrape from each website - ordered to show best data quality first
    # Remote.co is last because it may have "N/A" company names
    scrapers = [
        scrape_workable,      # Two-step: accurate location/job type
        scrape_dynamitejobs,  # Two-step: accurate location/job type
        scrape_remotive,      # Two-step: accurate location/job type
        scrape_mercor,        # Single-step with metadata
        scrape_remoteco       # Single-step: high volume but may have N/A companies
    ]
    
    for scraper in scrapers:
        try:
            jobs = scraper()
            all_jobs.extend(jobs)
        except Exception as e:
            print(f"Error in {scraper.__name__}: {e}")
        
        time.sleep(5)  # Longer delay between websites to avoid rate limits
    
    print("\n" + "=" * 60)
    print(f"COLLECTION COMPLETE - Total jobs scraped: {len(all_jobs)}")
    print("=" * 60)
    
    # Deduplicate jobs
    unique_jobs = deduplicate_jobs(all_jobs)
    
    print(f"\n[FINAL] Unique jobs after deduplication: {len(unique_jobs)}")
    
    # Export to CSV and JSON
    if unique_jobs:
        export_to_csv(unique_jobs)
        export_to_json(unique_jobs)
        
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETED SUCCESSFULLY")
        print(f"Total unique jobs collected: {len(unique_jobs)}")
        print("Output files: jobs.csv, jobs.json")
    else:
        print("\n[WARNING] No jobs were collected!")


if __name__ == "__main__":
    main()
