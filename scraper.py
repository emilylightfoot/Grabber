import cloudscraper  # NEW: Replaces requests for anti-bot
import os
import time
import random
import csv
from itertools import product
from datetime import datetime

def download_pdf(url_template, year, num, folder):
    url = url_template.format(year=year, num=num)
    filename = f"{year}_{num:04d}.pdf"
    filepath = os.path.join(folder, filename)
    
    os.makedirs(folder, exist_ok=True)
    
    if os.path.exists(filepath):
        return True, False, "skipped"
    
    # NEW: Use cloudscraper per request (fresh session avoids blocks)
    scraper = cloudscraper.create_scraper()
    
    try:
        head = scraper.head(url, timeout=10)
        if head.status_code != 200:
            return False, False, f"missed (HEAD {head.status_code})"
        
        response = scraper.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            try:
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True, True, "downloaded"
            except OSError as e:
                if os.path.exists(filepath):
                    os.remove(filepath)
                return False, False, f"error: {e}"
        else:
            return False, False, f"failed: {response.status_code}"
    except Exception as e:  # Broader catch for scraper errors
        return False, False, f"connection error: {str(e)[:50]}..."

def scrape_site(url_template, years, start_num, end_num, folder, site_alias, max_consecutive_miss=1000):  # INCREASED default
    site_folder = os.path.join(folder, site_alias)
    os.makedirs(site_folder, exist_ok=True)
    
    consecutive_miss = 0
    new_downloaded = 0
    total_checked = 0
    logs = []
    csv_logs = []  # [timestamp, year, num, status]
    prev_year = None
    
    all_combos = list(product(years, range(start_num, end_num + 1)))
    total_combos = len(all_combos)
    
    # NEW: Log start
    logs.append(f"--- Scraping {site_alias} ({len(years)} years, {start_num}-{end_num}) ---")
    
    for i, (year, num) in enumerate(all_combos):  # No tqdm; Streamlit handles progress
        # Reset misses per year
        if prev_year != year:
            consecutive_miss = 0
            prev_year = year
            year_folder = os.path.join(site_folder, str(year))
            logs.append(f"--- Starting year {year} ---")
        
        year_folder = os.path.join(site_folder, str(year))
        success, is_new, status = download_pdf(url_template, year, num, year_folder)
        total_checked += 1
        
        log_entry = f"{year}_{num:04d}.pdf: {status}"
        logs.append(log_entry)
        csv_logs.append([datetime.now().isoformat(), year, num, status])
        
        if is_new:
            new_downloaded += 1
            consecutive_miss = 0
        elif not success:
            consecutive_miss += 1
            if consecutive_miss % 100 == 0:
                logs.append(f"Warning: {consecutive_miss} consecutive misses in {year} at {num}")
            if consecutive_miss >= max_consecutive_miss:
                logs.append(f"Stopped year {year} after {consecutive_miss} misses at {num}")
                # Continue to next year, but flag
        else:
            consecutive_miss = 0
        
        time.sleep(random.uniform(0.5, 1.5))  # Polite delay
        
        # Yield every 10 for UI
        if (i + 1) % 10 == 0 or i == total_combos - 1:
            success_rate = (new_downloaded / total_checked * 100) if total_checked > 0 else 0
            progress_val = (i + 1) / total_combos if total_combos > 0 else 1.0
            yield {
                'progress': min(1.0, max(0.0, progress_val)),
                'checked': total_checked,
                'new': new_downloaded,
                'rate': f"{success_rate:.1f}%",
                'log': log_entry,
                'miss_streak': consecutive_miss  # NEW: For UI if wanted
            }
    
    # Save CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(site_folder, f"{site_alias}_logs_{timestamp}.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'year', 'num', 'status'])
        writer.writerows(csv_logs)
    
    # NEW: Total count across years
    total_pdfs = 0
    for y in years:
        y_folder = os.path.join(site_folder, str(y))
        if os.path.exists(y_folder):
            total_pdfs += len([f for f in os.listdir(y_folder) if f.endswith('.pdf')])
    
    yield {
        'progress': 1.0,
        'checked': total_checked,
        'new': new_downloaded,
        'rate': f"{new_downloaded / total_checked * 100:.1f}%" if total_checked > 0 else "0%",
        'complete': True,
        'csv_path': csv_path,
        'logs': logs[-100:],  # Last 100
        'total_pdfs': total_pdfs  # NEW: Grand total
    }

def test_url(url_template, test_year, test_num):
    """Quick test: Use full GET to bypass HEAD blocks"""
    try:
        url = url_template.format(year=test_year, num=test_num)
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, stream=True, timeout=10)  # CHANGED: GET instead of HEAD
        if response.status_code == 200 and 'application/pdf' in response.headers.get('content-type', ''):
            # Quick peek: First 100 bytes should start with PDF magic '%PDF-'
            first_bytes = b''.join(response.iter_content(chunk_size=100))[:4]
            if first_bytes == b'%PDF':
                return True, url
        return False, f"Failed: Status {response.status_code}, Type: {response.headers.get('content-type', 'N/A')}"
    except Exception as e:
        return False, f"Error: {e}"