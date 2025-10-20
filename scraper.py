import requests
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
    
    # ENHANCED: Full browser headers to evade detection (no 403/404 blocks)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://dier.gov.mt/decrees/',  # Fake from decrees index to look legit
        'Accept': 'application/pdf, text/html;q=0.9, */*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    session = requests.Session()  # Reuse session for cookies if needed
    session.headers.update(headers)
    
    try:
        # SKIP HEAD: Go straight to GET (more reliable for this site)
        response = session.get(url, stream=True, timeout=30)
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            if 'application/pdf' in content_type:
                # PDF magic check for extra safety
                first_bytes = b''.join(response.iter_content(chunk_size=100))[:4]
                if first_bytes != b'%PDF':
                    return False, False, f"not PDF (bytes: {first_bytes})"
                
                # Reset stream and download
                response = session.get(url, stream=True, timeout=30)
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True, True, "downloaded"
            else:
                return False, False, f"wrong type: {content_type}"
        else:
            return False, False, f"failed: {response.status_code}"
    except requests.exceptions.RequestException as e:
        return False, False, f"error: {str(e)[:50]}..."
    finally:
        session.close()

def scrape_site(url_template, years, start_num, end_num, folder, site_alias, max_consecutive_miss=1000):
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
    
    logs.append(f"--- Scraping {site_alias} ({len(years)} years, {start_num}-{end_num}) ---")
    
    for i, (year, num) in enumerate(all_combos):
        if prev_year != year:
            consecutive_miss = 0
            prev_year = year
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
        else:
            consecutive_miss = 0
        
        time.sleep(random.uniform(0.5, 1.5))
        
        if (i + 1) % 10 == 0 or i == total_combos - 1:
            success_rate = (new_downloaded / total_checked * 100) if total_checked > 0 else 0
            progress_val = (i + 1) / total_combos if total_combos > 0 else 1.0
            yield {
                'progress': min(1.0, max(0.0, progress_val)),
                'checked': total_checked,
                'new': new_downloaded,
                'rate': f"{success_rate:.1f}%",
                'log': log_entry,
                'miss_streak': consecutive_miss
            }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(site_folder, f"{site_alias}_logs_{timestamp}.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'year', 'num', 'status'])
        writer.writerows(csv_logs)
    
    total_pdfs = sum(len([f for f in os.listdir(os.path.join(site_folder, str(y))) if f.endswith('.pdf')]) for y in years if os.path.exists(os.path.join(site_folder, str(y))))
    
    yield {
        'progress': 1.0,
        'checked': total_checked,
        'new': new_downloaded,
        'rate': f"{new_downloaded / total_checked * 100:.1f}%" if total_checked > 0 else "0%",
        'complete': True,
        'csv_path': csv_path,
        'logs': logs[-100:],
        'total_pdfs': total_pdfs
    }

def test_url(url_template, test_year, test_num):
    """Quick test: GET with PDF validation (no HEAD)"""
    try:
        url = url_template.format(year=test_year, num=test_num)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://dier.gov.mt/decrees/',
            'Accept': 'application/pdf, text/html;q=0.9, */*;q=0.8'
        }
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url, stream=True, timeout=15)
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            if 'application/pdf' in content_type:
                first_bytes = b''.join(response.iter_content(chunk_size=100))[:4]
                if first_bytes == b'%PDF':
                    return True, url
                else:
                    return False, f"Not PDF: First bytes {first_bytes}"
            else:
                # DEBUG: If HTML, peek for clues (e.g., 404/403 message)
                error_hint = response.text[:200].strip() if 'text/html' in content_type else "Unknown"
                return False, f"Wrong type: {content_type}. Hint: {error_hint}"
        else:
            return False, f"Status: {response.status_code}"
    except Exception as e:
        return False, f"Error: {e}"
    finally:
        session.close()