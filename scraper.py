import requests
import os
import time
import random
import csv
from tqdm import tqdm
from itertools import product
from datetime import datetime

def download_pdf(url_template, year, num, folder):
    url = url_template.format(year=year, num=num)
    filename = f"{year}_{num:04d}.pdf"
    filepath = os.path.join(folder, filename)
    
    os.makedirs(folder, exist_ok=True)
    
    if os.path.exists(filepath):
        return True, False, "skipped"
    
    # NEW: Add User-Agent to mimic a browser (fixes connection refusals)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        head = requests.head(url, headers=headers, timeout=10)
        if head.status_code != 200:
            return False, False, f"missed (HEAD {head.status_code})"
        
        response = requests.get(url, headers=headers, stream=True, timeout=30)
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
    except requests.exceptions.RequestException as e:
        # NEW: Better handling for connection errors
        return False, False, f"connection error: {str(e)[:50]}..."  # Truncate long errors

def scrape_site(url_template, years, start_num, end_num, folder, site_alias, max_consecutive_miss=500):  # INCREASED: From 100 to 500
    site_folder = os.path.join(folder, site_alias)
    os.makedirs(site_folder, exist_ok=True)
    
    consecutive_miss = 0
    new_downloaded = 0
    total_checked = 0
    logs = []
    csv_logs = []  # For export: [timestamp, year, num, status]
    prev_year = None  # NEW: Track year for resetting misses
    
    all_combos = list(product(years, range(start_num, end_num + 1)))
    total_combos = len(all_combos)
    
    # Progress generator: yield updates every 10 checks
    pbar = tqdm(all_combos, total=total_combos, desc=f"Scraping {site_alias} ({len(years)} years)")
    for i, (year, num) in enumerate(pbar):
        # NEW: Reset misses at the start of each year
        if prev_year != year:
            consecutive_miss = 0
            prev_year = year
            logs.append(f"--- Starting year {year} ---")
        
        year_folder = os.path.join(site_folder, str(year))
        success, is_new, status = download_pdf(url_template, year, num, year_folder)
        total_checked += 1
        
        if is_new:
            new_downloaded += 1
            consecutive_miss = 0  # Reset on success
        elif not success:
            consecutive_miss += 1
            if consecutive_miss >= max_consecutive_miss:
                logs.append(f"Stopped year {year} after {consecutive_miss} consecutive misses at num {num}")
                break  # Break only the year? No, but since per-year reset, it will continue next year
        else:
            consecutive_miss = 0  # Success resets (even if skipped/exists)
        
        log_entry = f"{year}_{num:04d}.pdf: {status}"
        logs.append(log_entry)
        csv_logs.append([datetime.now().isoformat(), year, num, status])
        
        # INCREASED: Slightly longer polite delay
        time.sleep(random.uniform(1.0, 2.0))
        
        # Yield progress every 10 for UI efficiency
        if (i + 1) % 10 == 0 or i == total_combos - 1:
            success_rate = (new_downloaded / total_checked * 100) if total_checked > 0 else 0
            progress_val = (i + 1) / total_combos if total_combos > 0 else 1.0
            yield {
                'progress': min(1.0, max(0.0, progress_val)),  # Ensure 0-1 clamp
                'checked': total_checked,
                'new': new_downloaded,
                'rate': f"{success_rate:.1f}%",
                'log': log_entry
            }
    
    # Save CSV
    csv_path = os.path.join(site_folder, f"{site_alias}_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'year', 'num', 'status'])
        writer.writerows(csv_logs)
    
    pbar.close()
    yield {
        'progress': 1.0,
        'checked': total_checked,
        'new': new_downloaded,
        'rate': f"{new_downloaded / total_checked * 100:.1f}%" if total_checked > 0 else "0%",
        'complete': True,
        'csv_path': csv_path,
        'logs': logs[-100:]  # Last 100 for display
    }

def test_url(url_template, test_year, test_num):
    """Quick HEAD test for one combo"""
    try:
        url = url_template.format(year=test_year, num=test_num)
        # NEW: Add same User-Agent as download
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        head = requests.head(url, headers=headers, timeout=10)
        return head.status_code == 200, url
    except Exception as e:
        return False, f"Error: {e}"