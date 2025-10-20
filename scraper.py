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
    
    head = requests.head(url, timeout=10)
    if head.status_code != 200:
        return False, False, "missed"
    
    response = requests.get(url, stream=True, timeout=30)
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

def scrape_site(url_template, years, start_num, end_num, folder, site_alias, max_consecutive_miss=100):
    site_folder = os.path.join(folder, site_alias)
    os.makedirs(site_folder, exist_ok=True)
    
    consecutive_miss = 0
    new_downloaded = 0
    total_checked = 0
    logs = []
    csv_logs = []  # For export: [timestamp, year, num, status]
    
    all_combos = list(product(years, range(start_num, end_num + 1)))
    total_combos = len(all_combos)
    
    # Progress generator: yield updates every 10 checks
    pbar = tqdm(all_combos, total=total_combos, desc=f"Scraping {site_alias} ({len(years)} years)")
    for i, (year, num) in enumerate(pbar):
        year_folder = os.path.join(site_folder, str(year))
        success, is_new, status = download_pdf(url_template, year, num, year_folder)
        total_checked += 1
        
        if is_new:
            new_downloaded += 1
        log_entry = f"{year}_{num:04d}.pdf: {status}"
        logs.append(log_entry)
        csv_logs.append([datetime.now().isoformat(), year, num, status])
        
        if not success:
            consecutive_miss += 1
            if consecutive_miss >= max_consecutive_miss:
                logs.append(f"Stopped after {consecutive_miss} misses at year {year}, num {num}")
                break
        else:
            consecutive_miss = 0
        
        time.sleep(random.uniform(0.5, 1.5))
        
        # Yield progress every 10 for UI efficiency
        if (i + 1) % 10 == 0 or i == total_combos - 1:
            success_rate = (new_downloaded / total_checked * 100) if total_checked > 0 else 0
            progress_val = min(1.0, max(0.0, (i + 1) / total_combos if total_combos > 0 else 1.0))
            yield {
                'progress': progress_val,  # Clamped 0.0 to 1.0 for Streamlit
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
        'progress': 1.0,  # 100% as 1.0
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
        head = requests.head(url, timeout=10)
        return head.status_code == 200, url
    except Exception as e:
        return False, f"Error: {e}"