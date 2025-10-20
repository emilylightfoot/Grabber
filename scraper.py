import cloudscraper
import os
import time
import random
from tqdm import tqdm  # pip install tqdm if not installed

def download_pdf(year, num, folder='downloads'):
    url = f"https://dier.gov.mt/wp-content/uploads/decrees/{year}/{num:04d}.pdf"
    filename = f"{year}_{num:04d}.pdf"
    filepath = os.path.join(folder, filename)
    
    os.makedirs(folder, exist_ok=True)
    
    if os.path.exists(filepath):
        print(f"Skipped (exists): {filename}")
        return True, False  # Success, not new
    
    scraper = cloudscraper.create_scraper()
    head = scraper.head(url)
    if head.status_code != 200:
        return False, False  # Fail, not new
    
    response = scraper.get(url, stream=True)
    if response.status_code == 200:
        try:
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {filename}")
            return True, True  # Success, new
        except OSError as e:
            print(f"Write error for {filename}: {e} (skipping)")
            if os.path.exists(filepath):
                os.remove(filepath)
            return False, False  # Fail, not new
    else:
        print(f"Failed: {filename} (status {response.status_code})")
        return False, False  # Fail, not new

def scrape_year(year, start_num=1, end_num=4100, max_consecutive_miss=1000):
    folder = f'downloads/{year}'  # Separate folder per year
    os.makedirs(folder, exist_ok=True)
    
    consecutive_miss = 0
    new_downloaded = 0
    total_checked = 0
    
    pbar = tqdm(range(start_num, end_num + 1), total=end_num - start_num + 1, 
               desc=f"Scraping {year} ({start_num}-{end_num})", unit="file")
    
    for num in pbar:
        success, is_new = download_pdf(year, num, folder=folder)
        total_checked += 1
        
        if is_new:
            new_downloaded += 1
        
        time.sleep(random.uniform(0.5, 1.5))
        
        # Update bar
        success_rate = (new_downloaded / total_checked * 100) if total_checked > 0 else 0
        pbar.set_postfix({
            'New': new_downloaded,
            'Rate': f"{success_rate:.1f}%",
            'Miss Streak': consecutive_miss
        })
        
        if not success:
            consecutive_miss += 1
            if consecutive_miss % 100 == 0:  # Warn on long streaks
                print(f"\nWarning: {consecutive_miss} consecutive misses at {num} - continuing...")
            if consecutive_miss >= max_consecutive_miss:
                print(f"\nStopped year {year} after {consecutive_miss} misses at {num}")
                break
        else:
            consecutive_miss = 0
    
    pbar.close()
    return new_downloaded, total_checked

def count_pdfs(folder='downloads'):
    total = 0
    if os.path.exists(folder):
        for subdir in os.listdir(folder):
            subpath = os.path.join(folder, subdir)
            if os.path.isdir(subpath):
                total += len([f for f in os.listdir(subpath) if f.endswith('.pdf')])
    return total

def main():
    year = 2023
    start_num = 1  # Full start for 2023 (decrees begin around 2800, but start low to catch all)
    end_num = 4100  # Covers up to end of 2023 (before 2024's 4100)
    max_miss = 1000  # High tolerance for gaps (e.g., 1000+ misses OK)
    print(f"\n--- Full scrape: Year {year} from {start_num} to {end_num} (miss limit: {max_miss}) ---")
    print("Note: Gaps are normal—script now warns on 100+ misses but pushes through.")
    new_count, total = scrape_year(year, start_num=start_num, end_num=end_num, max_consecutive_miss=max_miss)
    total_pdfs = count_pdfs()
    year_pdfs = len([f for f in os.listdir(f'downloads/{year}') if f.endswith('.pdf')]) if os.path.exists(f'downloads/{year}') else 0
    print(f"\nThis run: {new_count} new PDFs out of {total} checked")
    print(f"Success rate: {new_count / total * 100:.1f}%")
    print(f"Grand total across all years: {total_pdfs} PDFs")
    print(f"{year} total now: {year_pdfs} PDFs")

if __name__ == "__main__":
    main()