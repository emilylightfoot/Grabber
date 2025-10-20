import streamlit as st
from scraper import scrape_site, test_url
import os

st.title("Universal PDF Bulk Downloader")
st.write("Input a URL template with {year} and {num:04d} placeholders. Test first, then download!")

# Sidebar
st.sidebar.header("Settings")
url_template = st.sidebar.text_input("URL Template", value="https://dier.gov.mt/wp-content/uploads/decrees/{year}/{num:04d}.pdf")
site_alias = st.sidebar.text_input("Site Alias", value="dier")

# Years selection with "All" option for 1977-2025
use_all_years = st.sidebar.checkbox("Select All Years (1977-2025)", value=False)
if use_all_years:
    years = list(range(1977, 2026))  # Full range 1977 to 2025
    st.sidebar.write(f"Selected: All {len(years)} years (1977-2025)")
else:
    years = st.sidebar.multiselect("Years", options=list(range(1977, 2026)), default=[2025])  # Expanded range, default single

start_num = st.sidebar.number_input("Start Num", min_value=1, value=1)  # Default to 1 for older years
st.sidebar.info("💡 For recent years like 2025, try start_num=3000 to skip early misses.")  # NEW: Hint
end_num = st.sidebar.number_input("End Num", min_value=1, value=9999)  # Wide default for full sweep
download_folder = st.sidebar.text_input("Folder", value="./downloads")

# Test section
st.header("Quick Test")
col1, col2 = st.columns(2)
test_year = col1.number_input("Test Year", value=2025, min_value=1977, max_value=2025)
test_num = col2.number_input("Test Num", value=3121)  # CHANGED: To a known good one (from search)
if st.button("Test URL"):
    exists, details = test_url(url_template, test_year, test_num)
    if exists:
        st.success(f"✅ Exists: {details}")
    else:
        st.error(f"❌ Not found: {details}")

# Main run
progress_bar = st.progress(0)
status_text = st.empty()
log_container = st.container()

if st.button("Start Full Download"):
    if years:
        status_text.text("Starting scrape...")
        for update in scrape_site(url_template, years, start_num, end_num, download_folder, site_alias):
            progress_bar.progress(update['progress'])
            status_text.text(f"Checked: {update['checked']} | New: {update['new']} | Rate: {update['rate']}")
            if 'log' in update:
                with log_container:
                    st.text(update['log'])
        
        if update['complete']:
            st.success(f"Done! {update['new']} new PDFs. Logs CSV: {update['csv_path']}")
            with open(update['csv_path'], 'rb') as f:
                st.download_button("Download Logs CSV", f, file_name=os.path.basename(update['csv_path']))
            with log_container:
                st.subheader("Recent Logs")
                for log in update['logs']:
                    st.text(log)
    else:
        st.warning("Select at least one year.")