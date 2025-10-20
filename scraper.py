def test_url(url_template, test_year, test_num):
    """Quick HEAD test for one combo"""
    try:
        url = url_template.format(year=test_year, num=test_num)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        head = requests.head(url, headers=headers, timeout=10, allow_redirects=True)  # Added allow_redirects
        return head.status_code == 200, url
    except Exception as e:
        return False, f"Error: {e}"

# Similarly, ensure download_pdf has the same headers (as in my previous update)