import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import gc
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
from datetime import datetime
import uuid
import time
import asyncio
import sys
import os

"""
Web scraper with JavaScript support using Playwright.
This module can be used in both regular Python environments and Databricks notebooks.
Environment detection and async handling is automatic.
"""

# Detect if we're running in Databricks
IS_DATABRICKS = 'databricks' in sys.modules or 'DATABRICKS_RUNTIME_VERSION' in os.environ

# Add imports for Playwright
try:
    from playwright.sync_api import sync_playwright
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
    if IS_DATABRICKS:
        print("✅ Playwright (async+sync) is available for JavaScript rendering (Databricks mode)")
    else:
        print("✅ Playwright (sync+async) is available for JavaScript rendering")
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("⚠️ Playwright not available. Install with: pip install playwright")
    print("⚠️ Then run: playwright install")
    raise ImportError("Playwright is required for this script to work properly")

# Global variables
visited_lock = threading.Lock()
queue_lock = threading.Lock()
id_lock = threading.Lock()
visited = set()
queue = deque()
chunk_counter = 0  # Initialize the counter

def crawl_site(start_url, max_pages=1000, max_workers=8, start_url_as_root=False, verbose=True):
    def is_sitemap(url):
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            return res.headers.get("Content-Type", "").lower().startswith("application/xml") or url.endswith(".xml")
        except:
            return False

    def get_all_urls_from_sitemap(sitemap_url):
        urls = []
        to_process = [sitemap_url]
        processed = set()

        while to_process:
            url = to_process.pop()
            if url in processed:
                continue
            processed.add(url)
            try:
                res = requests.get(url, timeout=10)
                res.raise_for_status()
            except Exception as e:
                print(f"Warning: Could not retrieve sitemap {url} ({e}). Skipping.")
                continue
            soup = BeautifulSoup(res.text, 'xml')
            loc_tags = soup.find_all('loc')
            if not loc_tags:
                print(f"No <loc> tags found in {url}. Skipping this sitemap.")
                continue

            for loc in loc_tags:
                link = loc.get_text()
                if link.endswith('.xml'):
                    to_process.append(link)
                else:
                    urls.append(link)
        return urls[:max_pages]

    def is_javascript_heavy(url):
        """Check if a website appears to be JavaScript-heavy by comparing link counts."""
        if not PLAYWRIGHT_AVAILABLE:
            if verbose:
                print("Playwright is required but not available.")
            return False
            
        if verbose:
            print(f"🔍 Checking if {url} is JavaScript-heavy...")
        start_time = time.time()
        try:
            # First check with regular requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }
            resp = requests.get(url, headers=headers, timeout=30)  # Increased timeout
            if resp.status_code != 200:
                return False
                
            html_soup = BeautifulSoup(resp.content, "html.parser")
            regular_links = html_soup.find_all("a", href=True)
            if verbose:
                print(f"  - Found {len(regular_links)} links with regular HTTP request")
            
            # If we find a reasonable number of links, assume it's not JS-heavy
            if len(regular_links) > 10:
                if verbose:
                    print(f"  - Found sufficient links ({len(regular_links)}) without JavaScript. Using regular mode.")
                return False
                
            # Check with Playwright
            if verbose:
                print("  - Testing with Playwright browser rendering...")
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(viewport={"width": 1280, "height": 1080})
                    page = context.new_page()
                    page.goto(url, timeout=30000, wait_until="networkidle")
                    
                    rendered_html = page.content()
                    browser.close()
                    
                    pw_soup = BeautifulSoup(rendered_html, "html.parser")
                    pw_links = pw_soup.find_all("a", href=True)
                    
                    # If Playwright finds significantly more links, it's likely JS-heavy
                    is_js = len(pw_links) > len(regular_links) * 1.5
                    elapsed = time.time() - start_time
                    if verbose:
                        print(f"  - Found {len(pw_links)} links with JavaScript rendering")
                        print(f"  - JavaScript detection took {elapsed:.2f} seconds")
                        if is_js:
                            print(f"🔥 Site is JavaScript-heavy! Will use Playwright for crawling.")
                        else:
                            print(f"📄 Site appears to be regular HTML. Will use standard requests.")
                    return is_js
                    
            except Exception as e:
                if verbose:
                    print(f"❌ Playwright error: {e}")
                return False
            
            return False
                
        except Exception as e:
            if verbose:
                print(f"Error checking if site is JavaScript-heavy: {e}")
            return False

    def get_page_with_javascript(url):
        """Get page content using Playwright for JavaScript rendering."""
        if not PLAYWRIGHT_AVAILABLE:
            if verbose:
                print("Playwright is required but not available.")
            return None, None
            
        start_time = time.time()
        if verbose:
            print(f"🌐 Loading {url} with Playwright browser...")
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    viewport={"width": 1280, "height": 1080},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                
                # More reliable page loading with timeout and wait options
                page.goto(url, timeout=30000, wait_until="networkidle")
                
                # Wait for content to be properly loaded
                page.wait_for_load_state("networkidle")
                
                # Get the rendered content
                content = page.content()
                soup = BeautifulSoup(content, "html.parser")
                
                browser.close()
                
                elapsed = time.time() - start_time
                if verbose:
                    print(f"  - Page loaded with Playwright in {elapsed:.2f} seconds")
                return content, soup
                
        except Exception as e:
            if verbose:
                print(f"⚠️ Playwright error: {e}")
            return None, None

    def crawl_worker(base, path_prefix):
        # Check if the site is JavaScript-heavy on first URL
        js_heavy = None
        worker_id = threading.get_ident() % 10000  # Get a shorter worker ID for logging
        
        while True:
            with queue_lock:
                if not queue or len(visited) >= max_pages:
                    return
                url = queue.popleft()

            with visited_lock:
                if url in visited:
                    continue
                visited.add(url)

            try:
                # Add delay to avoid overwhelming the server
                time.sleep(1)  # 1 second delay between requests
                
                # Check if site is JavaScript-heavy on first URL
                if js_heavy is None:
                    js_heavy = is_javascript_heavy(url)
                    if js_heavy and verbose:
                        print(f"🤖 Worker {worker_id}: Detected JavaScript-heavy site. Using Playwright for: {url}")
                
                start_time = time.time()
                if js_heavy:
                    content, soup = get_page_with_javascript(url)
                    if soup is None:
                        # Fall back to regular requests if Playwright fails
                        if verbose:
                            print(f"⚠️ JavaScript rendering failed, falling back to regular HTTP for: {url}")
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Cache-Control': 'max-age=0',
                        }
                        resp = requests.get(url, headers=headers, timeout=30)  # Increased timeout
                        if resp.status_code != 200:
                            continue
                        soup = BeautifulSoup(resp.content, "html.parser")
                else:
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Cache-Control': 'max-age=0',
                    }
                    resp = requests.get(url, headers=headers, timeout=30)  # Increased timeout
                    if resp.status_code != 200:
                        continue
                    soup = BeautifulSoup(resp.content, "html.parser")
                    
                elapsed = time.time() - start_time
                if verbose:
                    print(f"🕒 Worker {worker_id}: Processed {url} in {elapsed:.2f} seconds")

                with visited_lock:
                    discovered_urls.append(url)
                    # Always show progress for URL counts (key metric)
                    if len(discovered_urls) % 50 == 0:
                        print(f"Crawled {len(discovered_urls)} URLs so far...")

                for a in soup.find_all("a", href=True):
                    link = urljoin(url, a["href"])
                    parsed_link = urlparse(link)
                    if parsed_link.netloc == base and (not start_url_as_root or parsed_link.path.startswith(path_prefix)):
                        with visited_lock:
                            if link not in visited:
                                with queue_lock:
                                    queue.append(link)
            except Exception as e:
                print(f"Error processing {url}: {e}")
                continue

    def crawl_html_site(start_url):
        base = urlparse(start_url).netloc
        path_prefix = urlparse(start_url).path
        with queue_lock:
            queue.append(start_url)

        if max_workers == 1:
            crawl_worker(base, path_prefix)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(crawl_worker, base, path_prefix) for _ in range(max_workers)]
                for f in as_completed(futures):
                    pass

        print(f"✅ Finished crawling. Total URLs discovered: {len(discovered_urls)}")
        return discovered_urls[:max_pages]

    if is_sitemap(start_url):
        print(f"Detected sitemap XML. Crawling sitemap: {start_url}")
        urls = get_all_urls_from_sitemap(start_url)
    else:
        print(f"Detected HTML site. Crawling from root page: {start_url}")
        global discovered_urls
        discovered_urls = []
        urls = crawl_html_site(start_url)
        
        # If no URLs were discovered but a specific page was requested, include that page
        if not urls and max_pages == 1:
            print(f"No links found, but processing the specified URL directly: {start_url}")
            urls = [start_url]

    return urls

def extract_date_general(soup):
    text = soup.get_text(" ", strip=True)[:3000]  # trim to reduce log size
    # Remove debug print that clutters output
    pattern = r"(last\s+updated\s+on|updated\s+on|published\s+on)?\s*(\w+\s+\d{1,2},\s+\d{4})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        try:
            return datetime.strptime(match.group(2), "%B %d, %Y").date().isoformat()
        except:
            return match.group(2)
    return None

def extract_published_date(full_soup):
    meta_names = ['pubdate', 'publishdate', 'timestamp', 'dc.date', 'date', 'article:published_time']
    for name in meta_names:
        tag = full_soup.find('meta', attrs={'name': name}) or full_soup.find('meta', attrs={'property': name})
        if tag and tag.get('content'):
            return tag['content']
    time_tag = full_soup.find('time')
    if time_tag and time_tag.get('datetime'):
        return time_tag['datetime']
    return extract_date_general(full_soup)

def extract_text_and_headings(html_content, verbose=True):
    if verbose:
        print("  - Starting content extraction...")
    
    # Check if we have content
    if not html_content or len(html_content.strip()) < 100:
        if verbose:
            print("  - HTML content is empty or too short")
        return "", "", [], None
    
    full_soup = BeautifulSoup(html_content, 'lxml')
    published_date = extract_published_date(full_soup)
    
    if verbose:
        print(f"  - Initial HTML length: {len(html_content)} chars")
    
    # Extract title before any modifications
    title_tag = full_soup.find('title')
    page_title = title_tag.get_text().strip() if title_tag else ""
    if verbose:
        print(f"  - Found title: {page_title}")

    soup = BeautifulSoup(str(full_soup), 'lxml')
    
    # Remove script and style tags
    for tag in soup(['script', 'style']):
        tag.decompose()
            
    # Remove comments
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    # Try to find the main content area using HTML structure
    content_area = None
    
    # 1. Try to find the <main> tag
    main_tag = soup.find('main')
    if main_tag:
        if verbose:
            print("  - Found <main> tag for content")
        content_area = main_tag
    
    # 2. If no main tag, look for common content container IDs/classes
    if not content_area:
        for selector in ['#content', '#main', '.main-content', '.content-main', '.article-content', 
                         '.post-content', '.entry-content', '.page-content', '.document-content',
                         '[role="main"]', 'article', '.article']:
            content_container = soup.select_one(selector)
            if content_container:
                if verbose:
                    print(f"  - Found content container with selector: {selector}")
                content_area = content_container
                break
    
    # If we found a content area, remove navigation elements from the DOM
    if content_area:
        # Remove navigation elements from the DOM before extracting text
        for nav_selector in ['nav', 'header', '.navbar', '.navigation', '.menu', '.sidebar', 
                             '.nav', '.site-header', '.site-nav', '.breadcrumbs',
                             'footer', '.footer', '.site-footer']:
            for nav_elem in soup.select(nav_selector):
                # Don't remove if it's inside our content area
                if content_area not in nav_elem.parents:
                    nav_elem.decompose()
    else:
        # If we couldn't find a specific content area, use the body but remove navigation
        if verbose:
            print("  - No specific content area found, using body with navigation removed")
        content_area = soup.body if soup.body else soup
        
        # Remove common navigation elements
        for nav_selector in ['nav', 'header', '.navbar', '.navigation', '.menu', '.sidebar', 
                             '.nav', '.site-header', '.site-nav', '.breadcrumbs',
                             'footer', '.footer', '.site-footer']:
            for nav_elem in soup.select(nav_selector):
                nav_elem.decompose()
    
    # Extract text with better formatting
    text = content_area.get_text(separator="\n")
    if verbose:
        print(f"  - Raw extracted text length: {len(text)} chars")
    
    # Clean up text - remove excessive whitespace and empty lines
    clean_text = "\n".join([line.strip() for line in text.splitlines() if line.strip()])
    if verbose:
        print(f"  - Cleaned text length: {len(clean_text)} chars")
    
    # Extract headers and their content
    headers_info = []
    headings = content_area.find_all(['h1', 'h2', 'h3'])
    if verbose:
        print(f"  - Found {len(headings)} headings")
    for heading in headings:
        level = heading.name
        heading_text = heading.get_text(" ", strip=True)
        content_chars = 0
        for sibling in heading.find_next_siblings():
            if sibling.name and sibling.name in ['h1', 'h2', 'h3']:
                if ((level == 'h1' and sibling.name == 'h1') or
                    (level == 'h2' and sibling.name in ['h1','h2']) or
                    (level == 'h3' and sibling.name in ['h1','h2','h3'])):
                    break
            content_chars += len(sibling.get_text(" ", strip=True))
        headers_info.append((level, heading_text, content_chars))
    
    return clean_text, page_title, headers_info, published_date

def chunk_text(text, chunk_size=4000, overlap=400):
    chunks = []
    if not text or chunk_size <= 0:
        return chunks
    text_length = len(text)
    overlap = min(overlap, chunk_size - 1)
    step = chunk_size - overlap
    for start in range(0, text_length, step):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)
        if end >= text_length:
            break
    return chunks

def fetch_and_chunk_pages(urls, output_parquet=None, max_workers=8, verbose=True):
    records = []
    global chunk_counter

    def process_url(url):
        global chunk_counter  # Explicitly declare we're using the global variable
        try:
            # First try with regular requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }
            res = requests.get(url, headers=headers, timeout=30)
            res.raise_for_status()
            html = res.text

            # Check if this might be a JavaScript-heavy page with minimal content
            soup = BeautifulSoup(html, 'lxml')
            body_text = soup.body.get_text(" ", strip=True) if soup.body else ""
            
            # If content seems minimal, try with Playwright
            if len(body_text) < 1000:
                if PLAYWRIGHT_AVAILABLE:
                    if verbose:
                        print(f"  - Content seems minimal ({len(body_text)} chars). Trying with JavaScript rendering...")
                    try:
                        # Use the function directly here instead of calling get_page_with_javascript
                        with sync_playwright() as p:
                            browser = p.chromium.launch(headless=True)
                            context = browser.new_context(
                                viewport={"width": 1280, "height": 1080},
                                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                            )
                            page = context.new_page()
                            
                            # More reliable page loading with timeout and wait options
                            page.goto(url, timeout=30000, wait_until="networkidle")
                            
                            # Wait for content to be properly loaded
                            page.wait_for_load_state("networkidle")
                            
                            # Get the rendered content
                            content = page.content()
                            js_soup = BeautifulSoup(content, "html.parser")
                            
                            # Only remove script and style elements to preserve content
                            try:
                                # Remove scripts
                                scripts = page.query_selector_all('script')
                                for script in scripts:
                                    try:
                                        script.evaluate("el => el.remove()")
                                    except:
                                        pass
                                        
                                # Remove styles
                                styles = page.query_selector_all('style')
                                for style in styles:
                                    try:
                                        style.evaluate("el => el.remove()")
                                    except:
                                        pass
                                        
                                if verbose:
                                    print("    - Removed script and style elements")
                            except Exception as e:
                                if verbose:
                                    print(f"    - Error removing script/style elements: {e}")
                                    
                            # Get the cleaned content after removing boilerplate
                            cleaned_content = page.content()
                            js_soup = BeautifulSoup(cleaned_content, "html.parser")
                            
                            browser.close()
                            
                            html = cleaned_content
                            # Check if we got more content with JavaScript rendering
                            js_body_text = js_soup.body.get_text(" ", strip=True) if js_soup.body else ""
                            if verbose:
                                print(f"  - Regular request: {len(body_text)} chars")
                                print(f"  - JavaScript render: {len(js_body_text)} chars")
                            
                            if len(js_body_text) > len(body_text) * 1.2:
                                if verbose:
                                    print(f"  - JavaScript rendering found {len(js_body_text) - len(body_text)} more characters!")
                    except Exception as e:
                        if verbose:
                            print(f"  - JavaScript rendering failed: {e}, using regular HTML content")
            
            if "text/html" not in res.headers.get("Content-Type", ""):
                return []
            if len(html) > 1_000_000:
                return []

            clean_text, page_title, headers_info, published_date = extract_text_and_headings(html, verbose)
            headers_summary = "; ".join(
                f"{level.upper()}: {text} ({chars} chars)" for level, text, chars in headers_info
            )
            chunks = chunk_text(clean_text)
            
            result = []
            for idx, chunk in enumerate(chunks):
                with id_lock:
                    chunk_id = f"chunk_{chunk_counter}"
                    chunk_counter += 1
                
                result.append({
                    "id": chunk_id,
                    "url": url,
                    "title": page_title,
                    "chunk_index": idx,
                    "text": chunk,
                    "headers_char_count": headers_summary,
                    "published_date": published_date
                })
            return result
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return []

    if max_workers == 1:
        for i, url in enumerate(urls, 1):
            result = process_url(url)
            records.extend(result)
            if i % 10 == 0 or i == len(urls):
                print(f"[{i}/{len(urls)}] Processed")
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_url, url): url for url in urls}
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                records.extend(result)
                if i % 10 == 0 or i == len(urls):
                    print(f"[{i}/{len(urls)}] Processed")

    df = pd.DataFrame(records)
    if output_parquet:
        df.to_parquet(output_parquet, index=False)
        print(f"Saved {len(df)} chunks to {output_parquet}")
    return df

def scrape_website(url, max_pages=20, max_workers=2, output_file=None, verbose=True):
    """
    Main function to scrape a website with JavaScript support.
    
    Args:
        url (str): The URL to scrape (can be a website or sitemap URL)
        max_pages (int): Maximum number of pages to scrape
        max_workers (int): Number of worker threads
        output_file (str): Optional output file path (CSV). If None, no file is saved.
        
    Returns:
        pandas.DataFrame: DataFrame containing the extracted content
    """
    print(f"🚀 Starting crawler with Playwright support for JavaScript-heavy sites")
    print(f"🔗 Target URL: {url}")
    print(f"📊 Max pages: {max_pages}, Workers: {max_workers}")
    
    try:
        # Install required packages if missing
        try:
            import pyarrow
        except ImportError:
            print("Installing pyarrow for parquet support...")
            import subprocess
            subprocess.check_call(["pip", "install", "pyarrow"])
            
        # Crawl the site with specified parameters
        discovered_urls = crawl_site(url, max_pages=max_pages, max_workers=max_workers, verbose=verbose)
        
        if discovered_urls:
            print(f"✅ Found {len(discovered_urls)} URLs to process")
            if verbose:
                print("Starting content extraction...")
            
            # Process the discovered URLs
            extracted_docs = fetch_and_chunk_pages(discovered_urls, max_workers=max_workers, verbose=verbose)
            
            # Check if the DataFrame is empty before accessing columns
            if not extracted_docs.empty:
                # Save to CSV if output_file is provided
                if output_file:
                    extracted_docs.to_csv(output_file, index=False)
                    print(f"✅ Data saved to {output_file}")
                
                print(f"✅ Successfully extracted {len(extracted_docs)} text chunks")
                
                print("Sample titles:")
                for title in extracted_docs['title'].unique()[:5]:
                    print(f"  - {title}")
                    
                return extracted_docs
            else:
                print("❌ No data was processed successfully.")
                return pd.DataFrame()
        else:
            print("❌ No URLs were discovered. Check the site or your internet connection.")
            return pd.DataFrame()
    
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user.")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return pd.DataFrame()

def scrape_single_url(url, use_javascript=None, verbose=True):
    """
    Scrape a single URL without crawling, useful for direct content extraction in notebooks.
    
    Args:
        url (str): The URL to scrape
        use_javascript (bool): Force using JavaScript rendering. If None, auto-detect.
        
    Returns:
        tuple: (text, title, headers_info, published_date) - The extracted content
    """
    if verbose:
        print(f"🔍 Scraping single URL: {url}")
    
    try:
        # Check if we should use JavaScript
        if use_javascript is None:
            if PLAYWRIGHT_AVAILABLE:
                # Use a simplified version of is_javascript_heavy logic directly
                if verbose:
                    print(f"  - Auto-detecting if {url} is JavaScript-heavy...")
                try:
                    # First check with regular requests
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Cache-Control': 'max-age=0',
                    }
                    resp = requests.get(url, headers=headers, timeout=30)
                    if resp.status_code != 200:
                        use_javascript = True
                    else:
                        html_soup = BeautifulSoup(resp.content, "html.parser")
                        regular_links = html_soup.find_all("a", href=True)
                        if verbose:
                            print(f"  - Found {len(regular_links)} links with regular HTTP request")
                        
                        # If we find a reasonable number of links, assume it's not JS-heavy
                        if len(regular_links) > 10:
                            if verbose:
                                print(f"  - Found sufficient links ({len(regular_links)}) without JavaScript. Using regular mode.")
                            use_javascript = False
                        else:
                            use_javascript = True
                            if verbose:
                                print(f"  - Found few links ({len(regular_links)}). Will use JavaScript rendering.")
                except Exception as e:
                    if verbose:
                        print(f"  - Error in auto-detection: {e}")
                    use_javascript = True
            else:
                use_javascript = False
                
        # Get the page content
        if use_javascript and PLAYWRIGHT_AVAILABLE:
            if verbose:
                print(f"  - Using JavaScript rendering for {url}")
            # Implement the JavaScript rendering logic directly here
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        viewport={"width": 1280, "height": 1080},
                        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                    page = context.new_page()
                    
                    # More reliable page loading with timeout and wait options
                    page.goto(url, timeout=30000, wait_until="networkidle")
                    
                    # Wait for content to be properly loaded
                    page.wait_for_load_state("networkidle")
                    
                    # Only remove script and style elements to preserve content
                    try:
                        # Remove scripts
                        scripts = page.query_selector_all('script')
                        for script in scripts:
                            try:
                                script.evaluate("el => el.remove()")
                            except:
                                pass
                                
                        # Remove styles
                        styles = page.query_selector_all('style')
                        for style in styles:
                            try:
                                style.evaluate("el => el.remove()")
                            except:
                                pass
                                
                        if verbose:
                            print("    - Removed script and style elements")
                    except Exception as e:
                        if verbose:
                            print(f"    - Error removing script/style elements: {e}")
                            
                    # Get the cleaned content after removing boilerplate
                    content = page.content()
                    
                    browser.close()
                    
                    # Print content length for debugging
                    if verbose:
                        print(f"  - JavaScript rendered content length: {len(content)} chars")
                    
            except Exception as e:
                if verbose:
                    print(f"⚠️ JavaScript rendering failed: {e}, falling back to regular HTTP")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                }
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                content = resp.text
        else:
            if verbose:
                print(f"  - Using regular HTTP request for {url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            content = resp.text
            
        # Extract the content
        clean_text, page_title, headers_info, published_date = extract_text_and_headings(content, verbose)
        return clean_text, page_title, headers_info, published_date
        
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return "", "", [], None

def scrape_url_to_dataframe(url, use_javascript=None, verbose=True):
    """
    Scrape a single URL and return a DataFrame with the extracted content.
    Automatically detects environment (Databricks vs regular Python) and handles async internally.
    
    Args:
        url (str): The URL to scrape
        use_javascript (bool): Force using JavaScript rendering. If None, auto-detect.
        
    Returns:
        pandas.DataFrame: DataFrame with the extracted content
    """
    def _create_dataframe_from_content(clean_text, page_title, headers_info, published_date):
        """Helper function to create DataFrame from extracted content."""
        if verbose:
            print(f"  - Extracted text length: {len(clean_text)} chars")
            print(f"  - Title: {page_title}")
        
        if not clean_text:
            if verbose:
                print("  - No content extracted, returning empty DataFrame")
            return pd.DataFrame()
            
        # Create chunks
        chunks = chunk_text(clean_text)
        if verbose:
            print(f"  - Created {len(chunks)} chunks from text")
        
        # Create records
        records = []
        for idx, chunk in enumerate(chunks):
            chunk_id = f"chunk_{idx}"
            
            headers_summary = "; ".join(
                f"{level.upper()}: {text} ({chars} chars)" for level, text, chars in headers_info
            )
            
            records.append({
                "id": chunk_id,
                "url": url,
                "title": page_title,
                "chunk_index": idx,
                "text": chunk,
                "headers_char_count": headers_summary,
                "published_date": published_date
            })
        
        if verbose:
            print(f"  - Created {len(records)} records")
        return pd.DataFrame(records)
    
    if IS_DATABRICKS:
        # In Databricks, use async but handle it internally
        import asyncio
        try:
            # Try to get the current event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're already in an event loop, create a task
                import concurrent.futures
                import threading
                
                def run_in_thread():
                    # Create a new event loop in a separate thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(scrape_single_url_async(url, use_javascript))
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    clean_text, page_title, headers_info, published_date = future.result()
            else:
                # No running loop, we can use asyncio.run
                clean_text, page_title, headers_info, published_date = asyncio.run(scrape_single_url_async(url, use_javascript))
        except RuntimeError:
            # Fallback to thread-based approach
            import concurrent.futures
            import threading
            
            def run_in_thread():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(scrape_single_url_async(url, use_javascript))
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                clean_text, page_title, headers_info, published_date = future.result()
    else:
        # Regular environment, use sync version
        clean_text, page_title, headers_info, published_date = scrape_single_url(url, use_javascript, verbose)
    
    return _create_dataframe_from_content(clean_text, page_title, headers_info, published_date)

def scrape_urls_batch(urls, max_workers=4, use_javascript=None, verbose=True):
    """
    Scrape multiple URLs in parallel and return a DataFrame.
    Automatically detects environment (Databricks vs regular Python) and handles async internally.
    
    Args:
        urls (list): List of URLs to scrape
        max_workers (int): Number of worker threads
        use_javascript (bool): Force using JavaScript rendering. If None, auto-detect for each URL.
        
    Returns:
        pandas.DataFrame: DataFrame with the extracted content from all URLs
    """
    if verbose:
        print(f"🔍 Batch scraping {len(urls)} URLs")
    all_records = []
    
    if IS_DATABRICKS:
        # In Databricks, handle async internally
        import asyncio
        import concurrent.futures
        import threading
        
        def run_batch_in_thread():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                async def process_urls_async():
                    results = []
                    for i, url in enumerate(urls, 1):
                        clean_text, page_title, headers_info, published_date = await scrape_single_url_async(url, use_javascript)
                        
                        if clean_text:
                            chunks = chunk_text(clean_text)
                            for idx, chunk in enumerate(chunks):
                                chunk_id = f"chunk_{len(results) + idx}"
                                headers_summary = "; ".join(
                                    f"{level.upper()}: {text} ({chars} chars)" for level, text, chars in headers_info
                                )
                                results.append({
                                    "id": chunk_id,
                                    "url": url,
                                    "title": page_title,
                                    "chunk_index": idx,
                                    "text": chunk,
                                    "headers_char_count": headers_summary,
                                    "published_date": published_date
                                })
                        
                        print(f"[{i}/{len(urls)}] Processed {url}")
                    
                    return results
                
                return new_loop.run_until_complete(process_urls_async())
            finally:
                new_loop.close()
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(run_batch_in_thread)
            all_records = future.result()
    else:
        # Regular environment, use sync version
        def process_url(url):
            clean_text, page_title, headers_info, published_date = scrape_single_url(url, use_javascript, verbose)
            
            if not clean_text:
                return []
                
            chunks = chunk_text(clean_text)
            records = []
            for idx, chunk in enumerate(chunks):
                chunk_id = f"chunk_{len(all_records) + len(records)}"
                headers_summary = "; ".join(
                    f"{level.upper()}: {text} ({chars} chars)" for level, text, chars in headers_info
                )
                records.append({
                    "id": chunk_id,
                    "url": url,
                    "title": page_title,
                    "chunk_index": idx,
                    "text": chunk,
                    "headers_char_count": headers_summary,
                    "published_date": published_date
                })
            return records
        
        if max_workers == 1:
            for i, url in enumerate(urls, 1):
                records = process_url(url)
                all_records.extend(records)
                print(f"[{i}/{len(urls)}] Processed {url}")
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_url, url): url for url in urls}
                for i, future in enumerate(as_completed(futures), 1):
                    records = future.result()
                    all_records.extend(records)
                    print(f"[{i}/{len(urls)}] Processed")
    
    if all_records:
        return pd.DataFrame(all_records)
    else:
        return pd.DataFrame()

# Add async versions of key functions for Databricks

async def get_page_with_javascript_async(url):
    """Async version of get_page_with_javascript for Databricks environments."""
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright is required but not available.")
        return None, None
        
    start_time = time.time()
    print(f"🌐 Loading {url} with Playwright browser (async)...")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            
            # More reliable page loading with timeout and wait options
            await page.goto(url, timeout=60000, wait_until="networkidle")
            
            # Wait for content to be properly loaded
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(3)  # Additional wait time for dynamic content
            
            # Only remove script and style elements to preserve content
            try:
                # Remove scripts
                scripts = await page.query_selector_all('script')
                for script in scripts:
                    try:
                        await script.evaluate("el => el.remove()")
                    except:
                        pass
                        
                # Remove styles
                styles = await page.query_selector_all('style')
                for style in styles:
                    try:
                        await style.evaluate("el => el.remove()")
                    except:
                        pass
                        
                print("    - Removed script and style elements")
            except Exception as e:
                print(f"    - Error removing script/style elements: {e}")
                    
            # Get the cleaned content after removing boilerplate
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            await browser.close()
            
            elapsed = time.time() - start_time
            print(f"  - Page loaded with Playwright in {elapsed:.2f} seconds (async)")
            return content, soup
            
    except Exception as e:
        print(f"⚠️ Playwright error (async): {e}")
        return None, None

async def scrape_single_url_async(url, use_javascript=None):
    """
    Async version of scrape_single_url for Databricks environments.
    
    Args:
        url (str): The URL to scrape
        use_javascript (bool): Force using JavaScript rendering. If None, auto-detect.
        
    Returns:
        tuple: (text, title, headers_info, published_date) - The extracted content
    """
    print(f"🔍 Scraping single URL (async): {url}")
    
    try:
        # Always use JavaScript in async mode for simplicity
        use_javascript = True if use_javascript is None else use_javascript
                
        # Get the page content
        if use_javascript:
            print(f"  - Using JavaScript rendering for {url}")
            content, soup = await get_page_with_javascript_async(url)
            if content is None:
                print("⚠️ JavaScript rendering failed, falling back to regular HTTP")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0',
                }
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                content = resp.text
        else:
            print(f"  - Using regular HTTP request for {url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            content = resp.text
            
        # Extract the content
        clean_text, page_title, headers_info, published_date = extract_text_and_headings(content)
        return clean_text, page_title, headers_info, published_date
        
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return "", "", [], None