# Databricks notebook source
dbutils.widgets.text("catalog", "users")
dbutils.widgets.text("schema", "rafi_kurlansik")
dbutils.widgets.text("table", "dbdocs_rag_chunks")
dbutils.widgets.text("url", "https://docs.databricks.com/en/doc-sitemap.xml")

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = dbutils.widgets.get("table")
URL = dbutils.widgets.get("url")

# COMMAND ----------

print(CATALOG)
print(SCHEMA)
print(TABLE)
print(URL)

# COMMAND ----------

import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import gc
from urllib.parse import urljoin, urlparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

visited_lock = threading.Lock()
queue_lock = threading.Lock()
visited = set()
queue = deque()

def crawl_site(start_url, max_pages=1000, max_workers=8):
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

    def crawl_worker(base):
        local_count = 0
        while True:
            with queue_lock:
                if not queue or len(visited) >= max_pages:
                    return
                url = queue.popleft()

            with visited_lock:
                if url in visited:
                    continue
                visited.add(url)
                local_count = len(visited)

            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.content, "html.parser")

                with visited_lock:
                    discovered_urls.append(url)
                    if len(discovered_urls) % 50 == 0:
                        print(f"Crawled {len(discovered_urls)} URLs so far...")

                for a in soup.find_all("a", href=True):
                    link = urljoin(url, a["href"])
                    if urlparse(link).netloc == base:
                        with visited_lock:
                            if link not in visited:
                                with queue_lock:
                                    queue.append(link)
            except:
                continue

    def crawl_html_site(start_url):
        base = urlparse(start_url).netloc
        with queue_lock:
            queue.append(start_url)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(crawl_worker, base) for _ in range(max_workers)]
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

    return urls

def extract_text_and_headings(html_content):
    soup = BeautifulSoup(html_content, 'lxml')

    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'form']):
        tag.decompose()
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    title_tag = soup.find('title')
    page_title = title_tag.get_text().strip() if title_tag else ""

    body = soup.body if soup.body else soup
    text = body.get_text(separator="\n")
    clean_text = "\n".join([line.strip() for line in text.splitlines() if line.strip()])

    headers_info = []
    headings = body.find_all(['h1', 'h2', 'h3'])
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
    return clean_text, page_title, headers_info

def chunk_text(text, chunk_size=800, overlap=80):
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

def fetch_and_chunk_pages(urls, output_parquet=None, max_workers=8):
    records = []

    def process_url(url):
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            html = res.text

            if "text/html" not in res.headers.get("Content-Type", ""):
                return []
            if len(html) > 1_000_000:
                return []

            clean_text, page_title, headers_info = extract_text_and_headings(html)
            headers_summary = "; ".join(
                f"{level.upper()}: {text} ({chars} chars)" for level, text, chars in headers_info
            )
            chunks = chunk_text(clean_text, chunk_size=800, overlap=80)
            return [{
                "url": url,
                "title": page_title,
                "chunk_index": idx,
                "text": chunk,
                "headers_char_count": headers_summary,
            } for idx, chunk in enumerate(chunks)]
        except Exception as e:
            print(f"Error processing {url}: {e}")
            return []

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

# COMMAND ----------

urls = crawl_site(URL, max_pages=10000, max_workers=16)
print(f"Found {len(urls)} pages.")

# COMMAND ----------

df_chunks = fetch_and_chunk_pages(urls, max_workers=16)
display(df_chunks)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

chunks_sdf = spark.createDataFrame(df_chunks)
chunks_sdf = chunks_sdf.withColumn("id", monotonically_increasing_id())

chunks_sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE}")
spark.sql(
    f"ALTER TABLE {CATALOG}.{SCHEMA}.{TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)