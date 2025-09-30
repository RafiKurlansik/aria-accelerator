# Databricks notebook source
# MAGIC %md
# MAGIC ### Helpers

# COMMAND ----------

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse


def extract_sections_by_headers(soup: BeautifulSoup) -> List[Dict]:
    """
    Extract content sections based on header hierarchy (h1, h2, h3).
    
    Args:
        soup: BeautifulSoup parsed HTML
        
    Returns:
        List of dictionaries with section information
    """
    sections = []
    
    # Find all headers and content
    headers = soup.find_all(['h1', 'h2', 'h3'])
    
    if not headers:
        # No headers found, treat entire content as one section
        text = soup.get_text()
        text = re.sub(r'\s+', ' ', text).strip()
        if text:
            sections.append({
                'header_level': 0,
                'header_text': 'Main Content',
                'content': text,
                'header_hierarchy': []
            })
        return sections
    
    # Track header hierarchy
    header_stack = []
    
    for i, header in enumerate(headers):
        header_level = int(header.name[1])  # h1 -> 1, h2 -> 2, etc.
        header_text = header.get_text().strip()
        
        # Update header hierarchy stack
        # Remove headers of same or lower level
        header_stack = [h for h in header_stack if h['level'] < header_level]
        header_stack.append({'level': header_level, 'text': header_text})
        
        # Get content between this header and the next header
        current_element = header.next_sibling
        section_content = []
        
        while current_element:
            if current_element.name in ['h1', 'h2', 'h3']:
                break
            
            if hasattr(current_element, 'get_text'):
                text = current_element.get_text().strip()
                if text and len(text) > 10:  # Filter out very short content
                    section_content.append(text)
            elif isinstance(current_element, str):
                text = current_element.strip()
                if text and len(text) > 10:
                    section_content.append(text)
            
            current_element = current_element.next_sibling
        
        # Combine section content
        content = ' '.join(section_content).strip()
        content = re.sub(r'\s+', ' ', content)
        
        if content:
            sections.append({
                'header_level': header_level,
                'header_text': header_text,
                'content': content,
                'header_hierarchy': [h['text'] for h in header_stack]
            })
    
    return sections

def chunk_text_with_overlap(text: str, chunk_size: int = 800, overlap: int = 80) -> List[Dict]:
    """
    Split text into overlapping chunks.
    
    Args:
        text: The text to chunk
        chunk_size: Target size for each chunk
        overlap: Number of characters to overlap between chunks
        
    Returns:
        List of dictionaries with chunk information
    """
    if not text or len(text) <= chunk_size:
        return [{"chunk_id": 0, "chunk_text": text, "start_pos": 0, "end_pos": len(text)}]
    
    chunks = []
    start = 0
    chunk_id = 0
    
    while start < len(text):
        # Calculate end position
        end = start + chunk_size
        
        # If this isn't the last chunk, try to break at a word boundary
        if end < len(text):
            # Look for the last space within the chunk to avoid breaking words
            last_space = text.rfind(' ', start, end)
            if last_space > start:
                end = last_space
        
        # Extract chunk
        chunk_text = text[start:end].strip()
        
        if chunk_text:  # Only add non-empty chunks
            chunks.append({
                "chunk_id": chunk_id,
                "chunk_text": chunk_text,
                "start_pos": start,
                "end_pos": end
            })
            chunk_id += 1
        
        # Move start position for next chunk (with overlap)
        start = end - overlap
        
        # Ensure we don't go backwards
        if start <= 0:
            start = end
        
        # Break if we've processed all text
        if start >= len(text):
            break
    
    return chunks

# COMMAND ----------

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import gc
import time
import psutil
import os
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

# Track memory usage
def get_memory_usage():
    """Get current memory usage in GB"""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    return memory_mb / 1024

def force_memory_cleanup():
    """Aggressive memory cleanup"""
    gc.disable()  # Disable automatic GC
    gc.collect()  # Manual collection
    gc.collect()  # Second pass for cycles
    gc.collect()  # Third pass to be sure
    gc.enable()   # Re-enable automatic GC

def check_memory_pressure(max_memory_gb: float = 100.0) -> bool:
    """Check if we're using too much memory"""
    current_memory = get_memory_usage()
    if current_memory > max_memory_gb:
        print(f"⚠️  High memory usage: {current_memory:.1f}GB > {max_memory_gb}GB limit")
        return True
    return False

# Modify batch_scrape_and_chunk for better memory management
def batch_scrape_and_chunk_memory_safe(urls: List[str], 
                                      chunk_size: int = 800, 
                                      overlap: int = 80, 
                                      timeout: int = 15, 
                                      use_headers: bool = True, 
                                      max_content_size: int = 10_000_000,
                                      delay_between_requests: float = 1.0,
                                      gc_frequency: int = 10,  # More frequent
                                      max_memory_gb: float = 50.0,  # Memory limit
                                      save_frequency: int = 100,  # Save every N URLs
                                      output_table: str = None) -> pd.DataFrame:
    """
    Memory-safe batch processing with aggressive cleanup and intermediate saves.
    """
    
    all_chunks = []
    successful_count = 0
    memory_warnings = 0
    
    # Track starting memory
    start_memory = get_memory_usage()
    print(f"🏁 Starting memory usage: {start_memory:.1f}GB")
    
    try:
        for i, url in enumerate(urls):
            current_memory = get_memory_usage()
            print(f"🔄 Processing {i+1}/{len(urls)} | Memory: {current_memory:.1f}GB | {url}")
            
            # Check memory pressure before processing
            if check_memory_pressure(max_memory_gb):
                memory_warnings += 1
                print(f"🧹 AGGRESSIVE CLEANUP #{memory_warnings}")
                
                # Save current progress to avoid losing work
                if all_chunks and output_table:
                    print(f"💾 Emergency save due to memory pressure...")
                    emergency_df = pd.concat(all_chunks, ignore_index=True)
                    # Save logic here (Spark table append)
                    all_chunks = []  # Clear from memory
                
                # Aggressive cleanup
                force_memory_cleanup()
                
                # Check if cleanup helped
                after_cleanup = get_memory_usage()
                print(f"📉 Memory after cleanup: {after_cleanup:.1f}GB")
                
                if after_cleanup > max_memory_gb * 0.8:  # Still too high
                    print(f"❌ Memory still too high after cleanup. Stopping to prevent crash.")
                    break
            
            try:
                df = scrape_and_chunk_url(url, chunk_size, overlap, timeout, use_headers, max_content_size)
                
                if not df.empty:
                    all_chunks.append(df)
                    successful_count += 1
                    print(f"✅ Created {len(df)} chunks")
                    
                    # Immediately clear the df from local scope
                    del df
                else:
                    print(f"⚪ No chunks created")
                
                # Periodic cleanup and saves
                if (i + 1) % gc_frequency == 0:
                    force_memory_cleanup()
                    current_memory = get_memory_usage()
                    print(f"🧹 Cleanup at URL {i+1} | Memory: {current_memory:.1f}GB")
                
                # Periodic saves to prevent losing work
                if (i + 1) % save_frequency == 0 and all_chunks and output_table:
                    print(f"💾 Intermediate save at URL {i+1}")
                    intermediate_df = pd.concat(all_chunks, ignore_index=True)
                    # Save to your Spark table here
                    print(f"Saved {len(intermediate_df)} chunks to {output_table}")
                    
                    # Clear from memory after saving
                    del intermediate_df
                    all_chunks = []
                    force_memory_cleanup()
                
                # Respectful delay
                if delay_between_requests > 0:
                    time.sleep(delay_between_requests)
                    
            except KeyboardInterrupt:
                print(f"\n⚠️  Interrupted by user at URL {i+1}")
                break
            except Exception as e:
                print(f"❌ Failed to process {url}: {e}")
                force_memory_cleanup()  # Cleanup even on errors
                continue
        
        final_memory = get_memory_usage()
        print(f"📊 Final summary:")
        print(f"   • Processed: {successful_count}/{len(urls)} URLs")
        print(f"   • Memory warnings: {memory_warnings}")
        print(f"   • Start memory: {start_memory:.1f}GB")
        print(f"   • Final memory: {final_memory:.1f}GB")
        print(f"   • Memory growth: {final_memory - start_memory:.1f}GB")
        
        if all_chunks:
            print(f"🔄 Final combination of {len(all_chunks)} DataFrames...")
            combined_df = pd.concat(all_chunks, ignore_index=True)
            
            # Final cleanup
            del all_chunks
            force_memory_cleanup()
            
            return combined_df
        else:
            return pd.DataFrame()  # Return empty if everything was saved incrementally
    
    finally:
        # Always clean up
        cleanup_session()
        force_memory_cleanup()

def scrape_and_chunk_url_memory_safe(url: str, chunk_size: int = 800, overlap: int = 80, 
                                    timeout: int = 15, use_headers: bool = True, 
                                    max_content_size: int = 10_000_000) -> pd.DataFrame:
    """
    Memory-safe version with aggressive cleanup
    """
    
    empty_cols = ['url', 'section_id', 'section_header', 'header_level', 
                 'header_hierarchy', 'chunk_id', 'section_chunk_id', 
                 'chunk_text', 'start_pos', 'end_pos', 'content_length', 
                 'header_context'] if use_headers else ['url', 'chunk_id', 'chunk_text', 'start_pos', 'end_pos', 'content_length']
    
    response = None
    soup = None
    
    try:
        # Size check
        is_acceptable, content_size = check_content_size(url, max_content_size, timeout=8)
        
        if content_size:
            size_mb = content_size / (1024 * 1024)
            print(f"📄 Content size: {size_mb:.2f}MB")
        else:
            print(f"📄 Content size: Unknown")
        
        if not is_acceptable:
            return pd.DataFrame(columns=empty_cols)
        
        # Fetch with shorter timeout
        response = scrape_url_with_retry(url, timeout=timeout, max_retries=1)
        if response is None:
            return pd.DataFrame(columns=empty_cols)
        
        print(f"🔍 Parsing HTML...")
        
        # Parse with lxml for speed (if available)
        try:
            soup = BeautifulSoup(response.content, 'lxml')
        except:
            soup = BeautifulSoup(response.content, 'html.parser')
        
        # Immediately close response
        response.close()
        del response
        response = None
        
        # Clean unwanted elements
        for element in soup(["script", "style", "nav", "header", "footer", "aside", "iframe", "embed", "object"]):
            element.decompose()
        
        # Process based on chunking method
        if use_headers:
            sections = extract_sections_by_headers(soup)
            
            # Clean soup immediately after extraction
            soup.decompose()
            del soup
            soup = None
            
            df_data = []
            global_chunk_id = 0
            
            for section_id, section in enumerate(sections):
                chunks = chunk_text_with_overlap(section['content'], chunk_size, overlap)
                
                for chunk in chunks:
                    header_context = ' > '.join(section['header_hierarchy']) if section['header_hierarchy'] else section['header_text']
                    
                    df_data.append({
                        'url': url,
                        'section_id': section_id,
                        'section_header': section['header_text'],
                        'header_level': section['header_level'],
                        'header_hierarchy': ' > '.join(section['header_hierarchy']) if section['header_hierarchy'] else '',
                        'chunk_id': global_chunk_id,
                        'section_chunk_id': chunk['chunk_id'],
                        'chunk_text': chunk['chunk_text'],
                        'start_pos': chunk['start_pos'],
                        'end_pos': chunk['end_pos'],
                        'content_length': len(chunk['chunk_text']),
                        'header_context': header_context
                    })
                    global_chunk_id += 1
            
            # Clear sections from memory
            del sections
            
        else:
            # Traditional chunking
            text = soup.get_text()
            
            # Clean soup immediately
            soup.decompose()
            del soup
            soup = None
            
            text = re.sub(r'\s+', ' ', text).strip()
            lines = text.split('\n')
            cleaned_lines = [line.strip() for line in lines if len(line.strip()) > 10]
            text = ' '.join(cleaned_lines)
            
            chunks = chunk_text_with_overlap(text, chunk_size, overlap)
            
            df_data = []
            for chunk in chunks:
                df_data.append({
                    'url': url,
                    'chunk_id': chunk['chunk_id'],
                    'chunk_text': chunk['chunk_text'],
                    'start_pos': chunk['start_pos'],
                    'end_pos': chunk['end_pos'],
                    'content_length': len(chunk['chunk_text'])
                })
            
            # Clear text and chunks
            del text, chunks
        
        # Create DataFrame
        df = pd.DataFrame(df_data)
        del df_data  # Clear the list
        
        return df
        
    except Exception as e:
        print(f"❌ Error processing {url}: {e}")
        return pd.DataFrame(columns=empty_cols)
        
    finally:
        # Aggressive cleanup of any remaining objects
        if response:
            try:
                response.close()
                del response
            except:
                pass
        
        if soup:
            try:
                soup.decompose()
                del soup
            except:
                pass
        
        # Force garbage collection
        gc.collect()

# COMMAND ----------

# MAGIC %md ### Sitemap to DF

# COMMAND ----------

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import gc
import time
import psutil
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Dict, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime


# Global session and locks for thread safety
_session = None
_lock = Lock()

def sitemap_to_dataframe(sitemap_source: Union[str, bytes], html_only: bool = True, filter_release_notes: bool = True) -> pd.DataFrame:
    """
    Parse a sitemap.xml and return a pandas DataFrame of URLs.
    
    Args:
        sitemap_source: Either a file path to sitemap.xml, URL to sitemap.xml, or XML content as string/bytes
        html_only: If True, filter out non-HTML files (PDFs, images, videos, etc.)
        filter_release_notes: If True, filter out release notes URLs
        
    Returns:
        pandas.DataFrame: DataFrame with columns ['url', 'lastmod', 'changefreq', 'priority']
    """
    
    # Handle different input types
    if isinstance(sitemap_source, str):
        if sitemap_source.startswith(('http://', 'https://')):
            # URL - fetch the sitemap
            response = requests.get(sitemap_source)
            response.raise_for_status()
            xml_content = response.content
        elif sitemap_source.startswith('<?xml'):
            # XML content as string
            xml_content = sitemap_source.encode('utf-8')
        else:
            # File path
            with open(sitemap_source, 'rb') as f:
                xml_content = f.read()
    else:
        # Bytes content
        xml_content = sitemap_source
    
    # Parse XML
    root = ET.fromstring(xml_content)
    
    # Handle namespace - sitemaps typically use this namespace
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    # Extract URL data
    urls_data = []
    
    # Check if this is a sitemap index or regular sitemap
    if root.tag.endswith('sitemapindex'):
        # This is a sitemap index file - extract sitemap URLs
        sitemaps = root.findall('.//ns:sitemap', namespace)
        for sitemap in sitemaps:
            loc = sitemap.find('ns:loc', namespace)
            lastmod = sitemap.find('ns:lastmod', namespace)
            
            if loc is not None:
                urls_data.append({
                    'url': loc.text,
                    'lastmod': lastmod.text if lastmod is not None else None,
                    'changefreq': None,
                    'priority': None,
                    'type': 'sitemap'
                })
    else:
        # Regular sitemap - extract URLs
        urls = root.findall('.//ns:url', namespace)
        for url in urls:
            loc = url.find('ns:loc', namespace)
            lastmod = url.find('ns:lastmod', namespace)
            changefreq = url.find('ns:changefreq', namespace)
            priority = url.find('ns:priority', namespace)
            
            if loc is not None:
                urls_data.append({
                    'url': loc.text,
                    'lastmod': lastmod.text if lastmod is not None else None,
                    'changefreq': changefreq.text if changefreq is not None else None,
                    'priority': priority.text if priority is not None else None,
                    'type': 'url'
                })
    
    # Create DataFrame
    df = pd.DataFrame(urls_data)
    
    # Convert lastmod to datetime if present
    if 'lastmod' in df.columns and not df['lastmod'].isna().all():
        df['lastmod'] = pd.to_datetime(df['lastmod'], errors='coerce')
    
    # Convert priority to float if present
    if 'priority' in df.columns and not df['priority'].isna().all():
        df['priority'] = pd.to_numeric(df['priority'], errors='coerce')
    
    # Apply filtering if requested
    if html_only or filter_release_notes:
        original_count = len(df)
        
        if html_only:
            # Filter out non-HTML files (PDFs, images, videos, office docs, etc.)
            non_html_extensions = [
                '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.zip', '.rar', '.7z', '.tar', '.gz',
                '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
                '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm',
                '.mp3', '.wav', '.ogg', '.m4a',
                '.xml', '.json', '.csv', '.txt'
            ]
            
            # Create a mask for URLs that don't end with non-HTML extensions
            html_mask = ~df['url'].str.lower().str.endswith(tuple(non_html_extensions))
            df = df[html_mask]
            
            print(f"HTML-only filtering: {original_count - len(df)} URLs filtered out")
        
        if filter_release_notes:
            # Filter out release notes URLs
            release_notes_patterns = [
                'release-notes',
                'release_notes',
                'releasenotes',
                'changelog',
                'change-log',
                'reference'
            ]
            
            # Create a mask for URLs that don't contain release notes patterns
            release_notes_mask = ~df['url'].str.lower().str.contains('|'.join(release_notes_patterns), na=False)
            filtered_count = len(df) - len(df[release_notes_mask])
            df = df[release_notes_mask]
            
            print(f"Release notes filtering: {filtered_count} URLs filtered out")
        
        print(f"Final DataFrame: {len(df)} URLs remaining from original {original_count}")
    
    return df

def get_memory_usage():
    """Get current memory usage in GB"""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    return memory_mb / 1024

def force_memory_cleanup():
    """Aggressive memory cleanup"""
    gc.disable()
    gc.collect()
    gc.collect()
    gc.collect()
    gc.enable()

def get_session():
    """Get or create thread-safe session"""
    global _session
    with _lock:
        if _session is None:
            _session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20,
                pool_maxsize=50,
                max_retries=requests.adapters.Retry(
                    total=2,
                    backoff_factor=0.3,
                    status_forcelist=[500, 502, 503, 504, 429, 408]
                )
            )
            _session.mount('http://', adapter)
            _session.mount('https://', adapter)
            _session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache'
            })
    return _session

def cleanup_session():
    """Clean up session"""
    global _session
    with _lock:
        if _session:
            _session.close()
            _session = None

def check_content_size_fast(url: str, max_size: int = 5_000_000, timeout: int = 8) -> Tuple[bool, Optional[int]]:
    """Fast content size check with shorter timeout"""
    try:
        session = get_session()
        response = session.head(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        content_length = response.headers.get('content-length')
        if content_length:
            try:
                size_bytes = int(content_length)
                is_acceptable = size_bytes <= max_size
                return is_acceptable, size_bytes
            except ValueError:
                pass
        return True, None
        
    except:
        return True, None

def scrape_url_safe(url: str, timeout: int = 15, max_retries: int = 2) -> Optional[requests.Response]:
    """Thread-safe URL scraping with retry"""
    session = get_session()
    
    for attempt in range(max_retries + 1):
        try:
            response = session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # Check actual content size
            if 'content-length' in response.headers:
                content_length = int(response.headers['content-length'])
                if content_length > 25_000_000:  # 25MB hard limit
                    response.close()
                    return None
            
            return response
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))
                continue
        except:
            break
    
    return None

def process_single_url_memory_safe(url: str, chunk_size: int = 800, overlap: int = 80, 
                                  timeout: int = 15, max_content_size: int = 5_000_000,
                                  url_counter: Dict = None) -> pd.DataFrame:
    """Process single URL with memory safety and thread safety"""
    
    # Thread-safe counter update
    if url_counter and 'lock' in url_counter:
        with url_counter['lock']:
            url_counter['current'] += 1
            current = url_counter['current']
            total = url_counter['total']
            print(f"🔄 [{current}/{total}] Processing: {url}")
    
    empty_cols = ['url', 'section_id', 'section_header', 'header_level', 
                 'header_hierarchy', 'chunk_id', 'section_chunk_id', 
                 'chunk_text', 'start_pos', 'end_pos', 'content_length', 'header_context']
    
    response = None
    soup = None
    
    try:
        # Quick size check
        is_acceptable, content_size = check_content_size_fast(url, max_content_size, timeout=5)
        
        if content_size:
            size_mb = content_size / (1024 * 1024)
            if size_mb > 1.0:  # Only print for larger files
                print(f"📄 Content: {size_mb:.1f}MB")
        
        if not is_acceptable:
            return pd.DataFrame(columns=empty_cols)
        
        # Fetch with retry
        response = scrape_url_safe(url, timeout, max_retries=1)
        if response is None:
            return pd.DataFrame(columns=empty_cols)
        
        # Parse efficiently
        try:
            soup = BeautifulSoup(response.content, 'lxml')
        except:
            soup = BeautifulSoup(response.content, 'html.parser')
        
        # Immediate cleanup
        response.close()
        del response
        response = None
        
        # Clean DOM
        for element in soup(["script", "style", "nav", "header", "footer", "aside", 
                           "iframe", "embed", "object", "noscript"]):
            element.decompose()
        
        # Extract sections
        sections = extract_sections_by_headers(soup)
        
        # Clean soup immediately
        soup.decompose()
        del soup
        soup = None
        
        # Process sections efficiently
        df_data = []
        global_chunk_id = 0
        
        for section_id, section in enumerate(sections):
            chunks = chunk_text_with_overlap(section['content'], chunk_size, overlap)
            
            for chunk in chunks:
                header_context = ' > '.join(section['header_hierarchy']) if section['header_hierarchy'] else section['header_text']
                
                df_data.append({
                    'url': url,
                    'section_id': section_id,
                    'section_header': section['header_text'],
                    'header_level': section['header_level'],
                    'header_hierarchy': ' > '.join(section['header_hierarchy']) if section['header_hierarchy'] else '',
                    'chunk_id': global_chunk_id,
                    'section_chunk_id': chunk['chunk_id'],
                    'chunk_text': chunk['chunk_text'],
                    'start_pos': chunk['start_pos'],
                    'end_pos': chunk['end_pos'],
                    'content_length': len(chunk['chunk_text']),
                    'header_context': header_context
                })
                global_chunk_id += 1
        
        # Clear sections
        del sections
        
        if df_data:
            df = pd.DataFrame(df_data)
            del df_data
            print(f"✅ Created {len(df)} chunks")
            return df
        else:
            return pd.DataFrame(columns=empty_cols)
        
    except Exception as e:
        print(f"❌ Error processing {url}: {e}")
        return pd.DataFrame(columns=empty_cols)
        
    finally:
        # Cleanup any remaining objects
        if response:
            try:
                response.close()
                del response
            except:
                pass
        
        if soup:
            try:
                soup.decompose()
                del soup
            except:
                pass
        
        # Force GC
        gc.collect()

def save_to_spark_table(df: pd.DataFrame, table_name: str, mode: str = "append"):
    """Save DataFrame to Spark table (placeholder - implement your Spark logic)"""
    try:
        # Replace with your actual Spark save logic
        print(f"💾 Saving {len(df)} chunks to {table_name} (mode: {mode})")
        
        # Example Spark save (uncomment and modify for your setup):
        # spark_df = spark.createDataFrame(df)
        # spark_df.write.mode(mode).saveAsTable(table_name)
        
        return True
    except Exception as e:
        print(f"❌ Error saving to {table_name}: {e}")
        return False

def process_sitemap_with_batched_threads(sitemap_url: str,
                                       chunk_size: int = 800,
                                       overlap: int = 80,
                                       max_urls: Optional[int] = None,
                                       timeout: int = 15,
                                       max_content_size: int = 5_000_000,
                                       batch_size: int = 25,
                                       max_workers: int = 8,
                                       delay_between_batches: float = 2.0,
                                       max_memory_gb: float = 50.0,
                                       save_frequency: int = 100,
                                       table_name: str = "rag_chunks",
                                       html_only: bool = True,
                                       filter_release_notes: bool = True) -> pd.DataFrame:
    """
    Process sitemap with batched threading and aggressive memory management.
    
    Args:
        sitemap_url: URL to sitemap.xml
        chunk_size: Target chunk size (default: 800)
        overlap: Chunk overlap (default: 80)
        max_urls: Maximum URLs to process (None for all)
        timeout: Request timeout (default: 15)
        max_content_size: Max content size in bytes (default: 5MB)
        batch_size: URLs per batch (default: 25)
        max_workers: Thread pool size (default: 8)
        delay_between_batches: Delay between batches (default: 2.0)
        max_memory_gb: Memory limit in GB (default: 50.0)
        save_frequency: Save every N URLs (default: 100)
        table_name: Spark table name (default: "rag_chunks")
        html_only: Filter non-HTML files (default: True)
        filter_release_notes: Filter release notes (default: True)
        
    Returns:
        pandas.DataFrame: Final batch of chunks (rest saved to table)
    """
    
    start_time = time.time()
    start_memory = get_memory_usage()
    
    print(f"🚀 Starting sitemap processing...")
    print(f"📊 Initial memory: {start_memory:.1f}GB")
    print(f"⚙️  Settings: batch_size={batch_size}, workers={max_workers}, memory_limit={max_memory_gb}GB")
    
    try:
        # Parse sitemap
        print(f"📋 Parsing sitemap: {sitemap_url}")
        sitemap_df = sitemap_to_dataframe(sitemap_url, html_only=html_only, filter_release_notes=filter_release_notes)
        
        if sitemap_df.empty:
            print("❌ No URLs found in sitemap")
            return pd.DataFrame()
        
        # Get URLs
        url_df = sitemap_df[sitemap_df['type'] == 'url']
        if url_df.empty:
            print("❌ No content URLs found")
            return pd.DataFrame()
        
        urls = url_df['url'].tolist()
        
        if max_urls:
            urls = urls[:max_urls]
        
        print(f"🎯 Processing {len(urls)} URLs in batches of {batch_size}")
        
        # Initialize counters
        url_counter = {
            'current': 0,
            'total': len(urls),
            'lock': Lock()
        }
        
        successful_count = 0
        memory_warnings = 0
        batch_results = []
        
        # Process in batches
        for batch_start in range(0, len(urls), batch_size):
            batch_end = min(batch_start + batch_size, len(urls))
            batch_urls = urls[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(urls) + batch_size - 1) // batch_size
            
            current_memory = get_memory_usage()
            print(f"\n🔄 Batch {batch_num}/{total_batches} | Memory: {current_memory:.1f}GB | URLs {batch_start+1}-{batch_end}")
            
            # Memory pressure check
            if current_memory > max_memory_gb:
                memory_warnings += 1
                print(f"⚠️  Memory pressure! {current_memory:.1f}GB > {max_memory_gb}GB")
                
                # Emergency save
                if batch_results:
                    emergency_df = pd.concat(batch_results, ignore_index=True)
                    if save_to_spark_table(emergency_df, table_name, "append"):
                        print(f"💾 Emergency saved {len(emergency_df)} chunks")
                        successful_count += len(emergency_df)
                        batch_results = []
                        del emergency_df
                
                # Aggressive cleanup
                print("🧹 Aggressive memory cleanup...")
                force_memory_cleanup()
                
                after_memory = get_memory_usage()
                print(f"📉 Memory after cleanup: {after_memory:.1f}GB")
                
                if after_memory > max_memory_gb * 0.9:
                    print("❌ Memory still too high after cleanup. Stopping.")
                    break
            
            # Process batch with threading
            batch_chunks = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all URLs in current batch
                future_to_url = {
                    executor.submit(
                        process_single_url_memory_safe,
                        url, chunk_size, overlap, timeout, max_content_size, url_counter
                    ): url for url in batch_urls
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_url, timeout=timeout*2):
                    url = future_to_url[future]
                    try:
                        result_df = future.result(timeout=5)
                        if not result_df.empty:
                            batch_chunks.append(result_df)
                    except Exception as e:
                        print(f"❌ Thread error for {url}: {e}")
            
            # Process batch results
            if batch_chunks:
                batch_df = pd.concat(batch_chunks, ignore_index=True)
                batch_results.append(batch_df)
                print(f"✅ Batch {batch_num} complete: {len(batch_df)} chunks")
                
                # Clear batch chunks
                del batch_chunks, batch_df
            
            # Periodic saves
            total_chunks = sum(len(df) for df in batch_results)
            if total_chunks >= save_frequency:
                combined_df = pd.concat(batch_results, ignore_index=True)
                
                if save_to_spark_table(combined_df, table_name, "append"):
                    print(f"💾 Saved {len(combined_df)} chunks to {table_name}")
                    successful_count += len(combined_df)
                    batch_results = []
                    del combined_df
                
                # Cleanup after save
                force_memory_cleanup()
            
            # Inter-batch delay
            if delay_between_batches > 0 and batch_num < total_batches:
                time.sleep(delay_between_batches)
        
        # Final save
        if batch_results:
            final_df = pd.concat(batch_results, ignore_index=True)
            
            if save_to_spark_table(final_df, table_name, "append"):
                print(f"💾 Final save: {len(final_df)} chunks")
                successful_count += len(final_df)
            
            # Return the final batch
            final_result = final_df.copy()
        else:
            final_result = pd.DataFrame()
        
        # Summary
        end_time = time.time()
        end_memory = get_memory_usage()
        duration = (end_time - start_time) / 60
        
        print(f"\n📊 PROCESSING COMPLETE")
        print(f"⏱️  Duration: {duration:.1f} minutes")
        print(f"🎯 URLs processed: {url_counter['current']}/{len(urls)}")
        print(f"✅ Total chunks created: {successful_count}")
        print(f"🧠 Memory: {start_memory:.1f}GB → {end_memory:.1f}GB (Δ{end_memory-start_memory:+.1f}GB)")
        print(f"⚠️  Memory warnings: {memory_warnings}")
        
        return final_result
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Process interrupted by user")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return pd.DataFrame()
        
    finally:
        # Always cleanup
        cleanup_session()
        force_memory_cleanup()
        print("🧹 Final cleanup complete")



# COMMAND ----------

urls_df = sitemap_to_dataframe("https://docs.databricks.com/en/doc-sitemap.xml")
urls_df['url'][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processor

# COMMAND ----------

#!/usr/bin/env python3
"""
Unified Sitemap Processor for RAG Applications
Processes sitemaps with memory-safe threading and saves to Unity Catalog volumes as parquet files.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
import re
import gc
import time
import psutil
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import List, Dict, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# Global session and locks for thread safety
_session = None
_lock = Lock()

def get_memory_usage() -> float:
    """Get current memory usage in GB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024 * 1024)

def force_memory_cleanup():
    """Aggressive memory cleanup"""
    gc.disable()
    for _ in range(3):
        gc.collect()
    gc.enable()

def get_session():
    """Get or create thread-safe session with optimized settings"""
    global _session
    with _lock:
        if _session is None:
            _session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20,
                pool_maxsize=50,
                max_retries=requests.adapters.Retry(
                    total=2,
                    backoff_factor=0.3,
                    status_forcelist=[500, 502, 503, 504, 429, 408]
                )
            )
            _session.mount('http://', adapter)
            _session.mount('https://', adapter)
            _session.headers.update({
                'User-Agent': 'Mozilla/5.0 (compatible; RAG-Scraper/1.0)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Connection': 'keep-alive'
            })
    return _session

def cleanup_session():
    """Clean up session"""
    global _session
    with _lock:
        if _session:
            _session.close()
            _session = None

def parse_sitemap(sitemap_source: Union[str, bytes], 
                 html_only: bool = True, 
                 filter_release_notes: bool = True) -> pd.DataFrame:
    """Parse sitemap.xml and return filtered URLs"""
    
    # Handle different input types
    if isinstance(sitemap_source, str):
        if sitemap_source.startswith(('http://', 'https://')):
            response = requests.get(sitemap_source)
            response.raise_for_status()
            xml_content = response.content
        elif sitemap_source.startswith('<?xml'):
            xml_content = sitemap_source.encode('utf-8')
        else:
            with open(sitemap_source, 'rb') as f:
                xml_content = f.read()
    else:
        xml_content = sitemap_source
    
    # Parse XML
    root = ET.fromstring(xml_content)
    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    
    urls_data = []
    
    # Handle sitemap index vs regular sitemap
    if root.tag.endswith('sitemapindex'):
        sitemaps = root.findall('.//ns:sitemap', namespace)
        for sitemap in sitemaps:
            loc = sitemap.find('ns:loc', namespace)
            if loc is not None:
                urls_data.append({'url': loc.text, 'type': 'sitemap'})
    else:
        urls = root.findall('.//ns:url', namespace)
        for url in urls:
            loc = url.find('ns:loc', namespace)
            if loc is not None:
                urls_data.append({'url': loc.text, 'type': 'url'})
    
    df = pd.DataFrame(urls_data)
    if df.empty:
        return df
    
    # Apply filters
    original_count = len(df)
    
    if html_only:
        non_html_extensions = [
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.7z', '.tar', '.gz',
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
            '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm',
            '.mp3', '.wav', '.ogg', '.m4a', '.xml', '.json', '.csv', '.txt'
        ]
        html_mask = ~df['url'].str.lower().str.endswith(tuple(non_html_extensions))
        df = df[html_mask]
        print(f"📋 HTML-only filter: {original_count - len(df)} URLs removed")
    
    if filter_release_notes:
        release_patterns = [
            'release-notes', 'release_notes', 'releasenotes',
            'changelog', 'change-log', 'whatsnew', 'whats-new'
        ]
        release_mask = ~df['url'].str.lower().str.contains('|'.join(release_patterns), na=False)
        filtered_count = len(df) - len(df[release_mask])
        df = df[release_mask]
        print(f"📋 Release notes filter: {filtered_count} URLs removed")
    
    print(f"📋 Final URLs: {len(df)} from original {original_count}")
    return df

def check_content_size(url: str, max_size: int = 5_000_000, timeout: int = 8) -> Tuple[bool, Optional[int]]:
    """Fast content size check"""
    try:
        session = get_session()
        response = session.head(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        content_length = response.headers.get('content-length')
        if content_length:
            size_bytes = int(content_length)
            return size_bytes <= max_size, size_bytes
        return True, None
    except:
        return True, None

def scrape_url_safe(url: str, timeout: int = 15) -> Optional[requests.Response]:
    """Safe URL scraping with retry"""
    session = get_session()
    
    for attempt in range(2):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            
            # Hard size limit check
            if 'content-length' in response.headers:
                if int(response.headers['content-length']) > 25_000_000:  # 25MB
                    response.close()
                    return None
            
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == 0:
                time.sleep(0.5)
                continue
        except:
            break
    return None

def extract_sections_by_headers(soup: BeautifulSoup) -> List[Dict]:
    """Extract content sections based on header hierarchy"""
    sections = []
    headers = soup.find_all(['h1', 'h2', 'h3'])
    
    if not headers:
        text = soup.get_text()
        text = re.sub(r'\s+', ' ', text).strip()
        if text:
            sections.append({
                'header_level': 0,
                'header_text': 'Main Content',
                'content': text,
                'header_hierarchy': []
            })
        return sections
    
    header_stack = []
    
    for header in headers:
        header_level = int(header.name[1])
        header_text = header.get_text().strip()
        
        # Update hierarchy
        header_stack = [h for h in header_stack if h['level'] < header_level]
        header_stack.append({'level': header_level, 'text': header_text})
        
        # Get content between headers
        current_element = header.next_sibling
        section_content = []
        
        while current_element:
            if current_element.name in ['h1', 'h2', 'h3']:
                break
            
            if hasattr(current_element, 'get_text'):
                text = current_element.get_text().strip()
                if text and len(text) > 10:
                    section_content.append(text)
            elif isinstance(current_element, str):
                text = current_element.strip()
                if text and len(text) > 10:
                    section_content.append(text)
            
            current_element = current_element.next_sibling
        
        content = ' '.join(section_content).strip()
        content = re.sub(r'\s+', ' ', content)
        
        if content:
            sections.append({
                'header_level': header_level,
                'header_text': header_text,
                'content': content,
                'header_hierarchy': [h['text'] for h in header_stack]
            })
    
    return sections

def chunk_text_with_overlap(text: str, chunk_size: int = 800, overlap: int = 80) -> List[Dict]:
    """Split text into overlapping chunks"""
    if not text or len(text) <= chunk_size:
        return [{"chunk_id": 0, "chunk_text": text, "start_pos": 0, "end_pos": len(text)}]
    
    chunks = []
    start = 0
    chunk_id = 0
    
    while start < len(text):
        end = start + chunk_size
        
        if end < len(text):
            last_space = text.rfind(' ', start, end)
            if last_space > start:
                end = last_space
        
        chunk_text = text[start:end].strip()
        
        if chunk_text:
            chunks.append({
                "chunk_id": chunk_id,
                "chunk_text": chunk_text,
                "start_pos": start,
                "end_pos": end
            })
            chunk_id += 1
        
        start = end - overlap
        if start <= 0:
            start = end
        if start >= len(text):
            break
    
    return chunks

def process_single_url(url: str, chunk_size: int = 800, overlap: int = 80, 
                      timeout: int = 15, max_content_size: int = 5_000_000,
                      url_counter: Dict = None) -> pd.DataFrame:
    """Process single URL with memory safety"""
    
    # Thread-safe counter
    if url_counter and 'lock' in url_counter:
        with url_counter['lock']:
            url_counter['current'] += 1
            current = url_counter['current']
            total = url_counter['total']
            print(f"🔄 [{current}/{total}] {url}")
    
    empty_cols = ['url', 'section_id', 'section_header', 'header_level', 
                 'header_hierarchy', 'chunk_id', 'section_chunk_id', 
                 'chunk_text', 'start_pos', 'end_pos', 'content_length', 'header_context']
    
    response = None
    soup = None
    
    try:
        # Size check
        is_acceptable, content_size = check_content_size(url, max_content_size)
        
        if content_size and content_size > 1_000_000:  # 1MB+
            print(f"📄 {content_size / (1024 * 1024):.1f}MB")
        
        if not is_acceptable:
            return pd.DataFrame(columns=empty_cols)
        
        # Fetch
        response = scrape_url_safe(url, timeout)
        if response is None:
            return pd.DataFrame(columns=empty_cols)
        
        # Parse efficiently
        try:
            soup = BeautifulSoup(response.content, 'lxml')
        except:
            soup = BeautifulSoup(response.content, 'html.parser')
        
        response.close()
        del response
        response = None
        
        # Clean DOM
        for element in soup(["script", "style", "nav", "header", "footer", "aside", 
                           "iframe", "embed", "object", "noscript"]):
            element.decompose()
        
        # Extract sections
        sections = extract_sections_by_headers(soup)
        soup.decompose()
        del soup
        soup = None
        
        # Process sections
        df_data = []
        global_chunk_id = 0
        
        for section_id, section in enumerate(sections):
            chunks = chunk_text_with_overlap(section['content'], chunk_size, overlap)
            
            for chunk in chunks:
                header_context = ' > '.join(section['header_hierarchy']) if section['header_hierarchy'] else section['header_text']
                
                df_data.append({
                    'url': url,
                    'section_id': section_id,
                    'section_header': section['header_text'],
                    'header_level': section['header_level'],
                    'header_hierarchy': ' > '.join(section['header_hierarchy']) if section['header_hierarchy'] else '',
                    'chunk_id': global_chunk_id,
                    'section_chunk_id': chunk['chunk_id'],
                    'chunk_text': chunk['chunk_text'],
                    'start_pos': chunk['start_pos'],
                    'end_pos': chunk['end_pos'],
                    'content_length': len(chunk['chunk_text']),
                    'header_context': header_context
                })
                global_chunk_id += 1
        
        del sections
        
        if df_data:
            df = pd.DataFrame(df_data)
            del df_data
            print(f"✅ {len(df)} chunks")
            return df
        else:
            return pd.DataFrame(columns=empty_cols)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return pd.DataFrame(columns=empty_cols)
        
    finally:
        # Cleanup
        if response:
            try:
                response.close()
            except:
                pass
        if soup:
            try:
                soup.decompose()
            except:
                pass
        gc.collect()

def save_to_parquet(df: pd.DataFrame, output_path: str, mode: str = "append") -> bool:
    """Save DataFrame to parquet file using Arrow"""
    try:
        if df.empty:
            return True
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to Arrow table for efficiency
        table = pa.Table.from_pandas(df)
        
        if mode == "append" and output_path.exists():
            # Read existing data and append
            existing_table = pq.read_table(output_path)
            combined_table = pa.concat_tables([existing_table, table])
            pq.write_table(combined_table, output_path)
        else:
            # Write new file
            pq.write_table(table, output_path)
        
        print(f"💾 Saved {len(df)} chunks to {output_path}")
        return True
        
    except Exception as e:
        print(f"❌ Save error: {e}")
        return False

def process_sitemap_with_batched_threads(urls_or_sitemap: Union[str, List[str]],
                                       output_path: str,
                                       chunk_size: int = 800,
                                       overlap: int = 80,
                                       max_urls: Optional[int] = None,
                                       timeout: int = 15,
                                       max_content_size: int = 5_000_000,
                                       batch_size: int = 25,
                                       max_workers: int = 8,
                                       delay_between_batches: float = 1.0,
                                       max_memory_gb: float = 50.0,
                                       save_frequency: int = 100,
                                       html_only: bool = True,
                                       filter_release_notes: bool = True) -> pd.DataFrame:
    """
    Process sitemap with memory-safe batched threading, saving to Unity Catalog volumes as parquet.
    
    Args:
        urls_or_sitemap: Either a URL to sitemap.xml or a list of URLs to process
        output_path: Path to save parquet file (e.g., '/Volumes/catalog/schema/volume/rag_chunks.parquet')
        chunk_size: Target chunk size (default: 800)
        overlap: Chunk overlap (default: 80)
        max_urls: Maximum URLs to process (None for all)
        timeout: Request timeout (default: 15s)
        max_content_size: Max content size in bytes (default: 5MB)
        batch_size: URLs per batch (default: 25)
        max_workers: Thread pool size (default: 8)
        delay_between_batches: Delay between batches (default: 1.0s)
        max_memory_gb: Memory limit in GB (default: 50.0)
        save_frequency: Save every N URLs (default: 100)
        html_only: Filter non-HTML files (default: True)
        filter_release_notes: Filter release notes (default: True)
        
    Returns:
        pandas.DataFrame: Final batch of chunks (rest saved to parquet)
    """
    
    start_time = time.time()
    start_memory = get_memory_usage()
    
    print(f"🚀 SITEMAP PROCESSOR - UNIFIED")
    print(f"📊 Memory: {start_memory:.1f}GB | Batch: {batch_size} | Workers: {max_workers}")
    print(f"💾 Output: {output_path}")
    
    try:
        # Get URLs - either from sitemap or directly from input
        if isinstance(urls_or_sitemap, str):
            # Parse sitemap
            print(f"📋 Parsing sitemap...")
            sitemap_df = parse_sitemap(urls_or_sitemap, html_only=html_only, filter_release_notes=filter_release_notes)
            
            if sitemap_df.empty:
                print("❌ No URLs found")
                return pd.DataFrame()
            
            urls = sitemap_df[sitemap_df['type'] == 'url']['url'].tolist()
        else:
            # Use provided URL list
            urls = urls_or_sitemap
            print(f"📋 Using provided list of {len(urls)} URLs")
            
            # Apply filters if needed
            if html_only or filter_release_notes:
                original_count = len(urls)
                
                if html_only:
                    non_html_extensions = [
                        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                        '.zip', '.rar', '.7z', '.tar', '.gz',
                        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
                        '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm',
                        '.mp3', '.wav', '.ogg', '.m4a', '.xml', '.json', '.csv', '.txt'
                    ]
                    urls = [url for url in urls if not url.lower().endswith(tuple(non_html_extensions))]
                    print(f"📋 HTML-only filter: {original_count - len(urls)} URLs removed")
                    original_count = len(urls)
                
                if filter_release_notes:
                    release_patterns = [
                        'release-notes', 'release_notes', 'releasenotes',
                        'changelog', 'change-log', 'whatsnew', 'whats-new'
                    ]
                    pattern = '|'.join(release_patterns)
                    urls = [url for url in urls if not re.search(pattern, url.lower())]
                    print(f"📋 Release notes filter: {original_count - len(urls)} URLs removed")
        
        if max_urls:
            urls = urls[:max_urls]
        
        print(f"🎯 Processing {len(urls)} URLs")
        
        # Initialize
        url_counter = {'current': 0, 'total': len(urls), 'lock': Lock()}
        successful_count = 0
        memory_warnings = 0
        batch_results = []
        
        # Process in batches
        total_batches = (len(urls) + batch_size - 1) // batch_size
        
        for batch_start in range(0, len(urls), batch_size):
            batch_end = min(batch_start + batch_size, len(urls))
            batch_urls = urls[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            
            current_memory = get_memory_usage()
            print(f"\n🔄 Batch {batch_num}/{total_batches} | Memory: {current_memory:.1f}GB")
            
            # Memory check
            if current_memory > max_memory_gb:
                memory_warnings += 1
                print(f"⚠️  Memory limit exceeded: {current_memory:.1f}GB")
                
                # Emergency save
                if batch_results:
                    emergency_df = pd.concat(batch_results, ignore_index=True)
                    if save_to_parquet(emergency_df, output_path, "append"):
                        successful_count += len(emergency_df)
                        batch_results = []
                        del emergency_df
                
                # Cleanup
                print("🧹 Memory cleanup...")
                force_memory_cleanup()
                
                after_memory = get_memory_usage()
                print(f"📉 Memory: {after_memory:.1f}GB")
                
                if after_memory > max_memory_gb * 0.9:
                    print("❌ Memory still high. Stopping.")
                    break
            
            # Process batch with threading
            batch_chunks = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {
                    executor.submit(process_single_url, url, chunk_size, overlap, 
                                  timeout, max_content_size, url_counter): url 
                    for url in batch_urls
                }
                
                for future in as_completed(future_to_url, timeout=timeout*2):
                    try:
                        result_df = future.result(timeout=5)
                        if not result_df.empty:
                            batch_chunks.append(result_df)
                    except Exception as e:
                        print(f"❌ Thread error: {e}")
            
            # Collect batch results
            if batch_chunks:
                batch_df = pd.concat(batch_chunks, ignore_index=True)
                batch_results.append(batch_df)
                print(f"✅ Batch complete: {len(batch_df)} chunks")
                del batch_chunks, batch_df
            
            # Periodic saves
            total_chunks = sum(len(df) for df in batch_results)
            if total_chunks >= save_frequency:
                combined_df = pd.concat(batch_results, ignore_index=True)
                
                if save_to_parquet(combined_df, output_path, "append"):
                    successful_count += len(combined_df)
                    batch_results = []
                    del combined_df
                
                force_memory_cleanup()
            
            # Delay between batches
            if delay_between_batches > 0 and batch_num < total_batches:
                time.sleep(delay_between_batches)
        
        # Final save
        final_result = pd.DataFrame()
        if batch_results:
            final_df = pd.concat(batch_results, ignore_index=True)
            
            if save_to_parquet(final_df, output_path, "append"):
                successful_count += len(final_df)
            
            final_result = final_df.copy()
        
        # Summary
        end_time = time.time()
        end_memory = get_memory_usage()
        duration = (end_time - start_time) / 60
        
        print(f"\n📊 PROCESSING COMPLETE")
        print(f"⏱️  Duration: {duration:.1f} minutes")
        print(f"🎯 URLs: {url_counter['current']}/{len(urls)}")
        print(f"✅ Chunks: {successful_count}")
        print(f"🧠 Memory: {start_memory:.1f}GB → {end_memory:.1f}GB (Δ{end_memory-start_memory:+.1f}GB)")
        print(f"💾 Saved to: {output_path}")
        
        return final_result
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Interrupted by user")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return pd.DataFrame()
    finally:
        cleanup_session()
        force_memory_cleanup()


# COMMAND ----------

urls1 = urls_df["url"][0:999]
urls2 = urls_df["url"][1000:1999]
urls3 = urls_df["url"][2000:]

# COMMAND ----------


# Usage example
result1 = process_sitemap_with_batched_threads(
    urls_or_sitemap=urls1,
    output_path="/Volumes/users/rafi_kurlansik/data/dbdocs_rag_chunks.parquet",
    chunk_size=800,
    overlap=80,
    max_urls=None,  # Process all
    timeout=180,
    max_content_size=5_000_000,  # 5MB
    batch_size=100,
    max_workers=50,
    delay_between_batches=1.0,
    max_memory_gb=40.0,
    save_frequency=100,
    html_only=True,
    filter_release_notes=True
) 

# COMMAND ----------


# Usage example
result2 = process_sitemap_with_batched_threads(
    urls_or_sitemap=urls2[0:600],
    output_path="/Volumes/users/rafi_kurlansik/data/dbdocs_rag_chunks.parquet",
    chunk_size=800,
    overlap=80,
    max_urls=None,  # Process all
    timeout=180,
    max_content_size=5_000_000,  # 5MB
    batch_size=100,
    max_workers=60,
    delay_between_batches=1.0,
    max_memory_gb=40.0,
    save_frequency=100,
    html_only=True,
    filter_release_notes=True
) 

# COMMAND ----------


# Usage example
result25 = process_sitemap_with_batched_threads(
    urls_or_sitemap=urls2[601:],
    output_path="/Volumes/users/rafi_kurlansik/data/dbdocs_rag_chunks.parquet",
    chunk_size=800,
    overlap=80,
    max_urls=None,  # Process all
    timeout=180,
    max_content_size=5_000_000,  # 5MB
    batch_size=100,
    max_workers=60,
    delay_between_batches=1.0,
    max_memory_gb=40.0,
    save_frequency=100,
    html_only=True,
    filter_release_notes=True
) 

# COMMAND ----------

# MAGIC %md ### ChatGPT option

# COMMAND ----------

import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import gc

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

def fetch_and_chunk_pages(urls, output_parquet=None):
    records = []
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Processing: {url}")
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            html = res.text
        except Exception as e:
            print(f"Error fetching {url}: {e}. Skipping this URL.")
            continue

        if "text/html" not in res.headers.get("Content-Type", ""):
            print(f"Skipping non-HTML content: {url}")
            continue
        if len(html) > 1_000_000:
            print(f"Skipping large page: {url} ({len(html)} chars)")
            continue

        clean_text, page_title, headers_info = extract_text_and_headings(html)
        headers_summary = "; ".join(
            f"{level.upper()}: {text} ({chars} chars)" for level, text, chars in headers_info
        )
        chunks = chunk_text(clean_text, chunk_size=800, overlap=80)
        for idx, chunk in enumerate(chunks):
            records.append({
                "url": url,
                "title": page_title,
                "chunk_index": idx,
                "text": chunk,
                "headers_char_count": headers_summary,
            })

        del html, clean_text, page_title, headers_info, headers_summary, chunks, res
        gc.collect()

    df = pd.DataFrame(records)
    if output_parquet:
        df.to_parquet(output_parquet, index=False)
        print(f"Saved {len(df)} chunks to {output_parquet}")
    return df


sitemap_url = "https://docs.databricks.com/en/doc-sitemap.xml"  # replace with actual sitemap URL
all_urls = get_all_urls_from_sitemap(sitemap_url)
print(f"Found {len(all_urls)} pages in sitemap.")
df_chunks = fetch_and_chunk_pages(all_urls, output_parquet="website_chunks.parquet")
print(df_chunks.head())

# COMMAND ----------

display(df_chunks)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE users.rafi_kurlansik.dbdocs_rag_chunks

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

chunks_sdf = spark.createDataFrame(df_chunks)
chunks_sdf = chunks_sdf.withColumn("id", monotonically_increasing_id())

chunks_sdf.write.mode("overwrite").saveAsTable("users.rafi_kurlansik.dbdocs_rag_chunks")

spark.sql(
    f"ALTER TABLE users.rafi_kurlansik.dbdocs_rag_chunks SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)