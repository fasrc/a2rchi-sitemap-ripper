#!/usr/bin/env python3
"""
download_site.py

Downloads raw HTML pages listed in a sitemap XML, with optional Readability cleanup.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional

import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm

# Attempt to import Readability
try:
    from readability import Document
    HAVE_READABILITY = True
except ImportError:
    HAVE_READABILITY = False

LAST_RUN_FILE = '.last_run.json'
MAPPING_FILE = 'url_mapping.csv'

def setup_logging() -> None:
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Download HTML files from a sitemap XML, with optional Readability cleanup.'
    )
    parser.add_argument('sitemap_url', help='URL of the sitemap.xml file')
    parser.add_argument('--output-dir', default='tmp', help='Directory to save HTML files (default: tmp)')
    parser.add_argument('--limit', type=int, help='Maximum number of pages to download')
    parser.add_argument('--force', action='store_true', help='Ignore last-modified times and download all pages')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent download threads (default: 5)')
    parser.add_argument('--retries', type=int, default=3, help='Retries per URL on failure (default: 3)')
    parser.add_argument('--readability', action='store_true',
                        help='Apply Readability cleanup to extract main content (requires readability-lxml)')
    return parser.parse_args()

def fetch_url(url: str) -> bytes:
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def parse_sitemap(xml_data: bytes) -> List[Tuple[str, Optional[str]]]:
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    root = ET.fromstring(xml_data)
    entries: List[Tuple[str, Optional[str]]] = []

    for elem in root.findall('.//ns:url', namespaces=ns):
        loc = elem.find('ns:loc', namespaces=ns)
        lastmod = elem.find('ns:lastmod', namespaces=ns)
        if loc is not None and loc.text:
            entries.append((loc.text.strip(), lastmod.text.strip() if lastmod is not None else None))
    return entries

def load_last_run(output_dir: str) -> Optional[float]:
    path = os.path.join(output_dir, LAST_RUN_FILE)
    if os.path.exists(path):
        with open(path, 'r') as f:
            data = json.load(f)
        return data.get('last_run_time')
    return None

def save_last_run(output_dir: str) -> None:
    path = os.path.join(output_dir, LAST_RUN_FILE)
    with open(path, 'w') as f:
        json.dump({'last_run_time': time.time()}, f)

def write_mapping(output_dir: str, url_map: Dict[str, str]) -> None:
    path = os.path.join(output_dir, MAPPING_FILE)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['URL', 'Filename'])
        for url, fname in url_map.items():
            writer.writerow([url, fname])

def url_to_filename(url: str) -> str:
    return hashlib.md5(url.encode('utf-8')).hexdigest() + '.html'

def process_entry(
    url: str,
    lastmod: Optional[str],
    args: argparse.Namespace,
    url_map: Dict[str, str]
) -> Tuple[str, str]:
    if not args.force and args.last_run_time and lastmod:
        try:
            ts = time.mktime(time.strptime(lastmod, '%Y-%m-%dT%H:%M:%S%z'))
            if ts <= args.last_run_time:
                return url, 'skipped'
        except Exception:
            pass

    attempts = args.retries
    while attempts > 0:
        try:
            content = fetch_url(url)
            # Apply Readability cleanup if requested
            if args.readability:
                if not HAVE_READABILITY:
                    logging.error("Readability library not installed. Please install readability-lxml to use --readability.")
                    return url, 'error'
                try:
                    html_str = content.decode('utf-8', errors='ignore')
                    doc = Document(html_str)
                    cleaned = doc.summary()
                    content = cleaned.encode('utf-8')
                except Exception as e:
                    logging.warning("Readability cleaning failed for %s: %s", url, e)

            fname = url_to_filename(url)
            dest = os.path.join(args.output_dir, fname)
            with open(dest, 'wb') as f:
                f.write(content)
            url_map[url] = fname
            return url, 'saved'
        except Exception as e:
            attempts -= 1
            logging.warning("Error fetching %s: %s (remaining retries: %d)", url, e, attempts)
    return url, 'error'

def main() -> None:
    setup_logging()
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    args.last_run_time = load_last_run(args.output_dir)
    xml_data = fetch_url(args.sitemap_url)
    entries = parse_sitemap(xml_data)
    if args.limit:
        entries = entries[:args.limit]

    url_map: Dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_entry, url, lm, args, url_map): url
            for url, lm in entries
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc='Downloading'):
            url, status = future.result()
            logging.info("%s: %s", status.upper(), url)

    save_last_run(args.output_dir)
    write_mapping(args.output_dir, url_map)

if __name__ == '__main__':
    main()
