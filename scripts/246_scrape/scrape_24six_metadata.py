#!/usr/bin/env python3
"""
Script to download metadata from all music collections on 24six.app
Optimized for high-speed concurrent scraping
"""

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
import argparse
import sys
from typing import List, Dict
import time
import os
import re
from pathlib import Path
from urllib.parse import urlparse

# Force unbuffered output for real-time progress display
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ['PYTHONUNBUFFERED'] = '1'

def sanitize_filename(filename: str) -> str:
    """Sanitize a string to be safe for use as a filename."""
    # Replace invalid filename characters with underscores
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')
    # Limit length to 200 characters to avoid filesystem issues
    if len(filename) > 200:
        filename = filename[:200]
    # Replace multiple underscores/spaces with single ones
    filename = re.sub(r'[_\s]+', ' ', filename)
    return filename or 'unknown'

async def download_album_art(session: aiohttp.ClientSession, image_url: str,
                             artist: str, title: str, collection_id: int,
                             output_dir: str, semaphore: asyncio.Semaphore,
                             max_retries: int = 3, timeout: int = 30) -> Dict:
    """Download album art image and save with proper naming in artist folders."""
    if not image_url:
        return {'status': 'no_url', 'collection_id': collection_id}

    async with semaphore:
        # Create filename from artist and title
        artist_name = sanitize_filename(artist) if artist else 'Unknown Artist'
        title_name = sanitize_filename(title) if title else 'Unknown Title'

        # Create artist directory
        artist_dir = Path(output_dir) / artist_name
        artist_dir.mkdir(parents=True, exist_ok=True)

        # Format: "Artist - Title"
        base_filename = f"{artist_name} - {title_name}"

        # Get file extension from URL
        parsed_url = urlparse(image_url)
        path = parsed_url.path
        ext = Path(path).suffix.lower()

        # Default to .jpg if no extension found or if extension is weird
        if not ext or ext not in ['.jpg', '.jpeg', '.png', '.webp', '.gif']:
            ext = '.jpg'

        filename = f"{base_filename}{ext}"
        filepath = artist_dir / filename

        last_error = None

        for attempt in range(max_retries):
            try:
                # Download the image
                async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    if response.status == 200:
                        content = await response.read()

                        # Save to file
                        with open(filepath, 'wb') as f:
                            f.write(content)

                        return {
                            'status': 'success',
                            'collection_id': collection_id,
                            'filename': f"{artist_name}/{filename}",
                            'size': len(content)
                        }
                    else:
                        last_error = f'error_http_{response.status}'
                        # Don't retry client errors (4xx)
                        if 400 <= response.status < 500:
                            return {'status': last_error, 'collection_id': collection_id}

            except asyncio.TimeoutError:
                last_error = 'error_timeout'
            except aiohttp.ClientError as e:
                last_error = f'error_client_{str(e)[:30]}'
            except Exception as e:
                last_error = f'error_{str(e)[:30]}'

            # Wait before retry with exponential backoff
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s, etc.
                await asyncio.sleep(wait_time)

        # All retries exhausted
        return {'status': f'{last_error}_after_{max_retries}_retries', 'collection_id': collection_id}

def extract_metadata(html_content):
    """Extract metadata from a collection page."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find JSON-LD structured data
    json_ld = soup.find('script', {'type': 'application/ld+json'})

    metadata = {
        'collection_id': None,
        'title': None,
        'artist': None,
        'publication_date': None,
        'duration': None,
        'image_url': None,
        'tracks': [],
        'raw_json_ld': None
    }

    if json_ld:
        try:
            # Try to parse the JSON-LD data
            json_string = json_ld.string
            if json_string:
                json_string = json_string.strip()
                if json_string:
                    data = json.loads(json_string)
                    metadata['raw_json_ld'] = data

                    # Extract album-level metadata
                    if '@type' in data:
                        if data['@type'] == 'MusicAlbum' or data['@type'] == 'MusicRecording':
                            metadata['title'] = data.get('name')
                            metadata['image_url'] = data.get('image')
                            metadata['publication_date'] = data.get('datePublished')
                            metadata['duration'] = data.get('duration')

                            # Extract artist
                            if 'byArtist' in data:
                                if isinstance(data['byArtist'], dict):
                                    metadata['artist'] = data['byArtist'].get('name')
                                elif isinstance(data['byArtist'], list) and len(data['byArtist']) > 0:
                                    metadata['artist'] = data['byArtist'][0].get('name')

                            # Extract tracks if available
                            if 'track' in data:
                                tracks = data['track'] if isinstance(data['track'], list) else [data['track']]
                                for track in tracks:
                                    metadata['tracks'].append({
                                        'name': track.get('name'),
                                        'duration': track.get('duration')
                                    })
        except (json.JSONDecodeError, AttributeError, TypeError) as e:
            # JSON parsing failed, will fallback to HTML extraction
            metadata['json_parse_error'] = str(e)[:50]

    # Fallback: try to extract from HTML if JSON-LD parsing failed
    if not metadata['title']:
        title_tag = soup.find('h1')
        if title_tag:
            metadata['title'] = title_tag.get_text(strip=True)

    # Extract artist from HTML if not found in JSON-LD
    if not metadata['artist']:
        # Look for artist link (contains /music/artist/ in href)
        artist_link = soup.find('a', href=lambda x: x and '/music/artist/' in x)
        if artist_link:
            # Extract only English text (first span with lang="en")
            en_span = artist_link.find('span', lang='en')
            if en_span:
                metadata['artist'] = en_span.get_text(strip=True)
            else:
                # Fallback to full text if no lang tag
                metadata['artist'] = artist_link.get_text(strip=True)

    return metadata

async def fetch_collection(collection_id: int, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore,
                          max_retries: int = 3, timeout: int = 20) -> Dict:
    """Fetch a single collection's metadata asynchronously with retry logic."""
    url = f"https://24six.app/app/music/collection/{collection_id}"

    async with semaphore:  # Limit concurrent requests
        last_error = None

        for attempt in range(max_retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    if response.status == 200:
                        html_content = await response.text()
                        metadata = extract_metadata(html_content)
                        metadata['collection_id'] = collection_id
                        metadata['url'] = url
                        metadata['status'] = 'success'
                        if attempt > 0:
                            metadata['retries'] = attempt
                        return metadata
                    elif response.status == 404:
                        # Don't retry 404s
                        return {'collection_id': collection_id, 'url': url, 'status': '404'}
                    else:
                        last_error = f'error_http_{response.status}'
                        # Don't retry client errors (4xx), only server errors (5xx)
                        if 400 <= response.status < 500:
                            return {'collection_id': collection_id, 'url': url, 'status': last_error}

            except asyncio.TimeoutError:
                last_error = 'error_timeout'
            except aiohttp.ClientError as e:
                last_error = f'error_client_{str(e)[:30]}'
            except Exception as e:
                last_error = f'error_{str(e)[:30]}'

            # Wait before retry with exponential backoff
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s, etc.
                await asyncio.sleep(wait_time)

        # All retries exhausted
        return {'collection_id': collection_id, 'url': url, 'status': f'{last_error}_after_{max_retries}_retries'}

async def process_and_save(metadata, all_metadata, output_file, csv_file, counters, start_time, total_items, lock, download_counters=None, json_only=False, last_save_time=None):
    """Process a single result and save periodically."""
    async with lock:
        all_metadata.append(metadata)

        status = metadata['status']
        if status == 'success':
            counters['success'] += 1
            print(f"✓ [{counters['success']}] {metadata['collection_id']}: {metadata.get('title', 'Unknown')}", flush=True)
        elif status == '404':
            counters['not_found'] += 1
            print(f"✗ [{counters['not_found']}] {metadata['collection_id']}: Not found", flush=True)
        else:
            counters['error'] += 1
            print(f"⚠ [{counters['error']}] {metadata['collection_id']}: {status}", flush=True)

        # Save every 15 seconds
        current_time = time.time()
        if last_save_time.get('time', 0) == 0 or (current_time - last_save_time['time']) >= 15:
            save_results(all_metadata, output_file, csv_file if not json_only else None)
            last_save_time['time'] = current_time
            print(f"[SAVED] Progress saved at {len(all_metadata)} items", flush=True)

        # Print progress summary every 10 items
        if len(all_metadata) % 10 == 0:
            elapsed = time.time() - start_time
            processed = len(all_metadata)
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = total_items - processed
            eta = remaining / rate if rate > 0 else 0

            progress_msg = f"[Progress] {processed}/{total_items} | Rate: {rate:.1f}/s | ETA: {eta:.0f}s | " \
                          f"✓{counters['success']} ✗{counters['not_found']} ⚠{counters['error']}"

            if download_counters:
                progress_msg += f" | Art: ✓{download_counters['success']} ⚠{download_counters['error']}"

            print(progress_msg, flush=True)

async def scrape_collections(start_id: int, end_id: int, output_file: str = 'metadata.json',
                            csv_file: str = 'metadata.csv', concurrency: int = 40,
                            batch_size: int = 500, download_art: bool = False,
                            art_dir: str = 'album_art', max_retries: int = 3,
                            collection_timeout: int = 20, art_timeout: int = 30,
                            json_only: bool = False) -> None:
    """Scrape metadata from a range of collection IDs using async/await with retry logic."""

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    all_metadata = []
    counters = {'success': 0, 'not_found': 0, 'error': 0}
    download_counters = {'success': 0, 'error': 0} if download_art else None
    last_save_time = {'time': 0}
    lock = asyncio.Lock()

    total_items = end_id - start_id + 1

    # Create album art directory if needed
    if download_art:
        Path(art_dir).mkdir(exist_ok=True)
        print(f"Album art will be saved to: {art_dir}/", flush=True)

    print(f"Starting high-speed scrape from collection {start_id} to {end_id}", flush=True)
    print(f"Concurrent requests: {concurrency}", flush=True)
    print(f"Total collections: {total_items}", flush=True)
    print(f"Max retries: {max_retries} with exponential backoff", flush=True)
    print(f"Timeouts: {collection_timeout}s (collections), {art_timeout}s (album art)", flush=True)
    print(f"Saving progress every 15 seconds", flush=True)
    print("-" * 60, flush=True)

    start_time = time.time()

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(concurrency)

    # Process in batches to avoid creating too many tasks at once
    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:

        for batch_start in range(start_id, end_id + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, end_id)

            # Create tasks for this batch
            async def fetch_and_process(collection_id):
                metadata = await fetch_collection(collection_id, session, semaphore, max_retries, collection_timeout)
                if not isinstance(metadata, Exception):
                    await process_and_save(metadata, all_metadata, output_file, csv_file,
                                          counters, start_time, total_items, lock, download_counters, json_only, last_save_time)

                    # Download album art if enabled and metadata was successful
                    if download_art and metadata.get('status') == 'success' and metadata.get('image_url'):
                        art_result = await download_album_art(
                            session, metadata['image_url'],
                            metadata.get('artist'), metadata.get('title'),
                            metadata['collection_id'], art_dir, semaphore,
                            max_retries, art_timeout
                        )

                        async with lock:
                            if art_result['status'] == 'success':
                                download_counters['success'] += 1
                                metadata['album_art_file'] = art_result['filename']
                            else:
                                download_counters['error'] += 1
                else:
                    async with lock:
                        counters['error'] += 1

            tasks = [
                fetch_and_process(collection_id)
                for collection_id in range(batch_start, batch_end + 1)
            ]

            # Execute batch concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

    # Final save to ensure everything is saved
    save_results(all_metadata, output_file, csv_file if not json_only else None)

    # Final summary
    elapsed = time.time() - start_time

    print("\n" + "=" * 60, flush=True)
    print(f"Scraping complete in {elapsed:.1f} seconds!", flush=True)
    print(f"Average rate: {len(all_metadata)/elapsed:.1f} requests/second", flush=True)
    print(f"Successful: {counters['success']}", flush=True)
    print(f"Not found: {counters['not_found']}", flush=True)
    print(f"Errors: {counters['error']}", flush=True)
    print(f"Total: {len(all_metadata)}", flush=True)

    if download_art and download_counters:
        print(f"\nAlbum art downloaded: {download_counters['success']}", flush=True)
        print(f"Album art errors: {download_counters['error']}", flush=True)
        print(f"Album art saved to: {art_dir}/", flush=True)

    print(f"\nResults saved to:", flush=True)
    print(f"  - JSON: {output_file}", flush=True)
    if not json_only:
        print(f"  - CSV: {csv_file}", flush=True)

def save_results(metadata_list, json_file, csv_file):
    """Save results to JSON and CSV files atomically to prevent corruption."""
    # Save JSON (only successful items)
    successful_items = [m for m in metadata_list if m.get('status') == 'success']

    # Sort by collection_id to maintain numerical order (1, 2, 3, 4, 5, etc.)
    successful_items.sort(key=lambda x: x.get('collection_id', 0))

    # Write to temporary file first, then atomically rename
    json_temp = json_file + '.tmp'
    with open(json_temp, 'w', encoding='utf-8') as f:
        json.dump(successful_items, f, indent=2, ensure_ascii=False)

    # Atomic rename - if interrupted, old file remains intact
    os.replace(json_temp, json_file)

    # Save CSV (only successful items) if csv_file is provided
    if csv_file and successful_items:
        csv_temp = csv_file + '.tmp'
        with open(csv_temp, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['collection_id', 'url', 'title', 'artist', 'publication_date', 'duration', 'image_url', 'album_art_file']
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(successful_items)

        # Atomic rename
        os.replace(csv_temp, csv_file)

def main():
    parser = argparse.ArgumentParser(
        description='High-speed concurrent metadata scraper for 24six.app music collections with retry logic',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape collections 1-1000 with default settings (40 concurrent requests, 3 retries)
  python scrape_24six_metadata.py --start 1 --end 1000

  # Download album art with proper naming (Artist - Title.jpg)
  python scrape_24six_metadata.py --start 1 --end 1000 --download-art

  # Save only JSON output (skip CSV)
  python scrape_24six_metadata.py --start 1 --end 1000 --json-only

  # Higher speed with 80 concurrent requests
  python scrape_24six_metadata.py --start 1 --end 20000 --concurrency 80

  # More aggressive retries with longer timeouts for unreliable connections
  python scrape_24six_metadata.py --start 1 --end 5000 --max-retries 5 --collection-timeout 30 --art-timeout 60

  # Conservative mode with 20 concurrent requests
  python scrape_24six_metadata.py --start 1 --end 5000 --concurrency 20

  # Custom output files and batch size
  python scrape_24six_metadata.py --start 1 --end 500 --output my_data.json --csv my_data.csv --batch-size 1000

Performance notes:
  - Default concurrency (40) provides ~40-80 requests/second with retries
  - Higher concurrency (80-150) can reach 80-150+ requests/second
  - Failed requests automatically retry with exponential backoff (default: 3 retries)
  - Timeouts: 20s for collections, 30s for album art (configurable)
  - Batch size controls how often progress is saved (default: 500)
  - Album art is named using extracted metadata: "Artist - Title.jpg"
        """
    )

    parser.add_argument('--start', type=int, default=1, help='Starting collection ID (default: 1)')
    parser.add_argument('--end', type=int, default=20000, help='Ending collection ID (default: 20000)')
    parser.add_argument('--output', type=str, default='metadata.json', help='Output JSON file (default: metadata.json)')
    parser.add_argument('--csv', type=str, default='metadata.csv', help='Output CSV file (default: metadata.csv)')
    parser.add_argument('--concurrency', type=int, default=40, help='Number of concurrent requests (default: 40, max recommended: 200)')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing and saving (default: 500)')
    parser.add_argument('--download-art', action='store_true', help='Download album art images with proper naming (Artist - Title.jpg)')
    parser.add_argument('--art-dir', type=str, default='album_art', help='Directory to save album art (default: album_art)')
    parser.add_argument('--max-retries', type=int, default=3, help='Maximum number of retries for failed requests (default: 3)')
    parser.add_argument('--collection-timeout', type=int, default=20, help='Timeout in seconds for collection fetches (default: 20)')
    parser.add_argument('--art-timeout', type=int, default=30, help='Timeout in seconds for album art downloads (default: 30)')
    parser.add_argument('--json-only', action='store_true', help='Save only JSON output (skip CSV file)')

    args = parser.parse_args()

    if args.start > args.end:
        print("Error: start ID must be less than or equal to end ID", file=sys.stderr)
        sys.exit(1)

    if args.concurrency < 1:
        print("Error: concurrency must be at least 1", file=sys.stderr)
        sys.exit(1)

    if args.batch_size < 1:
        print("Error: batch size must be at least 1", file=sys.stderr)
        sys.exit(1)

    try:
        asyncio.run(scrape_collections(args.start, args.end, args.output, args.csv,
                                       args.concurrency, args.batch_size,
                                       args.download_art, args.art_dir,
                                       args.max_retries, args.collection_timeout,
                                       args.art_timeout, args.json_only))
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user. Partial results have been saved.")
        sys.exit(0)

if __name__ == '__main__':
    main()
