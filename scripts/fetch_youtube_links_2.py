#!/usr/bin/env python3
"""
Fetch YouTube video IDs for songs in metadata.json
Uses yt-dlp to search YouTube and find the best matching video
Processes in batches with incremental commits for large datasets

Behavior:
- Pass 1: add entries for any tracks not yet present in youtube-links-2.json
  - if found → store full object
  - if not found → store None (temporary, for this run only)
- Pass 2 (when all tracks are present): retry previously-null entries
- Final cleanup: delete any entries that remain None (remove nulls from the JSON)
"""

import json
import os
import sys
import time
import random
import subprocess
from typing import Optional
import yt_dlp
import unicodedata
import re

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# -----------------------
# Configuration
# -----------------------
BATCH_SIZE = 3000
DELAY_BETWEEN_SEARCHES = 0.3
MAX_TRACKS_PER_RUN = 6000
SEARCH_MAX_ATTEMPTS = 2
RETRY_BACKOFF_BASE = 7
MAX_NULL_RETRIES_PER_RUN = 2000
NULL_RETRY_RESULTS = 10
OUTPUT_FILE = "youtube-links-2.json"   # <— switched here


# -----------------------
# Helpers
# -----------------------
def get_album_title(metadata, artist: str, track_name: str) -> str:
    """Find album title from metadata for (artist, track)."""
    for a in metadata:
        if a.get('artist', '') != artist:
            continue
        for t in a.get('tracks', []) or []:
            if t.get('name', '') == track_name:
                return a.get('title', '')
    return ''


# -----------------------
# Matching / Validation
# -----------------------
def validate_match(artist: str, track_name: str, video_title: str, video_channel: str = "") -> bool:
    """
    More tolerant validator — ignores apostrophes, accents, and allows partial word overlap.
    """

    def normalize(text):
        text = text.lower()
        text = unicodedata.normalize("NFKD", text)
        text = ''.join(c for c in text if not unicodedata.combining(c))  # remove accents
        text = re.sub(r"[’'`]", "", text)  # strip apostrophes
        text = re.sub(r"[^\w\s]", " ", text)
        return ' '.join(text.split())

    video_title_clean = normalize(video_title)
    video_channel_clean = normalize(video_channel)
    artist_clean = normalize(artist)
    track_clean = normalize(track_name)

    # Drop "feat" etc. for better matching
    track_clean = re.split(r"\b(ft|feat|featuring|with|and)\b", track_clean)[0].strip()

    artist_words = set(w for w in artist_clean.split() if len(w) > 1)
    track_words = set(w for w in track_clean.split() if len(w) > 1)
    video_text = f"{video_title_clean} {video_channel_clean}"

    artist_hits = sum(1 for w in artist_words if w in video_text)
    track_hits = sum(1 for w in track_words if w in video_text)

    if track_hits >= 1 and artist_hits >= 1:
        return True
    if track_hits >= max(1, int(len(track_words) * 0.4)):
        return True
    if artist_hits >= max(1, int(len(artist_words) * 0.4)):
        return True
    if any(w in video_channel_clean for w in artist_words):
        return True
    if track_clean in video_title_clean:
        return True
    return False


# -----------------------
# YouTube Search
# -----------------------
def _yt_search(query: str, results: int = 5):
    """Run a yt-dlp search and return entries (flat)."""
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True,
        'default_search': f'ytsearch{results}',
        'geo_bypass': True,
        'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'},
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(f"ytsearch{results}:{query}", download=False)


def search_youtube(artist: str, track_name: str, attempts: int = SEARCH_MAX_ATTEMPTS, widen_on_retry: bool = False) -> Optional[str]:
    """Search YouTube and return a video_id with retries/backoff."""
    query = f"{artist} {track_name} official audio"

    for attempt in range(attempts):
        results_count = 5 if (attempt == 0 or not widen_on_retry) else NULL_RETRY_RESULTS
        try:
            result = _yt_search(query, results=results_count)
            if result and 'entries' in result and result['entries']:
                for video in result['entries']:
                    if not video:
                        continue
                    vid = video.get('id')
                    title = video.get('title', '') or ''
                    channel = video.get('uploader', '') or video.get('channel', '') or ''
                    if vid and validate_match(artist, track_name, title, channel):
                        return vid
                return None

        except yt_dlp.utils.DownloadError as e:
            msg = str(e)
            transient = ('403' in msg) or ('429' in msg) or ('timed out' in msg.lower())
            if transient and attempt < attempts - 1:
                backoff = RETRY_BACKOFF_BASE * (attempt + 1)
                print(f"  Transient error on '{query}' ({msg}). Retrying in {backoff}s...", file=sys.stderr)
                time.sleep(backoff + random.uniform(0.5, 2.0))
                continue
            print(f"  Error searching for '{query}': {msg}", file=sys.stderr)
            return None
        except Exception as e:
            if attempt < attempts - 1:
                backoff = RETRY_BACKOFF_BASE * (attempt + 1)
                print(f"  Unexpected error on '{query}' ({e}). Retrying in {backoff}s...", file=sys.stderr)
                time.sleep(backoff + random.uniform(0.5, 2.0))
                continue
            print(f"  Error searching for '{query}': {e}", file=sys.stderr)
            return None
    return None


# -----------------------
# Git save/commit
# -----------------------
def save_and_commit(youtube_links, message):
    """Save youtube-links-2.json and commit to git if applicable."""
    print(f"\nSaving {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(youtube_links, f, indent=2, ensure_ascii=False)

    if not os.path.exists('.git'):
        print("No .git directory found — skipping commit/push.")
        return

    try:
        subprocess.run(['git', 'add', OUTPUT_FILE], check=True)
        commit = subprocess.run(['git', 'commit', '-m', message], check=False, capture_output=True, text=True)
        if commit.returncode == 0:
            subprocess.run(['git', 'push'], check=True)
            print(f"✓ Committed and pushed: {message}")
        else:
            print(f"Note: nothing to commit for: {message}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Git operation failed: {e}", file=sys.stderr)


# -----------------------
# Cleanup
# -----------------------
def cleanup_nulls(youtube_links: dict) -> int:
    """Delete entries with None values."""
    nulls = [k for k, v in youtube_links.items() if v is None]
    for k in nulls:
        del youtube_links[k]
    return len(nulls)


# -----------------------
# Main
# -----------------------
def main():
    print("Loading metadata.json...")
    try:
        with open('metadata.json', 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print("Error: metadata.json not found!", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: failed to parse metadata.json: {e}", file=sys.stderr)
        sys.exit(1)

    youtube_links = {}
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing {OUTPUT_FILE}...")
        try:
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                youtube_links = json.load(f)
        except Exception as e:
            print(f"Warning: failed to parse {OUTPUT_FILE}; starting fresh ({e})", file=sys.stderr)
            youtube_links = {}

    total_tracks = sum(len(album.get('tracks', [])) for album in metadata)
    existing_entries = len(youtube_links)
    remaining_new = max(0, total_tracks - existing_entries)

    print(f"Total tracks: {total_tracks}")
    print(f"Existing entries: {existing_entries}")
    print(f"Remaining NEW: {remaining_new}")
    print(f"Run cap: {MAX_TRACKS_PER_RUN}")

    processed = 0
    new_links = 0
    batch_count = 0

    # Pass 1: new tracks
    for album in metadata:
        artist = album.get('artist', '')
        album_title = album.get('title', '')
        tracks = album.get('tracks', []) or []
        if not artist or not tracks:
            continue

        for track in tracks:
            track_name = track.get('name', '')
            if not track_name:
                continue
            key = f"{artist}|{track_name}"

            if key in youtube_links:
                continue

            if processed >= MAX_TRACKS_PER_RUN:
                print(f"\nReached {MAX_TRACKS_PER_RUN} tracks, stopping.")
                save_and_commit(youtube_links, f"Auto-update: {new_links} new links ({existing_entries + processed})")
                removed = cleanup_nulls(youtube_links)
                if removed:
                    save_and_commit(youtube_links, f"Cleanup: removed {removed} nulls")
                print(f"\n✓ Session done: {processed} new tracks.")
                return

            video_id = search_youtube(artist, track_name)

            if video_id:
                youtube_links[key] = {
                    'artist': artist,
                    'track': track_name,
                    'album': album_title,
                    'video_id': video_id,
                    'url': f"https://www.youtube.com/watch?v={video_id}"
                }
                new_links += 1
                print(f"✓ Found: {artist} - {track_name} -> {video_id}")
            else:
                youtube_links[key] = None
                print(f"✗ Not found: {artist} - {track_name}")

            processed += 1
            time.sleep(DELAY_BETWEEN_SEARCHES + random.uniform(0.2, 1.2))

            if processed % BATCH_SIZE == 0:
                batch_count += 1
                save_and_commit(youtube_links, f"Batch {batch_count}: {new_links} new links ({processed})")
                print(f"\n--- Batch {batch_count} committed ---\n")

    if processed > 0:
        save_and_commit(youtube_links, f"Auto-update: {new_links} new links ({processed})")

    # Pass 2: null retry
    null_keys = [k for k, v in youtube_links.items() if v is None]
    if null_keys:
        if len(youtube_links) >= total_tracks:
            remaining_budget = max(0, MAX_TRACKS_PER_RUN - processed)
            to_retry = min(len(null_keys), remaining_budget, MAX_NULL_RETRIES_PER_RUN)
            if to_retry > 0:
                print(f"\nRetrying {to_retry} nulls...")
                retried = 0
                fixed = 0

                for key in null_keys[:to_retry]:
                    artist, track_name = key.split('|', 1)
                    album_title = get_album_title(metadata, artist, track_name)
                    video_id = search_youtube(artist, track_name, widen_on_retry=True)
                    retried += 1
                    processed += 1

                    if video_id:
                        youtube_links[key] = {
                            'artist': artist,
                            'track': track_name,
                            'album': album_title,
                            'video_id': video_id,
                            'url': f"https://www.youtube.com/watch?v={video_id}"
                        }
                        fixed += 1
                        print(f"✓ Fixed: {artist} - {track_name} -> {video_id}")
                    else:
                        print(f"✗ Still null: {artist} - {track_name}")

                    time.sleep(DELAY_BETWEEN_SEARCHES + random.uniform(0.2, 1.4))

                save_and_commit(youtube_links, f"Null-retry: fixed {fixed} of {retried}")
                print(f"\n✓ Null retry complete: fixed {fixed}/{retried}")
            else:
                print("\nNo run budget left for null retry.")
        else:
            print("\nSkipping null retry (still adding tracks).")

    removed = cleanup_nulls(youtube_links)
    if removed:
        save_and_commit(youtube_links, f"Cleanup: removed {removed} nulls (final)")

    total_non_null = sum(1 for v in youtube_links.values() if v is not None)
    print(f"\n✓ Complete! {new_links} new links.")
    print(f"Total entries: {len(youtube_links)} (non-null: {total_non_null})")


if __name__ == '__main__':
    main()
