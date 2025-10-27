#!/usr/bin/env python3
"""
Fetch YouTube video IDs for songs in metadata.json
Uses yt-dlp to search YouTube and find the best matching video
Processes in batches with incremental commits for large datasets

Behavior:
- Pass 1: add entries for any tracks not yet present in youtube-links.json
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

# -----------------------
# Configuration
# -----------------------
BATCH_SIZE = 1000                # Commit every 1000 updates
DELAY_BETWEEN_SEARCHES = 0.5     # Base delay (seconds) between searches
MAX_TRACKS_PER_RUN = 10000       # Hard cap on total operations per run (new + retries)

# Retry behavior
SEARCH_MAX_ATTEMPTS = 2          # 1 immediate try + 1 retry on transient errors (403/429/timeouts)
RETRY_BACKOFF_BASE = 7           # seconds; backoff grows linearly per attempt

# Null-retry behavior (after all tracks are "present")
MAX_NULL_RETRIES_PER_RUN = 2000  # extra attempts on previously-null entries per run
NULL_RETRY_RESULTS = 10          # widen search result set during null-retry pass


# -----------------------
# Helpers
# -----------------------
def get_album_title(metadata, artist: str, track_name: str) -> str:
    """Find album title from metadata for (artist, track)."""
    for a in metadata:
        if a.get('artist', '') != artist:
            continue
        tracks = a.get('tracks', []) or []
        for t in tracks:
            if t.get('name', '') == track_name:
                return a.get('title', '')
    return ''


# -----------------------
# Matching / Validation
# -----------------------
def validate_match(artist: str, track_name: str, video_title: str, video_channel: str = "") -> bool:
    """
    Very lenient validator. Prefer false positives over misses.
    """
    video_title_lower = video_title.lower()
    video_channel_lower = (video_channel or "").lower()
    artist_lower = artist.lower()
    track_lower = track_name.lower()

    import re
    def clean(text):
        text = re.sub(r'[^\w\s]', ' ', text)
        return ' '.join(text.split())

    video_title_clean = clean(video_title_lower)
    artist_clean = clean(artist_lower)
    track_clean = clean(track_lower)

    # Strip featured artists variants
    track_clean_no_feat = re.split(r'\b(ft|feat|featuring|with|and)\b', track_clean)[0].strip()

    artist_words = [w for w in artist_clean.split() if len(w) > 1]
    track_words  = [w for w in track_clean_no_feat.split() if len(w) > 1]

    artist_matches = sum(1 for w in artist_words if w in video_title_clean or w in video_channel_lower)
    track_matches  = sum(1 for w in track_words  if w in video_title_clean)

    if artist_matches >= 1 and track_matches >= 1:
        return True
    if track_matches >= 1:
        return True
    if video_channel_lower and any(w in video_channel_lower for w in artist_words):
        return True
    if len(track_clean_no_feat) > 3 and track_clean_no_feat in video_title_clean:
        return True
    if artist_matches >= max(1, int(len(artist_words) * 0.5)):
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
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        },
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(f"ytsearch{results}:{query}", download=False)


def search_youtube(artist: str, track_name: str, attempts: int = SEARCH_MAX_ATTEMPTS, widen_on_retry: bool = False) -> Optional[str]:
    """
    Search YouTube and return a video_id, with retry/backoff on transient errors.
    If widen_on_retry = True, second attempt fetches more results to increase odds.
    """
    query = f"{artist} {track_name}"

    for attempt in range(attempts):
        results_count = 5 if (attempt == 0 or not widen_on_retry) else NULL_RETRY_RESULTS
        try:
            result = _yt_search(query, results=results_count)
            if result and 'entries' in result and result['entries']:
                for video in result['entries']:
                    if not video:
                        continue
                    video_id = video.get('id')
                    video_title = video.get('title', '') or ''
                    video_channel = video.get('uploader', '') or video.get('channel', '') or ''
                    if video_id and validate_match(artist, track_name, video_title, video_channel):
                        return video_id
                return None  # no validated match found

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
    """Save youtube-links.json and commit to git (if git repo present)"""
    print(f"\nSaving youtube-links.json...")
    with open('youtube-links.json', 'w', encoding='utf-8') as f:
        json.dump(youtube_links, f, indent=2, ensure_ascii=False)

    if not os.path.exists('.git'):
        print("No .git directory found — skipping commit/push (local run?).")
        return

    try:
        subprocess.run(['git', 'add', 'youtube-links.json'], check=True)
        commit = subprocess.run(['git', 'commit', '-m', message], check=False, capture_output=True, text=True)
        if commit.returncode == 0:
            subprocess.run(['git', 'push'], check=True)
            print(f"✓ Committed and pushed: {message}")
        else:
            print(f"Note: nothing to commit for: {message}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Git operation failed: {e}", file=sys.stderr)


# -----------------------
# Cleanup: remove nulls
# -----------------------
def cleanup_nulls(youtube_links: dict) -> int:
    """Delete any entries with value None. Returns count removed."""
    null_keys = [k for k, v in youtube_links.items() if v is None]
    for k in null_keys:
        del youtube_links[k]
    return len(null_keys)


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
    if os.path.exists('youtube-links.json'):
        print("Loading existing youtube-links.json...")
        try:
            with open('youtube-links.json', 'r', encoding='utf-8') as f:
                youtube_links = json.load(f)
        except Exception as e:
            print(f"Warning: failed to parse youtube-links.json; starting fresh ({e})", file=sys.stderr)
            youtube_links = {}

    total_tracks = sum(len(album.get('tracks', [])) for album in metadata)
    existing_entries = len(youtube_links)  # includes any pre-existing nulls
    remaining_new = max(0, total_tracks - existing_entries)

    print(f"Total tracks in metadata: {total_tracks}")
    print(f"Entries already present (incl. nulls): {existing_entries}")
    print(f"Remaining NEW tracks to process: {remaining_new}")
    print(f"Run cap (MAX_TRACKS_PER_RUN): {MAX_TRACKS_PER_RUN}")

    processed = 0
    new_links = 0
    batch_count = 0

    # -----------------------
    # Pass 1: process NEW tracks (not yet in youtube_links)
    # -----------------------
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

            # Skip if already present (even if null; we'll handle in pass 2)
            if key in youtube_links:
                continue

            if processed >= MAX_TRACKS_PER_RUN:
                print(f"\nReached limit of {MAX_TRACKS_PER_RUN} operations for this run.")
                save_and_commit(youtube_links, f"Auto-update: {new_links} new YouTube links ({existing_entries + processed} total entries before cleanup)")
                # Final cleanup: remove nulls
                removed = cleanup_nulls(youtube_links)
                if removed:
                    save_and_commit(youtube_links, f"Cleanup: removed {removed} null entries (post-limit)")
                print(f"\n✓ Session complete! Processed {processed} new tracks")
                return

            video_id = search_youtube(artist, track_name, attempts=SEARCH_MAX_ATTEMPTS, widen_on_retry=False)

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
                # Temporarily record None so pass 2 can retry this run
                youtube_links[key] = None
                print(f"✗ Not found: {artist} - {track_name}")

            processed += 1

            # Jittered delay to reduce anti-bot triggers
            time.sleep(DELAY_BETWEEN_SEARCHES + random.uniform(0.2, 1.2))

            if processed % BATCH_SIZE == 0:
                batch_count += 1
                save_and_commit(youtube_links, f"Batch {batch_count}: {new_links} new links ({existing_entries + processed} total entries before cleanup)")
                print(f"\n--- Batch {batch_count} committed ({processed}/{MAX_TRACKS_PER_RUN} processed in this run) ---\n")

    # Commit after pass 1
    if processed > 0:
        save_and_commit(youtube_links, f"Auto-update: {new_links} new YouTube links ({existing_entries + processed} total entries before cleanup)")

    # -----------------------
    # Pass 2: retry NULL entries (only after all tracks are present)
    # -----------------------
    null_keys = [k for k, v in youtube_links.items() if v is None]
    if null_keys:
        if len(youtube_links) >= total_tracks:
            remaining_budget = max(0, MAX_TRACKS_PER_RUN - processed)
            to_retry = min(len(null_keys), remaining_budget, MAX_NULL_RETRIES_PER_RUN)
            if to_retry > 0:
                print(f"\nStarting null-retry pass: attempting {to_retry} of {len(null_keys)} null entries...")
                retried = 0
                fixed = 0
                retry_batch = 0

                for key in null_keys[:to_retry]:
                    artist, track_name = key.split('|', 1)
                    album_title = get_album_title(metadata, artist, track_name)
                    video_id = search_youtube(artist, track_name, attempts=SEARCH_MAX_ATTEMPTS, widen_on_retry=True)
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
                        print(f"✓ Fixed null: {artist} - {track_name} -> {video_id}")
                    else:
                        print(f"✗ Still null: {artist} - {track_name}")

                    time.sleep(DELAY_BETWEEN_SEARCHES + random.uniform(0.2, 1.4))

                    if retried % BATCH_SIZE == 0:
                        retry_batch += 1
                        save_and_commit(youtube_links, f"Null-retry batch {retry_batch}: fixed {fixed} (retried {retried}) before cleanup")
                        print(f"\n--- Null-retry batch {retry_batch} committed (fixed {fixed} / retried {retried}) ---\n")

                save_and_commit(youtube_links, f"Null-retry pass complete: fixed {fixed}, retried {retried} (before cleanup)")
                print(f"\n✓ Null-retry done: fixed {fixed} of {retried} attempted nulls.")
            else:
                print("\nNo remaining run budget for null-retry pass.")
        else:
            print("\nSkipping null-retry pass this run (still adding new tracks).")

    # -----------------------
    # Final cleanup: DELETE any remaining nulls
    # -----------------------
    removed = cleanup_nulls(youtube_links)
    if removed:
        save_and_commit(youtube_links, f"Cleanup: removed {removed} null entries (final)")

    # Summary
    total_non_null = sum(1 for v in youtube_links.values() if v is not None)
    print(f"\n✓ Complete! New links this run: {new_links}")
    print(f"Total entries in youtube-links.json: {len(youtube_links)} (all non-null)")
    print(f"Total non-null links: {total_non_null}")


if __name__ == '__main__':
    main()
