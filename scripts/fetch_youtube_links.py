#!/usr/bin/env python3
"""
YouTube Music Link Fetcher (Final Full ETA + Missing Log)
=========================================================

- Uses ytmusicapi (no scraping, no API key)
- Multi-threaded for speed (8 threads)
- ETA for full job shown on every line
- Appends to youtube-links-optimized.json (never overwrites)
- Writes not found songs to not_found.txt
- Retries transient JSON/parse failures automatically
- Cleans nulls and saves progress every 500
"""

import json
import os
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from ytmusicapi import YTMusic

# -----------------------
# Configuration
# -----------------------
OUTPUT_FILE = "youtube-links-optimized.json"
NOT_FOUND_LOG = "not_found.txt"
MAX_THREADS = 8
MAX_TRACKS_PER_RUN = 70000
BATCH_SAVE = 500
DELAY_BASE = 0.05
DELAY_JITTER = (0.02, 0.08)

sys.stdout.reconfigure(line_buffering=True)
ytm = YTMusic()

# -----------------------
# Helpers
# -----------------------
def load_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_json(data, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

def log_not_found(artist, track):
    with open(NOT_FOUND_LOG, "a", encoding="utf-8") as f:
        f.write(f"{artist} - {track}\n")

def clean_text(text):
    import re, unicodedata
    text = text or ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s\u0590-\u05FF]", " ", text)
    return " ".join(text.lower().split())

def validate_match(artist, track, title, artists):
    at = clean_text(artist)
    tr = clean_text(track)
    tt = clean_text(title)
    artist_hits = sum(1 for w in at.split() if w in tt)
    track_hits = sum(1 for w in tr.split() if w in tt)
    if artist_hits >= 1 and track_hits >= 1:
        return True
    if any(clean_text(a["name"]) in tt for a in (artists or [])):
        return True
    if tr in tt:
        return True
    return False

def format_eta(done, total, start):
    elapsed = time.time() - start
    rate = done / elapsed if elapsed > 0 else 0
    remaining = (total - done) / rate if rate > 0 else 0
    m, s = divmod(int(remaining), 60)
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"

# -----------------------
# YouTube Music Search
# -----------------------
def search_youtube_music(artist, track, max_retries=3):
    query = f"{artist} {track}"
    for attempt in range(max_retries):
        try:
            results = ytm.search(query, filter="songs")
            if not results:
                results = ytm.search(query, filter="videos")
            for item in results or []:
                title = item.get("title", "")
                artists = item.get("artists", [])
                video_id = item.get("videoId")
                if video_id and validate_match(artist, track, title, artists):
                    return video_id
            return None
        except Exception as e:
            msg = str(e)
            if "Expecting value" in msg or "JSON" in msg:
                print(f"⚠️ Retrying ({attempt+1}/{max_retries}) {artist} - {track} (empty response)")
                time.sleep(0.3 + random.uniform(0, 0.5))
                continue
            else:
                print(f"⚠️ Error searching {artist} - {track}: {e}")
                return None
    return None

# -----------------------
# Main
# -----------------------
def main():
    print("Loading metadata.json...")
    try:
        with open("metadata.json", "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print("❌ metadata.json not found!", file=sys.stderr)
        sys.exit(1)

    youtube_links = load_json(OUTPUT_FILE)
    total_tracks = sum(len(a.get("tracks", [])) for a in metadata)
    existing = len(youtube_links)
    remaining = total_tracks - existing

    run_limit = min(remaining, MAX_TRACKS_PER_RUN)
    print(f"Total tracks: {total_tracks}")
    print(f"Existing entries: {existing}")
    print(f"Remaining NEW tracks: {remaining}")
    print(f"Run cap: {run_limit}\n")

    start_time = time.time()
    processed = 0
    found = 0
    queue = []

    # Clear not_found log for this run
    open(NOT_FOUND_LOG, "w").close()

    # Build queue
    for album in metadata:
        artist = album.get("artist", "")
        for track in album.get("tracks", []) or []:
            name = track.get("name", "")
            if not artist or not name:
                continue
            key = f"{artist}|{name}"
            if key in youtube_links:
                continue
            queue.append((artist, name))
            if len(queue) >= run_limit:
                break
        if len(queue) >= run_limit:
            break

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(search_youtube_music, a, t): (a, t) for a, t in queue}

        for i, future in enumerate(as_completed(futures), 1):
            artist, track = futures[future]
            vid = future.result()
            key = f"{artist}|{track}"

            if vid:
                youtube_links[key] = {
                    "artist": artist,
                    "track": track,
                    "url": f"https://music.youtube.com/watch?v={vid}"
                }
                found += 1
                status = "✓ Found"
            else:
                youtube_links[key] = None
                log_not_found(artist, track)
                status = "✗ Not found"

            processed += 1
            percent = (processed + existing) / total_tracks * 100
            eta = format_eta(processed + existing, total_tracks, start_time)
            print(f"[{processed + existing}/{total_tracks} | {percent:4.1f}% | ETA {eta}] {status}: {artist} - {track}")
            sys.stdout.flush()

            if processed % BATCH_SAVE == 0:
                save_json(youtube_links, OUTPUT_FILE)
                print(f"💾 Saved progress ({processed}/{run_limit})...")

            time.sleep(DELAY_BASE + random.uniform(*DELAY_JITTER))

    # Cleanup nulls
    before = len(youtube_links)
    youtube_links = {k: v for k, v in youtube_links.items() if v is not None}
    removed = before - len(youtube_links)
    if removed:
        print(f"\n🧹 Removed {removed} null entries.")

    save_json(youtube_links, OUTPUT_FILE)

    elapsed = time.time() - start_time
    m, s = divmod(int(elapsed), 60)
    print(f"\n✓ Done — processed {processed}, found {found}, cleaned {removed}.")
    print(f"Elapsed: {m}m {s}s. Saved to {OUTPUT_FILE}. Missing written to {NOT_FOUND_LOG}")

if __name__ == "__main__":
    main()

