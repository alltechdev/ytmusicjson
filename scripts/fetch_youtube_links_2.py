#!/usr/bin/env python3
"""
Fast YouTube Link Fetcher (Topic-Aware + Hebrew + Logging)
==========================================================

Fetch YouTube video IDs for songs in metadata.json and write results to youtube-links-2.json.

Features:
- Concurrent yt-dlp searches (3 threads)
- Low randomized delay (0.15s base)
- Hebrew + Unicode support (keeps non-Latin characters)
- Auto-accepts "Topic" and "Official" channels
- Rejects karaoke, covers, remixes unless metadata includes them
- Logs all unfound tracks to missing_tracks.txt
"""

import json
import os
import sys
import time
import random
import subprocess
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import yt_dlp
import unicodedata
import re
import difflib

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# -----------------------
# Configuration
# -----------------------
OUTPUT_FILE = "youtube-links-2.json"
MISSING_LOG_FILE = "missing_tracks.txt"
BATCH_SIZE = 3000
DELAY_BETWEEN_SEARCHES = 0.15
MAX_THREADS = 3
MAX_TRACKS_PER_RUN = 3000
SEARCH_MAX_ATTEMPTS = 1
RETRY_BACKOFF_BASE = 6
MAX_NULL_RETRIES_PER_RUN = 2000
NULL_RETRY_RESULTS = 10

# Known manual overrides (force-accept)
OVERRIDE = {
    ("Simcha Leiner", "Harbei Nachat"): True,
}

# -----------------------
# Helpers
# -----------------------
def log_missing(artist: str, track: str):
    """Append missing track to log file."""
    with open(MISSING_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{artist} - {track}\n")

def get_album_title(metadata, artist: str, track_name: str) -> str:
    for a in metadata:
        if a.get("artist", "") != artist:
            continue
        for t in a.get("tracks", []) or []:
            if t.get("name", "") == track_name:
                return a.get("title", "")
    return ""


# -----------------------
# Matching / Validation
# -----------------------
def validate_match(artist: str, track_name: str, video_title: str, video_channel: str = "") -> bool:
    """Lenient validator with Hebrew/non-Latin support and Topic/Official handling."""

    def normalize(text):
        text = text.lower()
        text = unicodedata.normalize("NFKC", text)
        text = re.sub(r"[’'`]", "", text)
        # Keep Hebrew/Arabic
        text = re.sub(r"[^\w\s\u0590-\u05FF\u0600-\u06FF]", " ", text)
        return " ".join(text.split())

    vt = normalize(video_title)
    vc = normalize(video_channel)
    ar = normalize(artist)
    tr = normalize(track_name)
    tr = re.split(r"\b(ft|feat|featuring|with|and)\b", tr)[0].strip()

    artist_words = set(w for w in ar.split() if len(w) > 1)
    track_words = set(w for w in tr.split() if len(w) > 1)
    video_text = f"{vt} {vc}"

    artist_hits = sum(1 for w in artist_words if w in video_text)
    track_hits = sum(1 for w in track_words if w in vt)

    # Accept Topic/Official channels even if artist not in title
    if ("topic" in vc or "official" in vc) and track_hits >= 1:
        return True

    # Basic overlap
    if not (track_hits >= 1 and (artist_hits >= 1 or any(w in vc for w in artist_words))):
        if not (track_hits >= max(1, int(len(track_words) * 0.4))):
            return False

    # Reject bad words (unless metadata includes them)
    BAD_WORDS = ["karaoke", "cover", "mix", "remix", "medley", "top", "compilation"]
    tr_words = set(tr.split())
    for w in BAD_WORDS:
        if w in vt and w not in tr_words:
            return False

    # Artist–channel consistency
    if artist_hits < 1 and all(w not in vc for w in artist_words):
        if "topic" not in vc and "official" not in vc:
            return False

    # Fuzzy match for robustness
    similarity = difflib.SequenceMatcher(None, tr, vt).ratio()
    if similarity < 0.35 and track_hits < 2:
        return False

    return True


# -----------------------
# YouTube Search
# -----------------------
def _yt_search(query: str, results: int = 5):
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "simulate": True,
        "skip_download": True,
        "noplaylist": True,
        "default_search": f"ytsearch{results}",
        "geo_bypass": True,
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(f"ytsearch{results}:{query}", download=False)


def search_youtube(artist: str, track_name: str) -> Optional[str]:
    query = f"{artist} {track_name} official audio"

    if (artist, track_name) in OVERRIDE:
        try:
            result = _yt_search(query, results=1)
            if result and "entries" in result and result["entries"]:
                return result["entries"][0].get("id")
        except Exception:
            return None
        return None

    try:
        result = _yt_search(query, results=5)
        if not result or "entries" not in result:
            return None
        for video in result["entries"]:
            if not video:
                continue
            vid = video.get("id")
            title = video.get("title", "")
            channel = video.get("uploader", "") or video.get("channel", "")
            if vid and validate_match(artist, track_name, title, channel):
                return vid
        return None
    except Exception as e:
        print(f"  Search error for '{query}': {e}", file=sys.stderr)
        return None


# -----------------------
# Git save/commit
# -----------------------
def save_and_commit(youtube_links, message):
    print(f"\nSaving {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(youtube_links, f, indent=2, ensure_ascii=False)

    if not os.path.exists(".git"):
        print("No .git directory — skipping commit.")
        return

    try:
        subprocess.run(["git", "add", OUTPUT_FILE], check=True)
        commit = subprocess.run(
            ["git", "commit", "-m", message],
            check=False,
            capture_output=True,
            text=True,
        )
        if commit.returncode == 0:
            subprocess.run(["git", "push"], check=True)
            print(f"✓ Pushed commit: {message}")
        else:
            print(f"Nothing new to commit: {message}")
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}", file=sys.stderr)


# -----------------------
# Cleanup
# -----------------------
def cleanup_nulls(youtube_links: dict) -> int:
    nulls = [k for k, v in youtube_links.items() if v is None]
    for k in nulls:
        del youtube_links[k]
    return len(nulls)


# -----------------------
# Thread worker
# -----------------------
def process_track(artist, track_name, album_title):
    time.sleep(random.uniform(0.05, 0.3))
    vid = search_youtube(artist, track_name)
    if vid:
        print(f"✓ Found: {artist} - {track_name} -> {vid}")
        return artist, track_name, album_title, vid
    else:
        print(f"✗ Not found: {artist} - {track_name}")
        log_missing(artist, track_name)
        return artist, track_name, album_title, None


# -----------------------
# Main
# -----------------------
def main():
    print("Loading metadata.json...")
    try:
        with open("metadata.json", "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print("Error: metadata.json not found!", file=sys.stderr)
        sys.exit(1)

    youtube_links = {}
    if os.path.exists(OUTPUT_FILE):
        print(f"Loading existing {OUTPUT_FILE}...")
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                youtube_links = json.load(f)
        except Exception as e:
            print(f"Warning: failed to parse {OUTPUT_FILE}: {e}", file=sys.stderr)

    total_tracks = sum(len(a.get("tracks", [])) for a in metadata)
    existing = len(youtube_links)
    remaining = max(0, total_tracks - existing)
    print(f"Total tracks: {total_tracks}")
    print(f"Existing entries: {existing}")
    print(f"Remaining NEW: {remaining}")

    processed = 0
    new_links = 0
    batch_count = 0
    seen_queries = set()

    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = []
            for album in metadata:
                artist = album.get("artist", "")
                album_title = album.get("title", "")
                tracks = album.get("tracks", []) or []
                if not artist or not tracks:
                    continue

                for track in tracks:
                    track_name = track.get("name", "")
                    if not track_name:
                        continue
                    key = f"{artist}|{track_name}"

                    if key in youtube_links or key in seen_queries:
                        continue
                    seen_queries.add(key)

                    if processed >= MAX_TRACKS_PER_RUN:
                        print(f"\nReached limit {MAX_TRACKS_PER_RUN}.")
                        save_and_commit(youtube_links, f"Batch {batch_count} ({processed} processed)")
                        cleanup = cleanup_nulls(youtube_links)
                        if cleanup:
                            save_and_commit(youtube_links, f"Cleanup removed {cleanup} nulls")
                        return

                    futures.append(executor.submit(process_track, artist, track_name, album_title))
                    processed += 1
                    time.sleep(DELAY_BETWEEN_SEARCHES + random.uniform(0.05, 0.35))

            for i, f in enumerate(as_completed(futures), 1):
                artist, track, album, vid = f.result()
                key = f"{artist}|{track}"
                if vid:
                    youtube_links[key] = {
                        "artist": artist,
                        "track": track,
                        "album": album,
                        "video_id": vid,
                        "url": f"https://www.youtube.com/watch?v={vid}",
                    }
                    new_links += 1
                else:
                    youtube_links[key] = None

                if i % BATCH_SIZE == 0:
                    batch_count += 1
                    save_and_commit(youtube_links, f"Batch {batch_count}: {new_links} new links")
                    print(f"\n--- Batch {batch_count} committed ---\n")

        save_and_commit(youtube_links, f"Final save: {new_links} new links")

        removed = cleanup_nulls(youtube_links)
        if removed:
            save_and_commit(youtube_links, f"Cleanup: removed {removed} nulls (final)")

        print(f"\n✓ Done — {new_links} new links, {len(youtube_links)} total.")
        print(f"Missing tracks logged to: {MISSING_LOG_FILE}")

    except KeyboardInterrupt:
        print("\nInterrupted — saving progress...")
        save_and_commit(youtube_links, "Interrupted run save")
        sys.exit(0)


if __name__ == "__main__":
    main()
