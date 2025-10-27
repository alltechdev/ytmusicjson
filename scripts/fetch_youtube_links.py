#!/usr/bin/env python3
"""
Fetch YouTube video IDs for songs in metadata.json
Uses yt-dlp to search YouTube and find the best matching video
Processes in batches with incremental commits for large datasets
"""

import json
import os
import sys
import time
import subprocess
from typing import Optional
import yt_dlp

# Configuration
BATCH_SIZE = 100  # Commit every 100 tracks
DELAY_BETWEEN_SEARCHES = 0.5  # Delay in seconds to avoid rate limiting
MAX_TRACKS_PER_RUN = 10000  # Process max 10000 tracks per workflow run

def validate_match(artist: str, track_name: str, video_title: str, video_channel: str = "") -> bool:
    """
    Validate if the video likely matches the artist and track
    Uses very lenient matching - better to have some false positives than miss valid songs

    Args:
        artist: Expected artist name
        track_name: Expected track name
        video_title: Actual video title from YouTube
        video_channel: Channel name (optional)

    Returns:
        True if it's likely a match, False only if clearly wrong
    """
    video_title_lower = video_title.lower()
    video_channel_lower = video_channel.lower() if video_channel else ""
    artist_lower = artist.lower()
    track_lower = track_name.lower()

    # Remove special characters and extra whitespace for comparison
    import re
    def clean(text):
        # Remove special chars but keep spaces and letters
        text = re.sub(r'[^\w\s]', ' ', text)
        # Normalize whitespace
        return ' '.join(text.split())

    video_title_clean = clean(video_title_lower)
    artist_clean = clean(artist_lower)
    track_clean = clean(track_lower)

    # Strip featured artists from track name for better matching
    # Match patterns like: ft., feat., featuring, &, with, etc.
    track_clean_no_feat = re.split(r'\b(ft|feat|featuring|with|and)\b', track_clean)[0].strip()

    # Strategy: Be EXTREMELY lenient. Trust YouTube's search algorithm.
    # YouTube is very good at finding the right video, so if it's the top result, likely correct

    # Get meaningful words (more than 1 char)
    artist_words = [w for w in artist_clean.split() if len(w) > 1]
    track_words = [w for w in track_clean_no_feat.split() if len(w) > 1]

    # Count how many words match
    artist_matches = sum(1 for word in artist_words if word in video_title_clean or word in video_channel_lower)
    track_matches = sum(1 for word in track_words if word in video_title_clean)

    # EXTREMELY lenient criteria - accept if ANY of these are true:
    # 1. Any artist word + any track word
    if artist_matches >= 1 and track_matches >= 1:
        return True

    # 2. ANY track words matched (even just one)
    if track_matches >= 1:
        return True

    # 3. Artist in channel name (official channel)
    if video_channel_lower and any(word in video_channel_lower for word in artist_words):
        return True

    # 4. Full track name appears (even if very short)
    if len(track_clean_no_feat) > 3 and track_clean_no_feat in video_title_clean:
        return True

    # 5. Artist name in title (even without track name - might be compilation/album)
    if artist_matches >= len(artist_words) * 0.5:  # At least half of artist words
        return True

    # If none of the above, it's probably not the right video
    return False


def search_youtube(artist: str, track_name: str) -> Optional[str]:
    """
    Search YouTube for a song and return the video ID of the best match

    Args:
        artist: Artist name
        track_name: Track/song name

    Returns:
        YouTube video ID (e.g., 'dQw4w9WgXcQ') or None if not found
    """
    query = f"{artist} {track_name}"

    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True,
        'default_search': 'ytsearch5',  # Get top 5 results for validation
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch5:{query}", download=False)

            if result and 'entries' in result and len(result['entries']) > 0:
                # Try to find the best validated match
                for video in result['entries']:
                    if not video:
                        continue

                    video_id = video.get('id')
                    video_title = video.get('title', '')
                    video_channel = video.get('uploader', '') or video.get('channel', '')

                    # Validate that this video actually matches our search
                    if validate_match(artist, track_name, video_title, video_channel):
                        return video_id

                # If still no match after checking 5 results, it probably doesn't exist
                # But log it so we can review
                print(f"  Warning: No validated match for '{query}'", file=sys.stderr)
                return None

    except Exception as e:
        print(f"  Error searching for '{query}': {e}", file=sys.stderr)

    return None

def save_and_commit(youtube_links, message):
    """Save youtube-links.json and commit to git"""
    print(f"\nSaving youtube-links.json...")
    with open('youtube-links.json', 'w', encoding='utf-8') as f:
        json.dump(youtube_links, f, indent=2, ensure_ascii=False)

    # Git commit
    try:
        subprocess.run(['git', 'add', 'youtube-links.json'], check=True)
        subprocess.run(['git', 'commit', '-m', message], check=True)
        subprocess.run(['git', 'push'], check=True)
        print(f"✓ Committed and pushed: {message}")
    except subprocess.CalledProcessError as e:
        print(f"Warning: Git operation failed: {e}", file=sys.stderr)


def main():
    """Main function to process metadata and generate YouTube links"""

    # Load metadata
    print("Loading metadata.json...")
    try:
        with open('metadata.json', 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print("Error: metadata.json not found!", file=sys.stderr)
        sys.exit(1)

    # Try to load existing youtube-links.json to avoid re-processing
    youtube_links = {}
    if os.path.exists('youtube-links.json'):
        print("Loading existing youtube-links.json...")
        try:
            with open('youtube-links.json', 'r', encoding='utf-8') as f:
                youtube_links = json.load(f)
        except:
            pass

    # Process each album and track
    total_tracks = sum(len(album.get('tracks', [])) for album in metadata)
    existing_tracks = len(youtube_links)
    remaining_tracks = total_tracks - existing_tracks

    print(f"Total tracks: {total_tracks}")
    print(f"Already processed: {existing_tracks}")
    print(f"Remaining: {remaining_tracks}")
    print(f"Will process up to {MAX_TRACKS_PER_RUN} tracks this run")

    processed = 0
    new_links = 0
    batch_count = 0

    for album in metadata:
        artist = album.get('artist', '')
        album_title = album.get('title', '')
        tracks = album.get('tracks', [])

        if not artist or not tracks:
            continue

        for track in tracks:
            track_name = track.get('name', '')
            if not track_name:
                continue

            # Create unique key for this track
            key = f"{artist}|{track_name}"

            # Skip if we already have this link
            if key in youtube_links:
                continue

            # Stop if we've hit the limit for this run
            if processed >= MAX_TRACKS_PER_RUN:
                print(f"\nReached limit of {MAX_TRACKS_PER_RUN} tracks for this run")
                save_and_commit(youtube_links, f"Auto-update: {new_links} new YouTube links ({existing_tracks + processed} total)")
                print(f"\n✓ Session complete! Processed {processed} new tracks")
                print(f"Run the workflow again to continue processing remaining {remaining_tracks - processed} tracks")
                return

            # Search YouTube for this track
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
                # Store None to indicate we tried but didn't find it
                youtube_links[key] = None
                print(f"✗ Not found: {artist} - {track_name}")

            processed += 1

            # Delay to avoid rate limiting
            time.sleep(DELAY_BETWEEN_SEARCHES)

            # Commit batch
            if processed % BATCH_SIZE == 0:
                batch_count += 1
                save_and_commit(youtube_links, f"Batch {batch_count}: {new_links} new links ({existing_tracks + processed} total)")
                print(f"\n--- Batch {batch_count} committed ({processed}/{MAX_TRACKS_PER_RUN} processed this run) ---\n")

    # Final save
    if processed > 0:
        save_and_commit(youtube_links, f"Auto-update: {new_links} new YouTube links ({existing_tracks + processed} total)")

    print(f"\n✓ Complete! Processed {processed} tracks, found {new_links} new YouTube links")
    print(f"Total links in database: {sum(1 for v in youtube_links.values() if v is not None)}")

    if existing_tracks + processed < total_tracks:
        print(f"\n⚠ Still {total_tracks - existing_tracks - processed} tracks remaining")
        print("Run the workflow again to continue processing")

if __name__ == '__main__':
    main()
