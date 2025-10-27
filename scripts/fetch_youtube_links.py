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
MAX_TRACKS_PER_RUN = 1000  # Process max 1000 tracks per workflow run

def validate_match(artist: str, track_name: str, video_title: str) -> bool:
    """
    Validate if the video title likely matches the artist and track

    Args:
        artist: Expected artist name
        track_name: Expected track name
        video_title: Actual video title from YouTube

    Returns:
        True if it's likely a match, False otherwise
    """
    video_title_lower = video_title.lower()
    artist_lower = artist.lower()
    track_lower = track_name.lower()

    # Clean up common patterns
    track_lower = track_lower.replace('feat.', '').replace('ft.', '')
    video_title_lower = video_title_lower.replace('official', '').replace('video', '').replace('audio', '')

    # Extract meaningful words (more than 2 chars, not common words)
    common_words = {'the', 'and', 'feat', 'with', 'from', 'intro', 'outro'}
    artist_words = [w for w in artist_lower.split() if len(w) > 2 and w not in common_words]
    track_words = [w for w in track_lower.split() if len(w) > 2 and w not in common_words]

    # Check artist match - at least one significant word from artist name
    artist_match = False
    if artist_words:
        # For multi-word artists, check if last name appears (often most distinctive)
        if len(artist_words) > 1:
            artist_match = artist_words[-1] in video_title_lower
        # Or any significant artist word
        if not artist_match:
            artist_match = any(word in video_title_lower for word in artist_words)
    else:
        # If no significant words, just check the full artist name
        artist_match = artist_lower in video_title_lower

    # Check track match - at least one significant word from track name
    track_match = False
    if track_words:
        # Check if any significant track words appear
        track_match = any(word in video_title_lower for word in track_words)
    else:
        # If no significant words, check the full track name
        track_match = track_lower in video_title_lower

    # More lenient: match if we have artist OR strong track match
    # Strong track match = multiple words or full phrase
    strong_track_match = len([w for w in track_words if w in video_title_lower]) >= 2 or track_lower in video_title_lower

    return (artist_match and track_match) or strong_track_match


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
        'default_search': 'ytsearch3',  # Get top 3 results for validation
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch3:{query}", download=False)

            if result and 'entries' in result and len(result['entries']) > 0:
                # Try to find the best validated match
                for video in result['entries']:
                    if not video:
                        continue

                    video_id = video.get('id')
                    video_title = video.get('title', '')

                    # Validate that this video actually matches our search
                    if validate_match(artist, track_name, video_title):
                        return video_id

                # If no validated match, return None instead of the first result
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
