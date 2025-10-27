#!/usr/bin/env python3
"""
Fetch YouTube video IDs for songs in metadata.json
Uses yt-dlp to search YouTube and find the best matching video
"""

import json
import os
import sys
from typing import Optional
import yt_dlp

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

    # Check if both artist and track name appear in video title
    # Allow some flexibility for variations
    artist_words = artist_lower.split()
    track_words = track_lower.split()

    # At least some artist words should be in title
    artist_match = any(word in video_title_lower for word in artist_words if len(word) > 2)

    # At least some track words should be in title
    track_match = any(word in video_title_lower for word in track_words if len(word) > 2)

    return artist_match and track_match


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
    print(f"Processing {total_tracks} tracks from {len(metadata)} albums...")

    processed = 0
    new_links = 0

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
                processed += 1
                if processed % 100 == 0:
                    print(f"Progress: {processed}/{total_tracks} tracks processed...")
                continue

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
                print(f"Found: {artist} - {track_name} -> {video_id}")
            else:
                # Store None to indicate we tried but didn't find it
                youtube_links[key] = None
                print(f"Not found: {artist} - {track_name}")

            processed += 1

            # Progress update every 10 tracks
            if processed % 10 == 0:
                print(f"Progress: {processed}/{total_tracks} tracks processed, {new_links} new links found")

    # Save results
    print(f"\nSaving youtube-links.json...")
    with open('youtube-links.json', 'w', encoding='utf-8') as f:
        json.dump(youtube_links, f, indent=2, ensure_ascii=False)

    print(f"\nComplete! Processed {processed} tracks, found {new_links} new YouTube links")
    print(f"Total links in database: {sum(1 for v in youtube_links.values() if v is not None)}")

if __name__ == '__main__':
    main()
