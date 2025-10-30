#!/usr/bin/env python3
"""
Robust Artist ID Fetcher with Discography Validation

This script fetches YouTube channel IDs for artists using ytmusicapi
and validates them against known discography from metadata.json.

Only artists with confidence >= 70 are included in the output.
"""

import json
import re
import time
from collections import defaultdict
from difflib import SequenceMatcher
from ytmusicapi import YTMusic

# Initialize ytmusicapi
ytm = YTMusic()

def normalize_text(text):
    """Normalize text for comparison"""
    if not text:
        return ""
    # Convert to lowercase
    text = text.lower()
    # Remove Hebrew diacritics and special chars
    text = re.sub(r'[^\w\s\u0590-\u05FF-]', ' ', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text

def calculate_similarity(str1, str2):
    """Calculate similarity ratio between two strings"""
    norm1 = normalize_text(str1)
    norm2 = normalize_text(str2)
    return SequenceMatcher(None, norm1, norm2).ratio()

def extract_artist_discography(metadata_path):
    """
    Extract artist names and their albums/tracks from metadata.json

    Returns: dict mapping artist_name -> {albums: [...], tracks: [...]}
    """
    print(f"Loading metadata from {metadata_path}...")
    with open(metadata_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    # Group by artist
    artist_data = defaultdict(lambda: {'albums': set(), 'tracks': set()})

    for item in metadata:
        if item.get('status') != 'success':
            continue

        artist = item.get('artist')
        if not artist:
            continue
        artist = artist.strip()
        if not artist:
            continue

        album_title = item.get('title', '').strip()
        if album_title:
            artist_data[artist]['albums'].add(album_title)

        # Add track names
        for track in item.get('tracks', []):
            track_name = track.get('name', '').strip()
            if track_name:
                artist_data[artist]['tracks'].add(track_name)

    # Convert sets to lists for JSON serialization later
    for artist in artist_data:
        artist_data[artist]['albums'] = list(artist_data[artist]['albums'])
        artist_data[artist]['tracks'] = list(artist_data[artist]['tracks'])

    print(f"Extracted discography for {len(artist_data)} artists")
    return dict(artist_data)

def search_artist_ytmusic(artist_name, max_retries=3):
    """
    Search for artist using multiple ytmusicapi strategies

    Returns: list of candidate results with channelId
    """
    candidates = []

    strategies = [
        # Strategy 1: Direct artist search
        lambda: ytm.search(artist_name, filter="artists", limit=5),
        # Strategy 2: Topic channel search (auto-generated channels)
        lambda: ytm.search(f"{artist_name} - Topic", filter="artists", limit=5),
    ]

    for strategy_idx, strategy in enumerate(strategies):
        for attempt in range(max_retries):
            try:
                results = strategy()
                if results:
                    for result in results:
                        channel_id = result.get('browseId')
                        if channel_id and channel_id.startswith('UC'):
                            # Extract channel name
                            channel_name = result.get('artist') or result.get('name', '')

                            # Add to candidates if not duplicate
                            if not any(c['channelId'] == channel_id for c in candidates):
                                candidates.append({
                                    'channelId': channel_id,
                                    'channelName': channel_name,
                                    'strategy': strategy_idx + 1
                                })
                    break  # Success, exit retry loop
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"  ⚠️  Search failed for {artist_name}: {e}")

    return candidates

def fetch_channel_uploads(channel_id, limit=50):
    """
    Fetch channel's uploads/albums

    Returns: list of video/album titles
    """
    titles = []

    try:
        # Get artist info including albums
        artist_info = ytm.get_artist(channel_id)

        # Extract album titles
        if artist_info and 'albums' in artist_info:
            albums = artist_info['albums'].get('results', [])
            for album in albums[:limit]:
                title = album.get('title', '')
                if title:
                    titles.append(title)

        # Also check singles/EPs
        if artist_info and 'singles' in artist_info:
            singles = artist_info['singles'].get('results', [])
            for single in singles[:limit]:
                title = single.get('title', '')
                if title:
                    titles.append(title)

        # If no albums found, try getting uploads
        if not titles and artist_info and 'songs' in artist_info:
            songs = artist_info['songs'].get('results', [])
            for song in songs[:limit]:
                title = song.get('title', '')
                if title:
                    titles.append(title)

    except Exception as e:
        print(f"  ⚠️  Could not fetch uploads for {channel_id}: {e}")

    return titles

def validate_discography_match(channel_uploads, known_albums, known_tracks):
    """
    Check how many known albums/tracks appear in channel's uploads

    Returns: (match_percentage, num_matches)
    """
    if not known_albums and not known_tracks:
        return 0, 0

    matched_albums = 0
    for known_album in known_albums:
        for upload_title in channel_uploads:
            if calculate_similarity(known_album, upload_title) >= 0.75:
                matched_albums += 1
                break  # Found match, move to next known album

    # Also check track matches (less weight)
    matched_tracks = 0
    for known_track in known_tracks[:20]:  # Limit to avoid too many comparisons
        for upload_title in channel_uploads:
            if calculate_similarity(known_track, upload_title) >= 0.80:
                matched_tracks += 1
                break

    # Calculate match percentage
    total_known = len(known_albums) + min(len(known_tracks), 10) * 0.3  # Tracks have less weight
    total_matched = matched_albums + matched_tracks * 0.3

    if total_known == 0:
        return 0, 0

    match_percentage = (total_matched / total_known) * 100
    num_matches = matched_albums + matched_tracks
    return min(100, match_percentage), num_matches

def calculate_confidence(artist_name, candidate, discography_score, num_matches):
    """
    Calculate overall confidence score (0-100)

    Components:
    - Name similarity: 0-60 points (increased weight)
    - Discography match: 0-40 points (decreased weight)
    - Bonus: up to +15 for perfect name + validation
    """
    # Name similarity score (0-60)
    name_sim = calculate_similarity(artist_name, candidate['channelName'])
    name_score = name_sim * 60

    # Discography score (0-40)
    disco_score = (discography_score / 100) * 40

    total = name_score + disco_score

    # Bonus: Perfect/near-perfect name match with some discography validation
    if name_sim >= 0.95 and num_matches >= 2:
        total = min(100, total + 15)  # Bonus for strong name + validation
    elif name_sim >= 0.90 and num_matches >= 3:
        total = min(100, total + 10)

    return {
        'total': round(total, 1),
        'name_similarity': round(name_sim * 100, 1),
        'discography_match': round(discography_score, 1)
    }

def process_artist(artist_name, discography):
    """
    Process single artist: search, validate, score

    Returns: dict with channelId and confidence, or None if not found
    """
    # Search for candidates
    candidates = search_artist_ytmusic(artist_name)

    if not candidates:
        return None

    # Validate each candidate
    best_candidate = None
    best_confidence = 0

    for candidate in candidates:
        # Fetch channel uploads
        uploads = fetch_channel_uploads(candidate['channelId'])

        if not uploads:
            continue

        # Validate against discography
        disco_score, num_matches = validate_discography_match(
            uploads,
            discography['albums'],
            discography['tracks']
        )

        # Calculate confidence
        confidence = calculate_confidence(artist_name, candidate, disco_score, num_matches)

        if confidence['total'] > best_confidence:
            best_confidence = confidence['total']
            best_candidate = {
                'channelId': candidate['channelId'],
                'channelName': candidate['channelName'],
                'confidence': confidence,
                'strategy': candidate['strategy']
            }

    return best_candidate

def main():
    """Main processing loop"""
    # Paths
    metadata_path = '/home/asternheim/json/ytmusicjson-main/metadata.json'
    output_path = '/home/asternheim/ytjson/artists_verified.json'
    detailed_output_path = '/home/asternheim/ytjson/artists_verified_detailed.json'

    # Extract discography from metadata
    artist_discography = extract_artist_discography(metadata_path)

    print(f"\nProcessing {len(artist_discography)} artists...")
    print("=" * 60)

    # Process each artist
    results_simple = []  # Simple format matching original artists.json
    results_detailed = []  # Detailed format with confidence scores
    found_count = 0
    skipped_count = 0

    for idx, (artist_name, discography) in enumerate(artist_discography.items(), 1):
        print(f"\n[{idx}/{len(artist_discography)}] {artist_name}")

        result = process_artist(artist_name, discography)

        if result and result['confidence']['total'] >= 70:
            found_count += 1
            print(f"  ✓ Found: {result['channelName']}")
            print(f"    Confidence: {result['confidence']['total']}% "
                  f"(name: {result['confidence']['name_similarity']}%, "
                  f"disco: {result['confidence']['discography_match']}%)")

            # Simple format (matches original artists.json)
            results_simple.append({
                'id': result['channelId'],
                'name': artist_name
            })

            # Detailed format (for review)
            results_detailed.append({
                'id': result['channelId'],
                'name': artist_name,
                'confidence': result['confidence']['total'],
                'matchedChannelName': result['channelName'],
                'validationDetails': result['confidence']
            })
        else:
            skipped_count += 1
            if result:
                print(f"  ✗ Skipped (low confidence: {result['confidence']['total']}%)")
            else:
                print(f"  ✗ Skipped (not found)")

        # Rate limiting
        time.sleep(0.5)

    # Save results
    print("\n" + "=" * 60)
    print(f"Processing complete!")
    print(f"  Found: {found_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Success rate: {found_count / len(artist_discography) * 100:.1f}%")

    # Save simple format (matches original artists.json structure)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({'artists': results_simple}, f, indent=2, ensure_ascii=False)

    # Save detailed format (for review)
    with open(detailed_output_path, 'w', encoding='utf-8') as f:
        json.dump(results_detailed, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved {len(results_simple)} verified artists to: {output_path}")
    print(f"✓ Saved detailed version to: {detailed_output_path}")

if __name__ == '__main__':
    main()
