# YouTube Music JSON Search

A searchable interface for music metadata with validated YouTube links.

## Features

- 🔍 Search through thousands of albums and tracks
- ▶️ Direct YouTube links (validated)
- 🤖 Automated link generation via GitHub Actions
- 🎵 Clean, responsive interface

## How It Works

1. **metadata.json** - Contains music metadata from 24six
2. **GitHub Actions** - Automatically searches YouTube for each track
3. **youtube-links.json** - Generated file with validated YouTube video IDs
4. **index.html** - Search interface that shows real YouTube links

## YouTube Link States

- **▶ YouTube** (red, bold) - Validated direct link to the song
- **🔍 Search** (gray) - Link not yet validated, opens YouTube search
- **N/A** (gray) - Song verified as not available on YouTube

## Running Locally

The site is static HTML/JS. Just open `index.html` in a browser or run:

```bash
python -m http.server 8000
```

## Updating YouTube Links

The GitHub Action runs automatically when `metadata.json` changes, or you can trigger it manually:

1. Go to Actions tab on GitHub
2. Select "Generate YouTube Links"
3. Click "Run workflow"

The workflow will:
- Search YouTube for each track (top 3 results)
- Validate matches (artist + track name must appear in video title)
- Save validated links to `youtube-links.json`
- Commit and push the file

## Live Site

https://alltechdev.github.io/ytmusicjson/
