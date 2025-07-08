# Podcast Voice-to-Text Transcription Pipeline

## Overview

This pipeline fetches trending English-language podcasts and transcribes them into structured text using a local, fast, and private setup.

### What It Does

1. Fetch trending podcasts from the Podcast Index API  
2. Download one episode per show  
3. Convert audio to clean WAV format  
4. Transcribe audio with `faster-whisper` (OpenAI Whisper optimized)  
5. Output structured `.json` transcripts for each episode  

Everything runs locally without relying on external transcription services.

---

## Input

### `episodes.json`

Auto-generated file storing metadata for one episode per trending podcast.  
Each entry includes:

- `podcast_title`
- `episode_title`
- `audio_url`
- `description`
- `episode_id`

---

## Output

### `transcripts/` directory

Contains one `.json` file per episode with:

- Fully transcribed content
- Clean, chunk-stitched formatting
- Filenames based on sanitized episode titles (e.g., `Why_AI_Will_Change_Everything.json`)

---

## Key Features

- Single script handles both metadata fetching and transcription  
- English-language filtering (based on both metadata and audio detection)  
- Robust chunking to support long episodes  
- Parallel transcription using multiple processes  
- Local-only processing (no API keys or cloud infrastructure)

---

## Setup & Installation

### Prerequisites

- Python ≥ 3.8  
- `ffmpeg` (required by `pydub`)  
- `faster-whisper`

### Install Dependencies

```bash
# Install required Python packages
pip install -r requirements.txt

# If faster-whisper fails to install:
pip install git+https://github.com/guillaumekln/faster-whisper.git



Step                    Description
───────────────────────────────────────────────────────────────
1. Fetch Podcasts       → Retrieve trending shows from Podcast Index
2. Filter Language      → Keep only English podcasts
3. Fetch Episode        → Download one episode per podcast
4. Save Metadata        → Store in episodes.json
5. Download Audio       → Stream and prepare audio files
6. Convert to WAV       → Set to mono, 16kHz format for Whisper
7. Chunk Audio          → Split long episodes into 6-minute chunks
8. Transcribe Chunks    → Use faster-whisper for each chunk
9. Stitch Transcript    → Combine all chunks into one block of text
10. Save Transcript     → Write final .json to transcripts/




