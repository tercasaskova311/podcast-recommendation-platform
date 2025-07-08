# Podcast Voice-to-Text Transcription Pipeline

## Overview

1. Fetching trending English-language podcasts via the **Podcast Index API**  
2. Downloading one episode per podcast  
3. Converting audio into proper WAV format  
4. Transcribing speech into text using the **faster-whisper** (Whisper by OpenAI) model  
5. Saving transcripts as structured `.json` files

We chose **Podcast Index** for its **open-access podcast metadata**, unlike most commercial platforms. The transcription engine, **fast-whisper**, is a faster, optimized version of OpenAI’s Whisper and runs **locally**.

---

## Input

- **`episodes.json`**  
  Auto-generated file containing one episode per English-language trending podcast. Includes:
  - `podcast_title`
  - `episode_title`
  - `audio_url`
  - `description`
  - `episode_id`  
---

##  Output

- **`transcripts/` directory**  
  Contains one `.json` file per episode, with clean, chunk-stitched transcript text.  
  Filenames are sanitized from episode titles (e.g., `Why_AI_Will_Change_Everything.json`).

---

##  Key Features

- **One script** handles both **fetching** and **transcribing**
- Filters for **English-language** shows using both metadata and language detection
- Supports **chunked processing** for long-form audio
- **Parallel transcription** with configurable number of processes
- **Local** transcription—no 3rd-party services or cloud dependencies

---

##  Setup & Configuration

### Prerequisites

- Python ≥ 3.8  
- `ffmpeg` (used by `pydub`)  
- `faster-whisper` + dependencies  

###  Install requirements

```bash

If faster-whisper fails to install, try:
pip install git+https://github.com/guillaumekln/faster-whisper.git

Install ffmpeg

macOS (Homebrew):
brew install ffmpeg

Script Behavior

The pipeline follows a clear flow:

🔄 Pipeline Stages
Step	Description
1. Fetch Podcasts	Gets top 50 trending podcasts from Podcast Index
2. Filter Language	Keeps only English podcasts (metadata + detection)
3. Fetch Episode	Retrieves 1 episode per podcast (via feed ID)
4. Save Metadata	Saves all usable episodes to episodes.json
5. Download Audio	Streams audio in memory for each episode
6. Convert to WAV	Sets to mono, 16kHz for Whisper compatibility
7. Chunk Audio	Splits long episodes into 6-min chunks
8. Transcribe Chunks	Runs faster-whisper on each chunk
9. Stitch Transcript	Combines all chunks into one text block
10. Save Transcript	Writes final .json transcript file

