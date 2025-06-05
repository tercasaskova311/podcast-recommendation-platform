# Voice-to-Text Transcription Script

## Overview  
This script automates the process of downloading podcast episodes, converting audio to the proper format, and transcribing spoken content using the Whisper speech-to-text model.

We chose **Podcast Index metadata** because it offers an **open API with trending podcast data**. Most major podcast providers restrict API access, making Podcast Index one of the few open-source platforms for podcast metadata.

The transcription engine is based on the **fast-whisper** model—a fine-tuned, efficient implementation of OpenAI’s Whisper model. While some other services (e.g., AssemblyAI) provide faster or lighter transcription APIs, they often come with API restrictions or costs. Fast-whisper offers a good balance between speed and accuracy with local deployment.

---

## Inputs  
- **`top_podcasts.json`**  
  Contains podcast metadata from Podcast Index API: `title`, `description`, `id`, `author`, etc.

- **`episodes_url.json`**  
  Retrieved the  `audio_url` for given podcast + contains othermeta data.

---

## Outputs  
- Transcripts are saved as JSON files inside the `transcripts/` directory, named by sanitized episode titles.

---

## Key Features  
- Fetches trending podcast episodes based on metrics like recent listens, uploads, and total plays.  
- Uses Hugging Face’s **fast-whisper** speech-to-text model for efficient local transcription.  
- Supports parallel processing with configurable worker count to transcribe multiple episodes simultaneously.

---

## Setup & Configuration  

- **Authentication:**  
  Set environment variable `HUGGINGFACE_TOKEN` with your Hugging Face API token.

- **Model Size:**  
  Select Whisper model size (`tiny`, `base`, `small`, etc.) to balance speed and transcription accuracy.

- **Concurrency:**  
  Configure `MAX_WORKERS` to control how many episodes are processed in parallel (default is 2).

## Installation Instructions

1. **Install `ffmpeg` (required by `pydub`)**

- macOS (Homebrew):
  ```bash
  brew install ffmpeg
- Ubuntu/Debian:
sudo apt update && sudo apt install ffmpeg


2. **If faster-whisper install fails (alternative method):** 
``` pip install git+https://github.com/guillaumekln/faster-whisper.git
---

## How It Works  

The pipeline consists of these stages:  
**Download → Convert → Chunk → Transcribe → Save**

1. **Load Episode Metadata**  
   Reads episode details, including audio URLs, from `episodes_url.json`.

2. **Download Audio**  
   Downloads audio in chunks for efficient memory and network usage.

3. **Convert Audio Format**  
   Converts audio to WAV format, with mono channel and 16kHz sample rate—the input format required by Whisper.

4. **Temporary File Handling**  
   Saves chunks as temporary WAV files for transcription.

5. **Transcribe Audio**  
   Processes audio chunks with the Whisper model to generate text transcripts.

6. **Assemble Transcript**  
   Combines transcribed chunks into a full episode transcript.

7. **Parallel Processing**  
   Uses `ProcessPoolExecutor` with configurable `MAX_WORKERS` to transcribe multiple episodes concurrently.

---
