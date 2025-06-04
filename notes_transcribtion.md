# Voice-to-Text Transcription Script

## Overview  
This script automates the process of downloading podcast episodes, converting audio to the appropriate format, and transcribing their spoken content using the Whisper speech-to-text model.

---

## Inputs  
- **top_podcasts.json**  
  Contains podcast metadata from the Podcast Index API, including fields like `title`, `description`, `id`, and `author`.

- **top_episodes.json**  
  Contains episode metadata, including `audio_url`, `title`, and `id`.

---

## Outputs  
- Transcripts are saved as JSON files inside the `transcripts/` directory.

---

## Key Features  
- Fetches trending podcast episodes based on recent listener engagement, recent uploads, total plays, etc.  
- Use Hugging Face’s Whisper speech-to-text model. 
- Parallel processing with configurable worker count for faster transcription.

---

## Setup & Configuration  

- **Authentication:**  
  Env variable `HUGGINGFACE_TOKEN` with Hugging Face token.

- **Model Size:**  
  Whisper model size (`tiny`, `base`, `small`, etc.) speed vs. accuracy.

- **Concurrency:**  
  Adjust `MAX_WORKERS` to set how many episodes are processed in parallel (speed).

---

## How It Works 

1. **Load Episode Metadata:**  
   Reads `top_episodes.json` to get episode details, including audio URLs.

2. **Download Audio:**  
   Downloads audio files in chunks for efficient handling of large files.

3. **Convert Audio Format:**  
   Converts audio to WAV, mono channel, 16kHz sample rate — the required format for Whisper transcription.

4. **Handle Temporary Files:**  
   Saves the converted audio temporarily on disk for processing.

5. **Transcribe Audio:**  
   Uses the Whisper model to convert audio to text, capturing spoken content.

6. **Assemble Transcript:**  
   Combines transcribed segments into a full transcript string.

7. **Parallel Processing:**  
   Uses `ThreadPoolExecutor` to process multiple episodes simultaneously, controlled by the `MAX_WORKERS` setting (default: 2).

---
