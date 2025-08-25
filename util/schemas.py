EPISODES_SCHEMA = {
    "episode_id": "string",
    "podcast_title": "string",
    "podcast_author": "string",
    "podcast_url": "string",
    "episode_title": "string",
    "description": "string",
    "audio_url": "string",
    "json_str": "string"
}

TRANSCRIPTS_SCHEMA = {
    "episode_id": "string",
    "transcript": "string",
    "failed": "boolean",
    "error": "string",
    "duration": "double",      # seconds; keep as float
    "language": "string",
    "analyzed": "boolean",
    "ingest_ts": "string",      # keep STRING to avoid Spark<->delta-rs timestamp quirks
    "retry_count": "integer"
}
