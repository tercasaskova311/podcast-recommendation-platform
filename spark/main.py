import argparse

from pipelines import (
    download_transcripts_pipeline,
    analyze_transctips_pipeline
)
from scripts.streaming import streaming_user_events_pipeline

parser = argparse.ArgumentParser()

parser.add_argument("--job", choices=[
    "download-transcripts-pipeline",
    "analyze-transcripts-pipeline",
    "streaming_user_events_pipeline"
], required=True)

args = parser.parse_args()

if args.job == "download-transcripts-pipeline":
    download_transcripts_pipeline.run_pipeline()
elif args.job == "analyze-transcripts-pipeline":
    analyze_transctips_pipeline.run_pipeline()
elif args.job == "streaming_user_events_pipeline":
    streaming_user_events_pipeline.main()