import argparse

from pipelines import (
    raw_podcast_pipeline,
    streaming_pipeline,
    summary_pipeline
)

parser = argparse.ArgumentParser()
parser.add_argument("--job", choices=[
    "raw-podcast",
    "streaming",
    "summary"
], required=True)
args = parser.parse_args()

if args.job == "raw-podcast":
    raw_podcast_pipeline.run_raw_podcast_pipeline()
elif args.job == "streaming":
    streaming_pipeline.run_streaming_pipeline()
elif args.job == "summary":
    summary_pipeline.run_summary_pipeline()