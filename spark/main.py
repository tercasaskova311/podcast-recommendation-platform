import argparse

from spark.pipelines import (
    episode_raw_pipeline,
    streaming_pipeline,
    summary_pipeline
)

parser = argparse.ArgumentParser()
parser.add_argument("--job", choices=[
    "raw-episode",
    "streaming",
    "summary"
], required=True)
args = parser.parse_args()

if args.job == "raw-episode":
    episode_raw_pipeline.run_raw_episode_pipeline()
elif args.job == "streaming":
    streaming_pipeline.run_streaming_pipeline()
elif args.job == "summary":
    summary_pipeline.run_summary_pipeline()