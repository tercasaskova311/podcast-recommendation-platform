import argparse

from spark.pipelines import (
    metadata_pipeline,
    streaming_pipeline,
    summary_pipeline,
    transcripts_pipeline
)

parser = argparse.ArgumentParser()
parser.add_argument("--job", choices=[
    "metadata",
    "transcripts",
    "streaming",
    "summary"
], required=True)
args = parser.parse_args()

if args.job == "metadata":
    metadata_pipeline.run_metadata_pipeline()
elif args.job == "transcripts":
    transcripts_pipeline.run_transcripts_en_pipeline()
elif args.job == "streaming":
    streaming_pipeline.run_streaming_pipeline()
elif args.job == "summary":
    summary_pipeline.run_summary_pipeline()