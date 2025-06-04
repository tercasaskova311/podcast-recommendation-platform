import argparse

from spark.pipelines import (
    metadata_pipeline,
    transcripts_en_pipeline,
    streaming_pipeline,
    summary_pipeline
)

parser = argparse.ArgumentParser()
parser.add_argument("--job", choices=[
    "metadata",
    "transcripts-en",
    "streaming",
    "summary"
], required=True)
args = parser.parse_args()

if args.job == "metadata":
    metadata_pipeline.run_metadata_pipeline()
elif args.job == "transcripts-en":
    transcripts_en_pipeline.run_transcripts_en_pipeline()
elif args.job == "streaming":
    streaming_pipeline.run_streaming_pipeline()
elif args.job == "summary":
    summary_pipeline.run_summary_pipeline()