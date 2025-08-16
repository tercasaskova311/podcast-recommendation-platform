import argparse

from pipelines import (
    download_transcripts_pipeline,
    analyze_transctips_pipeline
)

parser = argparse.ArgumentParser()

parser.add_argument("--job", choices=[
    "download-transcripts-pipeline",
    "analyze-transcripts-pipeline"
], required=True)

args = parser.parse_args()

if args.job == "download-transcripts-pipeline":
    download_transcripts_pipeline.run_pipeline()
elif args.job == "analyze-transcripts-pipeline":
    analyze_transctips_pipeline.run_pipeline()