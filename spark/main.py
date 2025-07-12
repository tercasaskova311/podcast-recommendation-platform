import argparse

from spark.pipelines import (
    download_transcripts_pipeline
)

parser = argparse.ArgumentParser()
parser.add_argument("--job", choices=[
    "downlaod-transcripts-pipeline"
], required=True)
args = parser.parse_args()

if args.job == "downlaod-transcripts-pipeline":
    download_transcripts_pipeline.run_get_transcripts_pipeline()