import argparse

from spark.pipelines import (
    metadata_pipeline,
<<<<<<< HEAD
    transcripts_en_pipeline,
=======
>>>>>>> fe71a82ba088cd2f7045f2324c5ded66a84ca6ec
    streaming_pipeline,
    summary_pipeline,
    transcripts_pipeline
)

parser = argparse.ArgumentParser()
parser.add_argument("--job", choices=[
    "metadata",
<<<<<<< HEAD
    "transcripts-en",
=======
    "transcripts",
>>>>>>> fe71a82ba088cd2f7045f2324c5ded66a84ca6ec
    "streaming",
    "summary"
], required=True)
args = parser.parse_args()

if args.job == "metadata":
    metadata_pipeline.run_metadata_pipeline()
<<<<<<< HEAD
elif args.job == "transcripts-en":
    transcripts_en_pipeline.run_transcripts_en_pipeline()
=======
elif args.job == "transcripts":
    transcripts_pipeline.run_transcripts_en_pipeline()
>>>>>>> fe71a82ba088cd2f7045f2324c5ded66a84ca6ec
elif args.job == "streaming":
    streaming_pipeline.run_streaming_pipeline()
elif args.job == "summary":
    summary_pipeline.run_summary_pipeline()