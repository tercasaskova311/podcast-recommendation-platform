from deltalake import DeltaTable
from config.settings import DELTA_PATH_TRANSCRIPTS

# Open Delta table
dt = DeltaTable(DELTA_PATH_TRANSCRIPTS)

# Convert to pandas DataFrame
df = dt.to_pandas()

# Filter rows where failed == True
failed_df = df[df["failed"] == True]

print(failed_df)

# Optionally convert to list of dicts
failed_records = failed_df.to_dict(orient="records")
print(failed_records)
