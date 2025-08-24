import os
from typing import Any, Dict, List, Tuple, Set
import pyarrow as pa
import pandas as pd
from deltalake import DeltaTable, write_deltalake

# =========================
# Delta helpers (no Spark)
# =========================
def _delta_exists(path: str) -> bool:
    return os.path.exists(os.path.join(path, "_delta_log"))


def _safe_read_df(path: str) -> pd.DataFrame:
    try:
        return DeltaTable(path).to_pandas()
    except Exception:
        return pd.DataFrame()


def ensure_table(path: str, example_row: Dict[str, Any]):
    """Create empty Delta table (schema inferred) if missing."""
    if _delta_exists(path):
        return
    df = pd.DataFrame([example_row] if example_row else [{}])
    write_deltalake(path, df, mode="overwrite")


def get_existing_ids(path: str, id_col: str) -> Set[str]:
    """Return set of existing IDs in a Delta table (empty if missing)."""
    if not _delta_exists(path):
        return set()
    df = _safe_read_df(path)
    if df.empty or id_col not in df.columns:
        return set()
    return set(df[id_col].astype(str).tolist())

def write_delta_overwrite(path: str, df: pd.DataFrame, schema: pa.Schema) -> int:
    """Create a delta table with the specified schema and adding the data"""
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)
    write_deltalake(path, table, mode="overwrite")  # creates table if missing
    return len(df)

def upsert_delta(path: str, rows: List[Dict[str, Any]], key: str):
    """
    Idempotent upsert by `key`.
    Try delta-rs MERGE; fallback to Pandas de-dup overwrite.
    """
    if not rows:
        return
    ensure_table(path, rows[0])
    incoming = pd.DataFrame(rows)
    incoming[key] = incoming[key].astype(str)

    # Try MERGE (available on recent deltalake)
    try:
        dt = DeltaTable(path)
        (
            dt.merge(
                incoming,
                predicate=f"s.{key} = t.{key}",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        return
    except Exception:
        pass  # Fallback below

    current = _safe_read_df(path)
    if not current.empty and key in current.columns:
        current[key] = current[key].astype(str)
        combined = pd.concat([current, incoming], ignore_index=True)
        combined.sort_values(key, inplace=True, kind="stable")
        combined.drop_duplicates(key, keep="last", inplace=True)
    else:
        combined = incoming

    write_deltalake(path, combined, mode="overwrite")