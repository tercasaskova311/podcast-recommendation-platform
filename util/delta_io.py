# util/delta_io.py
import os
from typing import Any, Dict, List, Set, Optional

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


# ---------- Schema helpers ----------

# Map your simple type names to PyArrow types
_TYPE_MAP = {
    "string": pa.string(),
    "boolean": pa.bool_(),
    "double": pa.float64(),
    "integer": pa.int64(),
}

def to_arrow_schema(schema_dict: Dict[str, str]) -> pa.Schema:
    return pa.schema([(col, _TYPE_MAP[typ]) for col, typ in schema_dict.items()])

def coerce_df_to_schema(df: pd.DataFrame, schema_dict: Dict[str, str]) -> pd.DataFrame:
    """Ensure df has exactly these columns and dtypes; coerce safely."""
    import numpy as np

    # Ensure columns exist in the right order; drop extras
    for col in schema_dict.keys():
        if col not in df.columns:
            df[col] = None
    df = df[list(schema_dict.keys())].copy()

    # Coerce by logical type
    for col, typ in schema_dict.items():
        s = df[col]
        if typ == "string":
            df[col] = s.apply(lambda x: None if (x is None or (isinstance(x, str) and x.strip() == "")) else str(x))
        elif typ == "boolean":
            def _to_bool(x):
                if isinstance(x, (bool, np.bool_)): return bool(x)
                if x in (1, "1", "true", "True", "t", "T", "yes", "y", "Y"): return True
                if x in (0, "0", "false", "False", "f", "F", "no", "n", "N"): return False
                # treat everything else (None, "boolean", "", etc.) as False by default
                return False
            df[col] = s.map(_to_bool).astype(bool)
        elif typ == "double":
            df[col] = pd.to_numeric(s, errors="coerce")  # float with NaN allowed
        elif typ == "integer":
            ser = pd.to_numeric(s, errors="coerce").fillna(0)
            # use Int64? Delta/Arrow want physical types; we’ll cast to int64 cleanly
            df[col] = ser.astype("int64")
        else:
            # default: leave as-is
            pass

    return df


# ---------- Delta helpers (no Spark) ----------

def _delta_exists(path: str) -> bool:
    return os.path.exists(os.path.join(path, "_delta_log"))

def _safe_read_df(path: str) -> pd.DataFrame:
    try:
        return DeltaTable(path).to_pandas()
    except Exception:
        return pd.DataFrame()

def ensure_table(path: str, schema_dict: Dict[str, str]) -> None:
    """
    Create an EMPTY Delta table with the given schema if missing.
    (No placeholder rows; prevents 'boolean'/'integer' strings ever landing in data.)
    """
    if _delta_exists(path):
        return
    empty_tbl = pa.Table.from_arrays(
        [pa.array([], type=_TYPE_MAP[t]) for t in schema_dict.values()],
        names=list(schema_dict.keys()),
    )
    write_deltalake(path, empty_tbl, mode="overwrite")

def get_existing_ids(path: str, id_col: str) -> Set[str]:
    if not _delta_exists(path):
        return set()
    df = _safe_read_df(path)
    if df.empty or id_col not in df.columns:
        return set()
    return set(df[id_col].astype(str).tolist())

def write_delta_overwrite(path: str, df: pd.DataFrame, schema_dict: Dict[str, str]) -> int:
    """Create/overwrite a Delta table using the provided logical schema dict."""
    ensure_table(path, schema_dict)
    df = coerce_df_to_schema(df, schema_dict)
    table = pa.Table.from_pandas(df, schema=to_arrow_schema(schema_dict), preserve_index=False, safe=False)
    write_deltalake(path, table, mode="overwrite")
    return len(df)

def upsert_delta(path: str, rows: List[Dict[str, Any]], key: str, schema: Dict[str, str]) -> None:
    """
    Idempotent upsert by `key` using your schema dict.
    Uses Delta MERGE when available; otherwise overwrite with de-dup.
    """
    if not rows:
        return

    ensure_table(path, schema)

    incoming = pd.DataFrame(rows)
    incoming[key] = incoming[key].astype(str)
    incoming = coerce_df_to_schema(incoming, schema)

    # Try MERGE
    try:
        dt = DeltaTable(path)
        (
            dt.merge(incoming, predicate=f"s.{key} = t.{key}", source_alias="s", target_alias="t")
              .when_matched_update_all()
              .when_not_matched_insert_all()
              .execute()
        )
        return
    except Exception:
        pass

    current = _safe_read_df(path)
    if not current.empty and key in current.columns:
        current[key] = current[key].astype(str)
        current = coerce_df_to_schema(current, schema)
        combined = pd.concat([current, incoming], ignore_index=True)
        # prefer newest by ingest_ts if present, else by index order
        sort_cols = [key] + (["ingest_ts"] if "ingest_ts" in combined.columns else [])
        combined = combined.sort_values(by=sort_cols, kind="stable").drop_duplicates(subset=[key], keep="last")
    else:
        combined = incoming

    combined = coerce_df_to_schema(combined, schema)
    write_deltalake(path, pa.Table.from_pandas(combined, schema=to_arrow_schema(schema), preserve_index=False, safe=False), mode="overwrite")
