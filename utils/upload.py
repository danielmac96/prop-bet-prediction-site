"""
utils/upload.py
===============
Generic upsert utility for Supabase.
Handles NaN → None conversion, batching, and retries.
"""

import time
import logging
import pandas as pd
from typing import List

from db.client import supabase
from config import BATCH_SIZE, MAX_RETRIES

logger = logging.getLogger(__name__)


def _to_records(df: pd.DataFrame) -> List[dict]:
    """Convert DataFrame to list of dicts, replacing NaN with None."""
    return df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")


def upsert(df: pd.DataFrame, table: str, conflict_cols: List[str]) -> None:
    """
    Upsert a DataFrame into a Supabase table in batches.

    Args:
        df:             Cleaned DataFrame ready for upload.
        table:          Supabase table name.
        conflict_cols:  Columns that define a unique row (upsert key).
    """
    if df.empty:
        logger.warning(f"[{table}] Empty DataFrame — skipping upload.")
        return

    conflict_str = ",".join(conflict_cols)
    records = _to_records(df)
    total = len(records)

    logger.info(f"[{table}] Upserting {total} rows (conflict: {conflict_str})")

    for i in range(0, total, BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        _upsert_batch_with_retry(batch, table, conflict_str)
        logger.info(f"[{table}] ✓ {min(i + BATCH_SIZE, total)}/{total} rows")

    logger.info(f"[{table}] Done — {total} rows upserted.")


def _upsert_batch_with_retry(batch: List[dict], table: str, conflict_str: str) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            supabase.table(table).upsert(batch, on_conflict=conflict_str).execute()
            return
        except Exception as e:
            logger.warning(f"[{table}] Attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"[{table}] Upload failed after {MAX_RETRIES} attempts."
                ) from e
            time.sleep(2 ** attempt)  # exponential back-off: 2s, 4s, 8s
