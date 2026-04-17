"""
sample_schemas.py
=================
Pull a small sample from every loader, save individual CSVs, and produce
two consolidated schema files for cross-table analysis.

Usage:
    python sample_schemas.py                        # all tables, season from config.py
    python sample_schemas.py --season 2024          # override season
    python sample_schemas.py --rows 10              # rows per table (default: 25)
    python sample_schemas.py --dir ./my_samples     # output dir (default: ./schema_samples/)
    python sample_schemas.py --tables weekly_player_data snap_count

Outputs (written to --dir):
    <table_name>.csv        one per loader — raw sample rows
    schema_overview.csv     one row per (table, column) — open in a spreadsheet to
                            compare dtypes/nulls/values across all 14 tables at once
    column_overlap.csv      columns that appear in 2+ tables — surfaces join keys
                            and naming inconsistencies
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from tabulate import tabulate

from pipeline.config import CURRENT_SEASON
from inspect_tables import _run_loader, TABLE_REGISTRY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_ROWS = 25
DEFAULT_DIR  = Path(__file__).parent.resolve() / "schema_samples"


def _color(text, code): return f"\033[{code}m{text}\033[0m"
def green(t):  return _color(t, "32")
def yellow(t): return _color(t, "33")
def red(t):    return _color(t, "31")
def bold(t):   return _color(t, "1")


# ── Sampling ───────────────────────────────────────────────────────────────────

def sample_all(tables: list, seasons: list, rows: int, out_dir: Path) -> dict:
    """Run each loader, save head(rows) as CSV. Returns {name: df} for successes."""
    out_dir.mkdir(parents=True, exist_ok=True)
    loaded = {}

    for name in tables:
        cfg      = TABLE_REGISTRY[name]
        out_path = out_dir / cfg["csv"]
        logger.info(f"[{name}] loading...")
        try:
            df     = _run_loader(name, seasons)
            sample = df.head(rows)
            sample.to_csv(out_path, index=False)
            logger.info(f"[{name}] {len(df)} rows total → saved {len(sample)} to {out_path.name}")
            loaded[name] = sample
        except Exception as exc:
            logger.error(f"[{name}] FAILED: {exc}")

    return loaded


# ── Schema overview ────────────────────────────────────────────────────────────

def build_schema_overview(loaded: dict) -> pd.DataFrame:
    """One row per (table, column) with dtype, null%, n_unique, 3 sample values."""
    records = []
    for name, df in loaded.items():
        cfg          = TABLE_REGISTRY[name]
        conflict_set = set(cfg["keys"])
        supabase_tbl = name  # logical name == TABLE_REGISTRY key; actual table in pipeline/config.py

        for col in df.columns:
            series    = df[col]
            null_pct  = round(series.isnull().mean() * 100, 1)
            n_unique  = int(series.nunique(dropna=True))
            uniq_vals = series.dropna().unique().tolist()
            samples   = [str(v)[:50] for v in uniq_vals[:3]]
            while len(samples) < 3:
                samples.append("")

            records.append({
                "table":          name,
                "supabase_table": supabase_tbl,
                "column":         col,
                "dtype":          str(series.dtype),
                "is_conflict_key": col in conflict_set,
                "null_pct":       null_pct,
                "n_unique":       n_unique,
                "sample_val_1":   samples[0],
                "sample_val_2":   samples[1],
                "sample_val_3":   samples[2],
            })

    return pd.DataFrame(records)


# ── Column overlap ─────────────────────────────────────────────────────────────

def build_column_overlap(loaded: dict) -> pd.DataFrame:
    """Columns appearing in 2+ tables — identifies join keys and redundancies."""
    col_map: dict[str, list] = {}
    for name, df in loaded.items():
        for col in df.columns:
            col_map.setdefault(col, []).append((name, str(df[col].dtype)))

    records = []
    for col, entries in col_map.items():
        if len(entries) < 2:
            continue
        records.append({
            "column":   col,
            "n_tables": len(entries),
            "tables":   ", ".join(t for t, _ in entries),
            "dtypes":   ", ".join(sorted(set(d for _, d in entries))),
        })

    if not records:
        return pd.DataFrame(columns=["column", "n_tables", "tables", "dtypes"])

    return (pd.DataFrame(records)
            .sort_values("n_tables", ascending=False)
            .reset_index(drop=True))


# ── Terminal summary ───────────────────────────────────────────────────────────

def print_summary(tables: list, loaded: dict, out_dir: Path) -> None:
    all_tables = set(tables)
    failed     = sorted(all_tables - set(loaded.keys()))

    print(f"\n{'=' * 70}")
    print(bold("  SCHEMA SAMPLE SUMMARY"))
    print(f"{'=' * 70}")

    rows = []
    for name in tables:
        if name in loaded:
            df   = loaded[name]
            keys = TABLE_REGISTRY[name]["keys"]
            rows.append([green(name), len(df), len(df.columns), ", ".join(keys), green("OK")])
        else:
            rows.append([red(name), "—", "—", "—", red("FAILED")])

    print(tabulate(rows,
                   headers=["Table", "Rows", "Cols", "Conflict Keys", "Status"],
                   tablefmt="simple",
                   colalign=("left", "right", "right", "left", "left")))

    print(f"\n  Output directory: {out_dir}")
    print(f"  {bold('schema_overview.csv')}  — one row per (table, column), sort/filter in a spreadsheet")
    print(f"  {bold('column_overlap.csv')} — columns shared across 2+ tables\n")

    if failed:
        print(yellow(f"  Loaders that failed: {failed}\n"))


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sample each NFL loader and produce schema alignment CSVs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sample_schemas.py
  python sample_schemas.py --season 2024 --rows 10
  python sample_schemas.py --tables weekly_player_data snap_count weekly_team_data
        """,
    )
    parser.add_argument("--season", type=int, default=CURRENT_SEASON,
                        help=f"Season for seasonal loaders (default: {CURRENT_SEASON})")
    parser.add_argument("--rows",   type=int, default=DEFAULT_ROWS,
                        help=f"Rows to sample per table (default: {DEFAULT_ROWS})")
    parser.add_argument("--dir",    type=str, default=None,
                        help="Output directory (default: ./schema_samples/)")
    parser.add_argument("--tables", nargs="*", default=None,
                        help="Subset of tables to process (default: all)")
    args = parser.parse_args()

    out_dir        = Path(args.dir).expanduser().resolve() if args.dir else DEFAULT_DIR
    tables_to_run  = args.tables if args.tables else list(TABLE_REGISTRY.keys())

    invalid = [t for t in tables_to_run if t not in TABLE_REGISTRY]
    if invalid:
        print(red(f"Unknown tables: {invalid}"))
        print(f"Valid: {list(TABLE_REGISTRY.keys())}")
        sys.exit(1)

    print(bold(f"\n  NFL Schema Sampler"))
    print(f"  Season : {args.season}")
    print(f"  Rows   : {args.rows} per table")
    print(f"  Tables : {len(tables_to_run)}")
    print(f"  Output : {out_dir}\n")

    loaded = sample_all(tables_to_run, [args.season], args.rows, out_dir)

    if not loaded:
        print(red("\n  All loaders failed — no schema files written."))
        sys.exit(1)

    overview = build_schema_overview(loaded)
    overview.to_csv(out_dir / "schema_overview.csv", index=False)
    logger.info(f"schema_overview.csv written ({len(overview)} rows)")

    overlap = build_column_overlap(loaded)
    overlap.to_csv(out_dir / "column_overlap.csv", index=False)
    logger.info(f"column_overlap.csv written ({len(overlap)} shared columns)")

    print_summary(tables_to_run, loaded, out_dir)


if __name__ == "__main__":
    main()
