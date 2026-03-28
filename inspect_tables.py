"""
inspect_tables.py
==================
Generate live samples from every loader, save as local CSVs, then inspect.

Usage:
    # Generate fresh CSVs from loaders, then inspect all tables
    python inspect_tables.py --generate --season 2025

    # Generate specific tables only
    python inspect_tables.py --generate --season 2025 --tables weekly_player_data snap_count

    # Inspect previously generated CSVs without re-fetching
    python inspect_tables.py

    # Control output location (default: ./samples/)
    python inspect_tables.py --generate --season 2025 --dir ./my_samples

    # More sample rows in the report (default: 5)
    python inspect_tables.py --rows 10

    # Export inspection summary to CSV after run
    python inspect_tables.py --export

Workflow:
    --generate  calls each loader, saves first 1000 rows to --dir as CSVs
    (no flag)   reads whatever CSVs are already in --dir and inspects them
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from tabulate import tabulate

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Default output dir for generated samples ───────────────────────────────────
SAMPLE_DIR = Path(__file__).parent.resolve() / "samples"

# ── Table registry ─────────────────────────────────────────────────────────────
# Maps logical name -> (csv filename, conflict keys, loader_type)
# loader_type: "seasonal" = needs seasons arg | "snapshot" = no seasons arg
TABLE_REGISTRY = {
    "player_info": {
        "csv":      "player_info.csv",
        "keys":     ["gsis_id"],
        "type":     "snapshot",
    },
    "depth_chart": {
        "csv":      "depth_chart.csv",
        "keys":     ["gsis_id"],
        "type":     "snapshot",
    },
    "fantasy_football_ids": {
        "csv":      "fantasy_football_ids.csv",
        "keys":     ["pfr_player_id"],
        "type":     "snapshot",
    },
    "fantasy_football_rankings": {
        "csv":      "fantasy_football_rankings.csv",
        "keys":     ["mergename", "pos", "team", "page_type"],
        "type":     "snapshot",
    },
    "nextgen": {
        "csv":      "nextgen.csv",
        "keys":     ["gsis_id", "season", "week"],
        "type":     "snapshot",
    },
    "pro_football_ref_adv_stats": {
        "csv":      "pro_football_ref_adv_stats.csv",
        "keys":     ["pfr_player_id", "game_id"],
        "type":     "snapshot",
    },
    "rosters": {
        "csv":      "rosters.csv",
        "keys":     ["gsis_id", "season", "week"],
        "type":     "seasonal",
    },
    "sched_final": {
        "csv":      "sched_final.csv",
        "keys":     ["team", "game_id"],
        "type":     "seasonal",
    },
    "weekly_player_data": {
        "csv":      "weekly_player_data.csv",
        "keys":     ["gsis_id", "game_id"],
        "type":     "seasonal",
    },
    "weekly_team_data": {
        "csv":      "weekly_team_data.csv",
        "keys":     ["team", "game_id"],
        "type":     "seasonal",
    },
    "snap_count": {
        "csv":      "snap_count.csv",
        "keys":     ["pfr_player_id", "game_id"],
        "type":     "seasonal",
    },
    "fantasy_football_opportunities": {
        "csv":      "fantasy_football_opportunities.csv",
        "keys":     ["gsis_id", "game_id"],
        "type":     "seasonal",
    },
    "play_by_play": {
        "csv":      "play_by_play.csv",
        "keys":     ["gsis_id", "game_id"],
        "type":     "seasonal",
    },
    "play_by_play_formations": {
        "csv":      "play_by_play_formations.csv",
        "keys":     ["gsis_id", "game_id"],
        "type":     "seasonal",
    },
}

SAMPLE_ROWS = 1000  # rows written to each CSV


def _color(text, code): return f"\033[{code}m{text}\033[0m"
def green(t):  return _color(t, "32")
def yellow(t): return _color(t, "33")
def red(t):    return _color(t, "31")
def bold(t):   return _color(t, "1")
def cyan(t):   return _color(t, "36")

SEP = "=" * 90


# ── Loader dispatch ────────────────────────────────────────────────────────────

def _run_loader(name: str, seasons: list) -> pd.DataFrame:
    """
    Call the correct loader function for a given table name.
    Imports are local so missing nflreadpy only errors on --generate, not inspect.
    """
    import loaders.schedule               as schedule_loader
    import loaders.weekly_player_stats    as weekly_player_loader
    import loaders.weekly_team_stats      as weekly_team_loader
    import loaders.play_by_play           as pbp_loader
    import loaders.formations             as formations_loader
    import loaders.snap_counts            as snap_loader
    import loaders.depth_chart            as depth_chart_loader
    import loaders.player_info            as player_info_loader
    import loaders.rosters                as rosters_loader
    import loaders.nextgen                as nextgen_loader
    import loaders.pfr_adv_stats          as pfr_loader
    import loaders.fantasy_ids            as fantasy_ids_loader
    import loaders.fantasy_opportunities  as opps_loader
    import loaders.fantasy_rankings       as rankings_loader

    if   name == "sched_final":                    return schedule_loader.load(seasons)
    elif name == "weekly_player_data":             return weekly_player_loader.load(seasons)
    elif name == "weekly_team_data":               return weekly_team_loader.load(seasons)
    elif name == "play_by_play":                   return pbp_loader.load(seasons)
    elif name == "play_by_play_formations":        return formations_loader.load(seasons)
    elif name == "snap_count":                     return snap_loader.load(seasons)
    elif name == "rosters":                        return rosters_loader.load(seasons)
    elif name == "fantasy_football_opportunities": return opps_loader.load(seasons)
    elif name == "player_info":                    return player_info_loader.load()
    elif name == "depth_chart":                    return depth_chart_loader.load()
    elif name == "nextgen":                        return nextgen_loader.load()
    elif name == "pro_football_ref_adv_stats":     return pfr_loader.load()
    elif name == "fantasy_football_ids":           return fantasy_ids_loader.load()
    elif name == "fantasy_football_rankings":      return rankings_loader.load()
    else:
        raise ValueError(f"No loader mapped for '{name}'")


def generate_samples(tables: list, seasons: list, sample_dir: Path) -> None:
    """Run each loader, save first SAMPLE_ROWS rows as a CSV."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    failed = []

    for name in tables:
        cfg      = TABLE_REGISTRY[name]
        out_path = sample_dir / cfg["csv"]
        loader_t = cfg["type"]

        logger.info(f"[{name}] Loading from nflreadpy...")
        try:
            df = _run_loader(name, seasons)
            sample = df.head(SAMPLE_ROWS)
            sample.to_csv(out_path, index=False)
            logger.info(f"[{name}] Saved {len(sample)} rows -> {out_path}")
        except Exception as e:
            logger.error(f"[{name}] FAILED: {e}")
            failed.append(name)

    if failed:
        print(red(f"\n  Generation failed for: {failed}"))
    else:
        print(green(f"\n  All samples saved to: {sample_dir}"))


# ── Inspection ─────────────────────────────────────────────────────────────────

def _null_pct(series):
    pct = series.isnull().mean() * 100
    s   = f"{pct:.1f}%"
    if pct == 0:    return green(s)
    elif pct < 10:  return yellow(s)
    else:           return red(s)


def _dtype_label(dtype):
    d = str(dtype)
    if "int"   in d: return cyan("int")
    if "float" in d: return cyan("float")
    if "obj"   in d: return "text"
    return d


def inspect_table(name: str, df: pd.DataFrame, n_rows: int) -> dict:
    conflict_cols     = TABLE_REGISTRY[name]["keys"]
    dupes             = df.duplicated(subset=conflict_cols).sum() if conflict_cols else 0
    null_cols_over_50 = [c for c in df.columns if df[c].isnull().mean() > 0.5]

    print(f"\n{SEP}")
    print(bold(f"  TABLE: {name.upper()}"))
    print(SEP)

    status = green("OK") if dupes == 0 else red(f"{dupes} DUPES FOUND")
    print(f"  Rows: {bold(str(len(df)))}   Cols: {bold(str(len(df.columns)))}   "
          f"Conflict key: {cyan(str(conflict_cols))}   Dupes: {status}")

    if null_cols_over_50:
        print(f"  {yellow('Columns >50% null:')} {null_cols_over_50}")

    # Column overview
    print(f"\n  {bold('COLUMN OVERVIEW')}")
    col_rows = []
    for col in df.columns:
        series   = df[col]
        n_unique = series.nunique()
        if pd.api.types.is_numeric_dtype(series):
            rng = f"{series.min()} -> {series.max()}"
        else:
            top = series.dropna().value_counts().index
            rng = str(top[0])[:40] if len(top) else "—"
        col_rows.append([
            "*" if col in conflict_cols else "",
            col,
            _dtype_label(series.dtype),
            _null_pct(series),
            n_unique,
            rng,
        ])
    print(tabulate(col_rows,
                   headers=["Key", "Column", "Dtype", "Null%", "Unique", "Range / Top Value"],
                   tablefmt="simple",
                   colalign=("center", "left", "left", "right", "right", "left")))

    # Sample rows
    print(f"\n  {bold(f'SAMPLE ({n_rows} rows)')}")
    sample = df.head(n_rows).copy()
    for col in sample.select_dtypes(include=["object", "string"]).columns:
        sample[col] = sample[col].astype(str).str[:28]
    print(tabulate(sample, headers="keys", tablefmt="simple",
                   showindex=False, floatfmt=".3f"))

    # Numeric summary
    num_cols = df.select_dtypes("number").columns.tolist()
    if num_cols:
        print(f"\n  {bold('NUMERIC SUMMARY')}")
        desc = df[num_cols].describe().T[["mean", "std", "min", "50%", "max"]]
        desc.columns = ["mean", "std", "min", "median", "max"]
        print(tabulate(desc.round(3), headers="keys", tablefmt="simple",
                       colalign=("left","right","right","right","right","right")))
    print()

    return {
        "table":           name,
        "rows":            len(df),
        "cols":            len(df.columns),
        "dupes":           int(dupes),
        "null_heavy_cols": len(null_cols_over_50),
        "status":          "DUPE_ERROR" if dupes > 0 else "OK",
    }


def print_summary(results: list) -> None:
    print(f"\n{SEP}")
    print(bold("  PIPELINE SUMMARY"))
    print(SEP)
    rows = []
    for r in results:
        status = green("OK") if r["status"] == "OK" else red("DUPES")
        rows.append([r["table"], r["rows"], r["cols"],
                     r["dupes"], r["null_heavy_cols"], status])
    print(tabulate(rows,
                   headers=["Table", "Rows", "Cols", "Dupes", ">50% Null Cols", "Status"],
                   tablefmt="simple",
                   colalign=("left","right","right","right","right","left")))
    print()


def export_summary(results: list, out_path: Path) -> None:
    pd.DataFrame(results).to_csv(out_path, index=False)
    print(green(f"  Summary exported -> {out_path}"))


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate loader samples and/or inspect NFL pipeline tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python inspect_tables.py --generate --season 2025
  python inspect_tables.py --generate --season 2025 --tables weekly_player_data snap_count
  python inspect_tables.py --rows 10
  python inspect_tables.py --export
        """,
    )
    parser.add_argument(
        "--generate", action="store_true",
        help="Run loaders and save fresh CSVs before inspecting.",
    )
    parser.add_argument(
        "--season", type=int, default=2025,
        help="Season to pull for seasonal loaders (default: 2025).",
    )
    parser.add_argument(
        "--tables", nargs="*", default=None,
        help="Subset of tables. Default: all. Example: --tables weekly_player_data snap_count",
    )
    parser.add_argument(
        "--dir", type=str, default=None,
        help=f"Folder for CSV samples. Default: ./samples/",
    )
    parser.add_argument("--rows",   type=int, default=5,   help="Sample rows to display (default: 5)")
    parser.add_argument("--export", action="store_true",   help="Export inspection summary to CSV")
    args = parser.parse_args()

    sample_dir    = Path(args.dir).expanduser().resolve() if args.dir else SAMPLE_DIR
    tables_to_run = args.tables if args.tables else list(TABLE_REGISTRY.keys())

    invalid = [t for t in tables_to_run if t not in TABLE_REGISTRY]
    if invalid:
        print(red(f"Unknown tables: {invalid}"))
        print(f"Valid: {list(TABLE_REGISTRY.keys())}")
        sys.exit(1)

    print(bold("\n  NFL Pipeline Table Inspector"))
    print(f"  Sample dir : {sample_dir}")
    print(f"  Tables     : {tables_to_run}")
    print(f"  Sample rows: {args.rows}")
    if args.generate:
        print(f"  Season     : {args.season}  (seasonal loaders)")

    # ── Step 1: generate ──────────────────────────────────────────────────────
    if args.generate:
        print(bold("\n  Generating samples from loaders..."))
        generate_samples(tables_to_run, [args.season], sample_dir)

    # ── Step 2: inspect ───────────────────────────────────────────────────────
    if not sample_dir.exists():
        print(red(f"\n  No samples directory found: {sample_dir}"))
        print(f"  Run with --generate first to create samples.")
        sys.exit(1)

    results   = []
    not_found = []

    for name in tables_to_run:
        csv_path = sample_dir / TABLE_REGISTRY[name]["csv"]
        if not csv_path.exists():
            print(yellow(f"  [{name}] CSV not found: {csv_path} — skipping. Run with --generate to create it."))
            not_found.append(name)
            continue
        df = pd.read_csv(csv_path, low_memory=False)
        results.append(inspect_table(name, df, n_rows=args.rows))

    if not_found:
        print(yellow(f"\n  {len(not_found)} table(s) skipped. Re-run with --generate to fetch them."))

    if results:
        print_summary(results)
        if args.export:
            export_summary(results, sample_dir / "inspection_summary.csv")


if __name__ == "__main__":
    main()