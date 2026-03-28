# nfl-pipeline

Production-grade NFL data pipeline → Supabase.  
Feeds weekly player/team stats for sportsbook modeling.

---

## Repo structure

```
nfl-pipeline/
├── config.py                        # seasons, table registry, conflict keys
├── requirements.txt
├── .env.example
│
├── db/
│   └── client.py                    # Supabase singleton
│
├── utils/
│   ├── upload.py                    # generic upsert (batching + retry)
│   └── validation.py                # per-feed QA checks
│
├── loaders/                         # one file per data feed — pure transforms
│   ├── schedule.py
│   ├── weekly_player_stats.py
│   ├── weekly_team_stats.py
│   ├── play_by_play.py
│   ├── formations.py
│   ├── snap_counts.py
│   ├── depth_chart.py
│   ├── player_info.py
│   ├── rosters.py
│   ├── nextgen.py
│   ├── pfr_adv_stats.py
│   ├── fantasy_ids.py
│   ├── fantasy_opportunities.py
│   └── fantasy_rankings.py
│
├── pipeline/
│   ├── initial_load.py              # one-time historical backfill
│   └── weekly_update.py            # Tuesday cron job
│
└── sql/
    └── schema.sql                   # CREATE TABLE + indexes (run once in Supabase)
```

---

## Setup

```bash
git clone <repo>
cd nfl-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# fill in SUPABASE_URL and SUPABASE_KEY
```

---

## Database setup (once)

Paste `sql/schema.sql` into the Supabase SQL editor and run it.  
This creates all tables and indexes.

---

## Initial backfill (once)

Pulls 2016 → present. Runtime: ~30–45 min.

```bash
# All feeds
python -m pipeline.initial_load

# Specific feeds only
python -m pipeline.initial_load --feeds schedule weekly_player_stats play_by_play
```

---

## Weekly update (every Tuesday)

```bash
# Uses CURRENT_SEASON + CURRENT_WEEK from config.py
python -m pipeline.weekly_update

# Override
python -m pipeline.weekly_update --season 2025 --week 4

# Re-run a single feed
python -m pipeline.weekly_update --feeds snap_counts pfr_adv_stats
```

### Crontab (runs 10am ET every Tuesday)
```
0 10 * * 2 cd /path/to/nfl-pipeline && /path/to/.venv/bin/python -m pipeline.weekly_update >> logs/weekly.log 2>&1
```

---

## Config

Edit `config.py` before each season/week:

```python
CURRENT_SEASON = 2025
CURRENT_WEEK   = 4      # ← update every Tuesday
```

To add a new feed:
1. Create `loaders/my_feed.py` with a `load(seasons)` function
2. Add an entry to `TABLES` in `config.py`
3. Add a `case` in `pipeline/initial_load.py` and `pipeline/weekly_update.py`
4. Add validation rules in `utils/validation.py`

---

## Table overview

| Table | Key | Strategy | Feed |
|---|---|---|---|
| `player_info` | `gsis_id` | snapshot | nfl players |
| `depth_chart` | `gsis_id` | snapshot | depth charts |
| `fantasy_football_ids` | `pfr_player_id` | snapshot | FF player IDs |
| `fantasy_football_rankings` | `mergename, pos, team, page_type` | snapshot | ECR rankings |
| `rosters` | `gsis_id, season, week` | weekly | weekly rosters |
| `sched_final` | `team, game_id` | weekly | schedules |
| `weekly_player_data` | `gsis_id, game_id` | weekly | player stats |
| `weekly_team_data` | `team, game_id` | weekly | team stats |
| `snap_count` | `pfr_player_id, game_id` | weekly | snap counts |
| `nextgen` | `gsis_id, season, week` | weekly | NGS tracking |
| `pro_football_ref_adv_stats` | `pfr_player_id, game_id` | weekly | PFR advanced |
| `fantasy_football_opportunities` | `gsis_id, game_id` | weekly | expected pts |
| `play_by_play` | `gsis_id, game_id` | weekly | PBP aggregated |
| `play_by_play_formations` | `gsis_id, game_id` | weekly | formation pcts |

---

## Key joins

```sql
-- Snap counts use pfr_player_id; bridge to gsis via fantasy_ids
SELECT s.*, i.gsis_id
FROM snap_count s
JOIN fantasy_football_ids i ON s.pfr_player_id = i.pfr_player_id;

-- Full player game profile
SELECT p.*, w.*, sc.offense_pct, f.form_shotgun_pct
FROM weekly_player_data p
JOIN sched_final sc_g   ON p.game_id = sc_g.game_id AND p.team = sc_g.team
JOIN weekly_team_data w ON p.game_id = w.game_id    AND p.team = w.team
JOIN snap_count sc      ON p.game_id = sc.game_id
JOIN fantasy_football_ids fi ON sc.pfr_player_id = fi.pfr_player_id AND fi.gsis_id = p.gsis_id
JOIN play_by_play_formations f ON p.game_id = f.game_id AND p.gsis_id = f.gsis_id;
```

---

## Modeling notes

- **Target variables**: `passing_yards`, `rushing_yards`, `receiving_yards`, `total_tds`, `receptions` — all in `weekly_player_data`
- **Key predictive features**: `opps_*_fantasy_points_exp` (opportunity model), `offense_pct` (snap share), `form_shotgun_pct`, `pfr_times_pressured_pct`, `ng_completion_percentage_above_expectation`
- **Recommended baseline**: Ridge regression or XGBoost on rolling 4-week windows; use Poisson regression for TDs (count variable)
- **Bet sizing**: Kelly Criterion with a 0.25 fractional Kelly cap until edge is validated on holdout
