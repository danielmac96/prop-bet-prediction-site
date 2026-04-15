-- =============================================================================
-- sql/schema.sql
-- Supabase table definitions for the NFL analytics pipeline.
-- Run once in the Supabase SQL editor before the initial_load.
--
-- Conventions:
--   - All text IDs (gsis_id, game_id) are TEXT — never cast to int
--   - Floats use DOUBLE PRECISION (matches pandas float64)
--   - Each table has a composite UNIQUE constraint matching config.TABLES conflict cols
--   - No foreign key constraints — enforce integrity in the pipeline layer
--   - Player metadata (name, position) lives only in player_info; join on gsis_id
-- =============================================================================


-- ── Static / snapshot tables ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.player_info (
    gsis_id                 TEXT PRIMARY KEY,
    pi_display_name         TEXT,
    pi_position_group       TEXT,
    ngs_position_group      TEXT,
    ngs_position            TEXT,
    height                  DOUBLE PRECISION,
    weight                  DOUBLE PRECISION,
    college_name            TEXT,
    college_conference      TEXT,
    rookie_season           BIGINT,
    draft_round             DOUBLE PRECISION,
    draft_pick              DOUBLE PRECISION,
    years_of_experience     BIGINT,
    pff_position            TEXT,
    pfr_id                  TEXT
);

CREATE TABLE IF NOT EXISTS public.depth_chart (
    gsis_id         TEXT PRIMARY KEY,
    dc_team         TEXT,
    dc_pos_abb      TEXT,
    dc_pos_slot     BIGINT,
    dc_pos_rank     BIGINT,
    dc_pos_name     TEXT,
    dc_pos_grp      TEXT
);

CREATE TABLE IF NOT EXISTS public.fantasy_football_ids (
    pfr_player_id   TEXT PRIMARY KEY,
    gsis_id         TEXT,
    sportradar_id   TEXT,
    espn_id         TEXT,
    sleeper_id      TEXT,
    yahoo_id        TEXT,
    mfl_id          TEXT
);


-- ── Weekly / seasonal tables ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.sched_final (
    game_id             TEXT,
    team                TEXT,
    opponent_team       TEXT,
    team_score          BIGINT,
    opp_score           BIGINT,
    rest_days           BIGINT,
    team_coach          TEXT,
    spread_from_team    DOUBLE PRECISION,
    team_moneyline      BIGINT,
    is_home             BIGINT,
    pfr                 TEXT,
    espn                BIGINT,
    old_game_id         BIGINT,
    season              BIGINT,
    game_type           TEXT,
    week                BIGINT,
    gameday             DATE,
    gametime            TEXT,
    location            TEXT,
    result              BIGINT,
    total               BIGINT,
    spread_line         DOUBLE PRECISION,
    total_line          DOUBLE PRECISION,
    away_moneyline      BIGINT,
    home_moneyline      BIGINT,
    div_game            BIGINT,
    roof                TEXT,
    surface             TEXT,
    temp                DOUBLE PRECISION,
    wind                DOUBLE PRECISION,
    referee             TEXT,
    stadium_id          TEXT,
    stadium             TEXT,
    UNIQUE (team, game_id)
);

-- Weekly roster snapshot. Physical attrs (height/weight) kept per-season as they
-- change year over year. Static bio data lives in player_info.
CREATE TABLE IF NOT EXISTS public.rosters (
    season              BIGINT,
    week                BIGINT,
    game_type           TEXT,
    gsis_id             TEXT,
    team                TEXT,
    ros_position        TEXT,
    ros_depth_chart_pos TEXT,
    ros_status          TEXT,
    ros_status_desc     TEXT,
    ros_height          DOUBLE PRECISION,
    ros_weight          DOUBLE PRECISION,
    UNIQUE (gsis_id, season, week)
);

-- Fact table: one row per player per game. No static player metadata —
-- join to player_info on gsis_id for name / position / headshot.
CREATE TABLE IF NOT EXISTS public.weekly_player_data (
    gsis_id                     TEXT,
    season                      BIGINT,
    week                        BIGINT,
    season_type                 TEXT,
    game_id                     TEXT,
    team                        TEXT,
    opponent_team               TEXT,
    completions                 BIGINT,
    attempts                    BIGINT,
    passing_yards               BIGINT,
    passing_tds                 BIGINT,
    passing_interceptions       BIGINT,
    sacks_suffered              BIGINT,
    passing_air_yards           BIGINT,
    passing_yards_after_catch   BIGINT,
    passing_first_downs         BIGINT,
    passing_epa                 DOUBLE PRECISION,
    passing_cpoe                DOUBLE PRECISION,
    pacr                        DOUBLE PRECISION,
    carries                     BIGINT,
    rushing_yards               BIGINT,
    rushing_tds                 BIGINT,
    rushing_fumbles             BIGINT,
    rushing_fumbles_lost        BIGINT,
    rushing_first_downs         BIGINT,
    rushing_epa                 DOUBLE PRECISION,
    receptions                  BIGINT,
    targets                     BIGINT,
    receiving_yards             BIGINT,
    receiving_tds               BIGINT,
    receiving_fumbles           BIGINT,
    receiving_fumbles_lost      BIGINT,
    receiving_air_yards         BIGINT,
    receiving_yards_after_catch BIGINT,
    receiving_first_downs       BIGINT,
    receiving_epa               DOUBLE PRECISION,
    racr                        DOUBLE PRECISION,
    target_share                DOUBLE PRECISION,
    air_yards_share             DOUBLE PRECISION,
    wopr                        DOUBLE PRECISION,
    def_tackles_solo            BIGINT,
    def_tackles_with_assist     BIGINT,
    def_tackle_assists          BIGINT,
    def_tackles_for_loss        BIGINT,
    def_sacks                   DOUBLE PRECISION,
    def_qb_hits                 BIGINT,
    def_interceptions           BIGINT,
    def_pass_defended           BIGINT,
    total_tds                   BIGINT,
    UNIQUE (gsis_id, game_id)
);

CREATE TABLE IF NOT EXISTS public.weekly_team_data (
    season                      BIGINT,
    week                        BIGINT,
    team                        TEXT,
    season_type                 TEXT,
    game_id                     TEXT,
    opponent_team               TEXT,
    completions                 BIGINT,
    attempts                    BIGINT,
    passing_yards               BIGINT,
    passing_tds                 BIGINT,
    passing_interceptions       BIGINT,
    sacks_suffered              BIGINT,
    sack_yards_lost             BIGINT,
    sack_fumbles                BIGINT,
    sack_fumbles_lost           BIGINT,
    passing_air_yards           BIGINT,
    passing_yards_after_catch   BIGINT,
    passing_first_downs         BIGINT,
    passing_epa                 DOUBLE PRECISION,
    passing_cpoe                DOUBLE PRECISION,
    passing_2pt_conversions     BIGINT,
    carries                     BIGINT,
    rushing_yards               BIGINT,
    rushing_tds                 BIGINT,
    rushing_fumbles             BIGINT,
    rushing_fumbles_lost        BIGINT,
    rushing_first_downs         BIGINT,
    rushing_epa                 DOUBLE PRECISION,
    rushing_2pt_conversions     BIGINT,
    receptions                  BIGINT,
    targets                     BIGINT,
    receiving_yards             BIGINT,
    receiving_tds               BIGINT,
    receiving_fumbles           BIGINT,
    receiving_fumbles_lost      BIGINT,
    receiving_air_yards         BIGINT,
    receiving_yards_after_catch BIGINT,
    receiving_first_downs       BIGINT,
    receiving_epa               DOUBLE PRECISION,
    receiving_2pt_conversions   BIGINT,
    special_teams_tds           BIGINT,
    def_tackles_solo            BIGINT,
    def_tackles_with_assist     BIGINT,
    def_tackle_assists          BIGINT,
    def_tackles_for_loss        BIGINT,
    def_tackles_for_loss_yards  BIGINT,
    def_fumbles_forced          BIGINT,
    def_sacks                   DOUBLE PRECISION,
    def_sack_yards              DOUBLE PRECISION,
    def_qb_hits                 BIGINT,
    def_interceptions           BIGINT,
    def_interception_yards      BIGINT,
    def_pass_defended           BIGINT,
    def_tds                     BIGINT,
    def_fumbles                 BIGINT,
    def_safeties                BIGINT,
    misc_yards                  BIGINT,
    fumble_recovery_own         BIGINT,
    fumble_recovery_yards_own   BIGINT,
    fumble_recovery_opp         BIGINT,
    fumble_recovery_yards_opp   BIGINT,
    fumble_recovery_tds         BIGINT,
    penalties                   BIGINT,
    penalty_yards               BIGINT,
    timeouts                    BIGINT,
    punt_returns                BIGINT,
    punt_return_yards           BIGINT,
    kickoff_returns             BIGINT,
    kickoff_return_yards        BIGINT,
    fg_made                     BIGINT,
    fg_att                      BIGINT,
    fg_missed                   BIGINT,
    fg_blocked                  BIGINT,
    fg_long                     DOUBLE PRECISION,
    fg_pct                      DOUBLE PRECISION,
    pat_made                    BIGINT,
    pat_att                     BIGINT,
    pat_missed                  BIGINT,
    pat_blocked                 BIGINT,
    pat_pct                     DOUBLE PRECISION,
    gwfg_made                   BIGINT,
    gwfg_att                    BIGINT,
    gwfg_missed                 BIGINT,
    gwfg_blocked                BIGINT,
    gwfg_distance               BIGINT,
    UNIQUE (team, game_id)
);

CREATE TABLE IF NOT EXISTS public.snap_count (
    game_id             TEXT,
    season              BIGINT,
    week                BIGINT,
    game_type           TEXT,
    pfr_player_id       TEXT,
    snap_player_name    TEXT,
    snap_position       TEXT,
    team                TEXT,
    offense_snaps       DOUBLE PRECISION,
    offense_pct         DOUBLE PRECISION,
    defense_snaps       DOUBLE PRECISION,
    defense_pct         DOUBLE PRECISION,
    st_snaps            DOUBLE PRECISION,
    st_pct              DOUBLE PRECISION,
    UNIQUE (pfr_player_id, game_id)
);

CREATE TABLE IF NOT EXISTS public.play_by_play (
    game_id                 TEXT,
    gsis_id                 TEXT,
    team                    TEXT,
    pbp_dropbacks           DOUBLE PRECISION,
    pbp_scrambles           DOUBLE PRECISION,
    pbp_pass_attempts       DOUBLE PRECISION,
    pbp_completions         DOUBLE PRECISION,
    pbp_incompletions       DOUBLE PRECISION,
    pbp_sacks               DOUBLE PRECISION,
    pbp_pass_yards          DOUBLE PRECISION,
    pbp_air_yards_thrown    DOUBLE PRECISION,
    pbp_yac                 DOUBLE PRECISION,
    pbp_pass_tds            DOUBLE PRECISION,
    pbp_ints                DOUBLE PRECISION,
    pbp_pass_epa_total      DOUBLE PRECISION,
    pbp_qb_epa_total        DOUBLE PRECISION,
    pbp_air_epa             DOUBLE PRECISION,
    pbp_yac_epa             DOUBLE PRECISION,
    pbp_cpoe_mean           DOUBLE PRECISION,
    pbp_cp_mean             DOUBLE PRECISION,
    pbp_xyac_epa            DOUBLE PRECISION,
    pbp_xpass_mean          DOUBLE PRECISION,
    pbp_pass_oe_mean        DOUBLE PRECISION,
    pbp_first_down_pass     DOUBLE PRECISION,
    pbp_rz_dropbacks        DOUBLE PRECISION,
    pbp_nohuddle_snaps      DOUBLE PRECISION,
    pbp_shotgun_snaps       DOUBLE PRECISION,
    pbp_carries             DOUBLE PRECISION,
    pbp_rush_yards          DOUBLE PRECISION,
    pbp_rush_tds            DOUBLE PRECISION,
    pbp_rush_epa_total      DOUBLE PRECISION,
    pbp_xyac_rush_epa       DOUBLE PRECISION,
    pbp_rush_first_downs    DOUBLE PRECISION,
    pbp_tfl                 DOUBLE PRECISION,
    pbp_rz_carries          DOUBLE PRECISION,
    pbp_goal_line_carries   DOUBLE PRECISION,
    pbp_targets             DOUBLE PRECISION,
    pbp_receptions          DOUBLE PRECISION,
    pbp_rec_yards           DOUBLE PRECISION,
    pbp_rec_tds             DOUBLE PRECISION,
    pbp_rec_air_yards       DOUBLE PRECISION,
    pbp_rec_yac             DOUBLE PRECISION,
    pbp_rec_epa_total       DOUBLE PRECISION,
    pbp_rec_air_epa         DOUBLE PRECISION,
    pbp_rec_yac_epa         DOUBLE PRECISION,
    pbp_rec_first_downs     DOUBLE PRECISION,
    pbp_rz_targets          DOUBLE PRECISION,
    pbp_deep_targets        DOUBLE PRECISION,
    pbp_xyac_rec_mean       DOUBLE PRECISION,
    UNIQUE (gsis_id, game_id)
);

CREATE TABLE IF NOT EXISTS public.play_by_play_formations (
    game_id                 TEXT,
    gsis_id                 TEXT,
    team                    TEXT,
    form_off_snaps          BIGINT,
    form_shotgun_pct        DOUBLE PRECISION,
    form_under_center_pct   DOUBLE PRECISION,
    form_empty_back_pct     DOUBLE PRECISION,
    form_pressure_rate      DOUBLE PRECISION,
    form_avg_time_to_throw  DOUBLE PRECISION,
    form_avg_defenders_box  DOUBLE PRECISION,
    UNIQUE (gsis_id, game_id)
);

-- Next Gen Stats (AWS tracking). One row per player per week.
-- Wide join across passing / rushing / receiving — most cells NULL per player.
-- No player name/position — join to player_info on gsis_id.
CREATE TABLE IF NOT EXISTS public.nextgen (
    gsis_id                                         TEXT,
    season                                          BIGINT,
    week                                            BIGINT,
    season_type                                     TEXT,
    team                                            TEXT,
    -- passing metrics (QBs)
    ng_pass_avg_time_to_throw                       DOUBLE PRECISION,
    ng_pass_avg_completed_air_yards                 DOUBLE PRECISION,
    ng_pass_avg_intended_air_yards                  DOUBLE PRECISION,
    ng_pass_avg_air_yards_differential              DOUBLE PRECISION,
    ng_pass_aggressiveness                          DOUBLE PRECISION,
    ng_pass_max_completed_air_distance              DOUBLE PRECISION,
    ng_pass_avg_air_yards_to_sticks                 DOUBLE PRECISION,
    ng_pass_completion_percentage                   DOUBLE PRECISION,
    ng_pass_expected_completion_percentage          DOUBLE PRECISION,
    ng_pass_completion_percentage_above_expectation DOUBLE PRECISION,
    ng_pass_avg_air_distance                        DOUBLE PRECISION,
    ng_pass_max_air_distance                        DOUBLE PRECISION,
    ng_pass_passer_rating                           DOUBLE PRECISION,
    ng_pass_attempts                                BIGINT,
    ng_pass_pass_yards                              BIGINT,
    ng_pass_pass_touchdowns                         BIGINT,
    ng_pass_interceptions                           BIGINT,
    -- rushing metrics (all ball carriers)
    ng_rush_efficiency                              DOUBLE PRECISION,
    ng_rush_percent_attempts_gte_eight_defenders    DOUBLE PRECISION,
    ng_rush_avg_time_to_los                         DOUBLE PRECISION,
    ng_rush_rush_attempts                           BIGINT,
    ng_rush_rush_yards                              BIGINT,
    ng_rush_avg_rush_yards                          DOUBLE PRECISION,
    ng_rush_rush_touchdowns                         BIGINT,
    ng_rush_expected_rush_yards                     DOUBLE PRECISION,
    ng_rush_rush_yards_over_expected                DOUBLE PRECISION,
    ng_rush_rush_yards_over_expected_per_att        DOUBLE PRECISION,
    ng_rush_rush_pct_over_expected                  DOUBLE PRECISION,
    -- receiving metrics (all targets)
    ng_rec_avg_cushion                              DOUBLE PRECISION,
    ng_rec_avg_separation                           DOUBLE PRECISION,
    ng_rec_avg_intended_air_yards                   DOUBLE PRECISION,
    ng_rec_percent_share_of_intended_air_yards      DOUBLE PRECISION,
    ng_rec_receptions                               BIGINT,
    ng_rec_targets                                  BIGINT,
    ng_rec_catch_percentage                         DOUBLE PRECISION,
    ng_rec_rec_yards                                BIGINT,
    ng_rec_rec_touchdowns                           BIGINT,
    ng_rec_avg_yac                                  DOUBLE PRECISION,
    ng_rec_avg_expected_yac                         DOUBLE PRECISION,
    ng_rec_avg_yac_above_expectation                DOUBLE PRECISION,
    UNIQUE (gsis_id, season, week)
);

-- PFR advanced stats. One row per pfr_player_id per game.
-- Wide join across four role-specific frames — most cells NULL per player.
-- Join to fantasy_football_ids on pfr_player_id to get gsis_id.
CREATE TABLE IF NOT EXISTS public.pro_football_ref_adv_stats (
    game_id                         TEXT,
    pfr_game_id                     TEXT,
    season                          BIGINT,
    week                            BIGINT,
    game_type                       TEXT,
    team                            TEXT,
    opponent                        TEXT,
    pfr_player_id                   TEXT,
    pfr_player_name                 TEXT,
    -- QB accuracy / decision-making (pfr_pass frame)
    pfr_pass_drops                  DOUBLE PRECISION,
    pfr_pass_drop_pct               DOUBLE PRECISION,
    pfr_pass_bad_throws             DOUBLE PRECISION,
    pfr_pass_bad_throw_pct          DOUBLE PRECISION,
    -- pressure absorbed by QB
    pfr_pass_times_sacked           DOUBLE PRECISION,
    pfr_pass_times_blitzed          DOUBLE PRECISION,
    pfr_pass_times_hurried          DOUBLE PRECISION,
    pfr_pass_times_hit              DOUBLE PRECISION,
    pfr_pass_times_pressured        DOUBLE PRECISION,
    pfr_pass_times_pressured_pct    DOUBLE PRECISION,
    -- defensive pressure generated (same play, defender perspective)
    pfr_pass_def_blitzed            DOUBLE PRECISION,
    pfr_pass_def_hurried            DOUBLE PRECISION,
    pfr_pass_def_hitqb              DOUBLE PRECISION,
    -- rushing (pfr_rush frame)
    pfr_rush_carries                DOUBLE PRECISION,
    pfr_rush_ybc                    DOUBLE PRECISION,
    pfr_rush_ybc_avg                DOUBLE PRECISION,
    pfr_rush_yac                    DOUBLE PRECISION,
    pfr_rush_yac_avg                DOUBLE PRECISION,
    pfr_rush_broken_tackles         DOUBLE PRECISION,
    -- receiving (pfr_rec frame)
    pfr_rec_broken_tackles          DOUBLE PRECISION,
    pfr_rec_drops                   DOUBLE PRECISION,
    pfr_rec_drop_pct                DOUBLE PRECISION,
    pfr_rec_ints                    DOUBLE PRECISION,
    pfr_rec_passer_rating           DOUBLE PRECISION,
    -- coverage / pass rush / tackling (pfr_def frame)
    pfr_def_ints                    DOUBLE PRECISION,
    pfr_def_targets                 DOUBLE PRECISION,
    pfr_def_completions_allowed     DOUBLE PRECISION,
    pfr_def_completion_pct          DOUBLE PRECISION,
    pfr_def_yards_allowed           DOUBLE PRECISION,
    pfr_def_yards_per_cmp           DOUBLE PRECISION,
    pfr_def_yards_per_tgt           DOUBLE PRECISION,
    pfr_def_tds_allowed             DOUBLE PRECISION,
    pfr_def_passer_rating           DOUBLE PRECISION,
    pfr_def_adot                    DOUBLE PRECISION,
    pfr_def_air_yards_completed     DOUBLE PRECISION,
    pfr_def_yac                     DOUBLE PRECISION,
    pfr_def_times_blitzed           DOUBLE PRECISION,
    pfr_def_times_hurried           DOUBLE PRECISION,
    pfr_def_times_hitqb             DOUBLE PRECISION,
    pfr_def_sacks                   DOUBLE PRECISION,
    pfr_def_pressures               DOUBLE PRECISION,
    pfr_def_tackles_combined        DOUBLE PRECISION,
    pfr_def_missed_tackles          DOUBLE PRECISION,
    pfr_def_missed_tackle_pct       DOUBLE PRECISION,
    UNIQUE (pfr_player_id, game_id)
);

-- Fantasy opportunity model. One row per player per game.
-- Player name/position excluded — join to player_info on gsis_id.
CREATE TABLE IF NOT EXISTS public.fantasy_football_opportunities (
    gsis_id                         TEXT,
    game_id                         TEXT,
    season                          BIGINT,
    week                            DOUBLE PRECISION,
    team                            TEXT,
    opps_pass_attempt               DOUBLE PRECISION,
    opps_rec_attempt                DOUBLE PRECISION,
    opps_rush_attempt               DOUBLE PRECISION,
    opps_pass_air_yards             DOUBLE PRECISION,
    opps_rec_air_yards              DOUBLE PRECISION,
    opps_pass_completions           DOUBLE PRECISION,
    opps_receptions                 DOUBLE PRECISION,
    opps_pass_completions_exp       DOUBLE PRECISION,
    opps_receptions_exp             DOUBLE PRECISION,
    opps_pass_yards_gained          DOUBLE PRECISION,
    opps_rec_yards_gained           DOUBLE PRECISION,
    opps_rush_yards_gained          DOUBLE PRECISION,
    opps_pass_yards_gained_exp      DOUBLE PRECISION,
    opps_rec_yards_gained_exp       DOUBLE PRECISION,
    opps_rush_yards_gained_exp      DOUBLE PRECISION,
    opps_pass_touchdown             DOUBLE PRECISION,
    opps_rec_touchdown              DOUBLE PRECISION,
    opps_rush_touchdown             DOUBLE PRECISION,
    opps_pass_touchdown_exp         DOUBLE PRECISION,
    opps_rec_touchdown_exp          DOUBLE PRECISION,
    opps_rush_touchdown_exp         DOUBLE PRECISION,
    opps_pass_two_point_conv        DOUBLE PRECISION,
    opps_rec_two_point_conv         DOUBLE PRECISION,
    opps_rush_two_point_conv        DOUBLE PRECISION,
    opps_pass_two_point_conv_exp    DOUBLE PRECISION,
    opps_rec_two_point_conv_exp     DOUBLE PRECISION,
    opps_rush_two_point_conv_exp    DOUBLE PRECISION,
    opps_pass_first_down            DOUBLE PRECISION,
    opps_rec_first_down             DOUBLE PRECISION,
    opps_rush_first_down            DOUBLE PRECISION,
    opps_pass_first_down_exp        DOUBLE PRECISION,
    opps_rec_first_down_exp         DOUBLE PRECISION,
    opps_rush_first_down_exp        DOUBLE PRECISION,
    opps_pass_interception          DOUBLE PRECISION,
    opps_rec_interception           DOUBLE PRECISION,
    opps_pass_interception_exp      DOUBLE PRECISION,
    opps_rec_interception_exp       DOUBLE PRECISION,
    opps_rec_fumble_lost            DOUBLE PRECISION,
    opps_rush_fumble_lost           DOUBLE PRECISION,
    opps_pass_fantasy_points_exp    DOUBLE PRECISION,
    opps_rec_fantasy_points_exp     DOUBLE PRECISION,
    opps_rush_fantasy_points_exp    DOUBLE PRECISION,
    opps_pass_fantasy_points        DOUBLE PRECISION,
    opps_rec_fantasy_points         DOUBLE PRECISION,
    opps_rush_fantasy_points        DOUBLE PRECISION,
    opps_total_yards_gained         DOUBLE PRECISION,
    opps_total_yards_gained_exp     DOUBLE PRECISION,
    opps_total_touchdown            DOUBLE PRECISION,
    opps_total_touchdown_exp        DOUBLE PRECISION,
    opps_total_first_down           DOUBLE PRECISION,
    opps_total_first_down_exp       DOUBLE PRECISION,
    opps_total_fantasy_points       DOUBLE PRECISION,
    opps_total_fantasy_points_exp   DOUBLE PRECISION,
    opps_pass_completions_diff      DOUBLE PRECISION,
    opps_receptions_diff            DOUBLE PRECISION,
    opps_pass_yards_gained_diff     DOUBLE PRECISION,
    opps_rec_yards_gained_diff      DOUBLE PRECISION,
    opps_rush_yards_gained_diff     DOUBLE PRECISION,
    opps_pass_touchdown_diff        DOUBLE PRECISION,
    opps_rec_touchdown_diff         DOUBLE PRECISION,
    opps_rush_touchdown_diff        DOUBLE PRECISION,
    opps_pass_two_point_conv_diff   DOUBLE PRECISION,
    opps_rec_two_point_conv_diff    DOUBLE PRECISION,
    opps_rush_two_point_conv_diff   DOUBLE PRECISION,
    opps_pass_first_down_diff       DOUBLE PRECISION,
    opps_rec_first_down_diff        DOUBLE PRECISION,
    opps_rush_first_down_diff       DOUBLE PRECISION,
    opps_pass_interception_diff     DOUBLE PRECISION,
    opps_rec_interception_diff      DOUBLE PRECISION,
    opps_pass_fantasy_points_diff   DOUBLE PRECISION,
    opps_rec_fantasy_points_diff    DOUBLE PRECISION,
    opps_rush_fantasy_points_diff   DOUBLE PRECISION,
    opps_total_yards_gained_diff    DOUBLE PRECISION,
    opps_total_touchdown_diff       DOUBLE PRECISION,
    opps_total_first_down_diff      DOUBLE PRECISION,
    opps_total_fantasy_points_diff  DOUBLE PRECISION,
    UNIQUE (gsis_id, game_id)
);

CREATE TABLE IF NOT EXISTS public.fantasy_football_rankings (
    mergename               TEXT,
    pos                     TEXT,
    team                    TEXT,
    page_type               TEXT,
    rank_ecr_type           TEXT,
    rank_ecr                DOUBLE PRECISION,
    rank_sd                 DOUBLE PRECISION,
    rank_best               BIGINT,
    rank_worst              BIGINT,
    rank_rank_delta         DOUBLE PRECISION,
    rank_bye                DOUBLE PRECISION,
    rank_player_owned_avg   DOUBLE PRECISION,
    rank_player_owned_espn  DOUBLE PRECISION,
    rank_player_owned_yahoo DOUBLE PRECISION,
    rank_scrape_date        TEXT,
    UNIQUE (mergename, pos, team, page_type)
);


-- ── Indexes for common join / filter patterns ──────────────────────────────────

-- gsis_id lookups across all player tables
CREATE INDEX IF NOT EXISTS idx_weekly_player_gsis   ON public.weekly_player_data (gsis_id);
CREATE INDEX IF NOT EXISTS idx_weekly_player_season  ON public.weekly_player_data (season, week);
CREATE INDEX IF NOT EXISTS idx_pbp_gsis              ON public.play_by_play (gsis_id);
CREATE INDEX IF NOT EXISTS idx_formations_gsis       ON public.play_by_play_formations (gsis_id);
CREATE INDEX IF NOT EXISTS idx_opps_gsis             ON public.fantasy_football_opportunities (gsis_id);
CREATE INDEX IF NOT EXISTS idx_rosters_gsis          ON public.rosters (gsis_id);
CREATE INDEX IF NOT EXISTS idx_nextgen_gsis          ON public.nextgen (gsis_id);

-- game_id lookups
CREATE INDEX IF NOT EXISTS idx_weekly_player_game    ON public.weekly_player_data (game_id);
CREATE INDEX IF NOT EXISTS idx_sched_game            ON public.sched_final (game_id);
CREATE INDEX IF NOT EXISTS idx_weekly_team_game      ON public.weekly_team_data (game_id);

-- pfr_player_id bridge
CREATE INDEX IF NOT EXISTS idx_snap_pfr              ON public.snap_count (pfr_player_id);
CREATE INDEX IF NOT EXISTS idx_pfr_adv_pfr           ON public.pro_football_ref_adv_stats (pfr_player_id);
CREATE INDEX IF NOT EXISTS idx_ff_ids_pfr            ON public.fantasy_football_ids (pfr_player_id);
CREATE INDEX IF NOT EXISTS idx_ff_ids_gsis           ON public.fantasy_football_ids (gsis_id);

-- Team + season filters (used heavily in matchup models)
CREATE INDEX IF NOT EXISTS idx_sched_team_season     ON public.sched_final (team, season);
CREATE INDEX IF NOT EXISTS idx_team_data_season      ON public.weekly_team_data (team, season);
