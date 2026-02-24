"""
Fetch and load data tables for prop bet modeling.
Returns: player_df (actuals + projections), team_df (off + def, actuals + projections), schedule_df (weather).
"""

import pandas as pd
import numpy as np
import requests
import time
import warnings
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

import pytz

warnings.filterwarnings("ignore")

# =============================================================================
# SLEEPER API - Player, Team, Defense
# =============================================================================

JOIN_COLS = ["sport", "season", "season_type", "week", "date", "game_id", "team", "opponent"]
PLAYER_JOIN_COLS = ["sport", "season", "season_type", "week", "date", "game_id", "team", "opponent", 
"player_id", "player_metadata.channel_id","player_metadata.genius_id","player_metadata.rookie_year",
"player_first_name","player_last_name","player_position"]

PLAYER_STAT_COLS = [
    "metadata.channel_id", "metadata.genius_id", "metadata.rookie_year",
    "first_name", "last_name", "player_id", "position", "tm_off_snp", "off_snp",
    "pass_2pt", "pass_air_yd", "pass_att", "pass_cmp", "cmp_pct", "pass_cmp_40p",
    "pass_fd", "pass_inc", "pass_int", "pass_lng", "pass_rtg", "pass_rush_yd",
    "pass_rz_att", "pass_sack", "pass_sack_yds", "pass_td", "pass_td_40p",
    "pass_td_50p", "pass_td_lng", "pass_yd", "pass_ypa", "pass_ypc",
    "first_td", "anytime_tds", "rec", "rec_10_19", "rec_20_29", "rec_2pt",
    "rec_30_39", "rec_40p", "rec_5_9", "rec_air_yd", "rec_fd", "rec_lng",
    "rec_rz_tgt", "rec_td", "rec_td_40p", "rec_td_50p", "rec_td_lng",
    "rec_tgt", "rec_yar", "rec_yd", "rec_ypr", "rec_ypt",
    "rush_2pt", "rush_40p", "rush_att", "rush_fd", "rush_lng", "rush_rec_yd",
    "rush_rz_att", "rush_td", "rush_td_lng", "rush_tkl_loss", "rush_tkl_loss_yd",
    "rush_yd", "rush_ypa",
    "def_snp", "tm_def_snp", "idp_int", "idp_pass_def", "idp_qb_hit",
    "idp_sack", "idp_sack_yd", "idp_tkl", "idp_tkl_ast", "idp_tkl_loss",
    "idp_tkl_solo", "st_snp", "tm_st_snp", "st_tkl_solo",
]

TEAM_STAT_COLS = [
    "cmp_pct", "down_3_att", "down_3_conv", "down_3_pct", "down_4_att",
    "down_4_conv", "down_4_pct", "fd", "fga", "g2g_att", "g2g_conv",
    "int", "off_yd", "off_yd_per_play", "opp_fd", "opp_off_yd",
    "opp_off_yd_per_play", "opp_pass_fd", "opp_rush_fd",
    "pass_air_yd", "pass_att", "pass_cmp", "pass_fd", "pass_int", "pass_lng",
    "pass_rtg", "pass_rz_att", "pass_sack", "pass_sack_yds", "pass_td",
    "pass_td_lng", "pass_yd", "pass_ypa", "punts", "qb_hit",
    "rec", "rec_air_yd", "rec_lng", "rec_rz_tgt", "rec_td", "rec_td_lng",
    "rec_tgt", "rec_yar", "rec_yd", "rush_att", "rush_fd", "rush_lng",
    "rush_rz_att", "rush_td", "rush_td_lng", "rush_tkl_loss", "rush_tkl_loss_yd",
    "rush_yd", "rush_ypa", "rz_att", "rz_conv", "sack", "st_tkl_solo", "td", "tkl",
]

DEF_STAT_COLS = [
    "yds_allow", "def_3_and_out", "tkl_loss", "tkl_solo", "def_forced_punts",
    "tkl_ast", "pts_allow", "def_4_and_stop",
]

PLAYER_PROJ_COLS = [
    "metadata.channel_id", "metadata.genius_id", "metadata.rookie_year",
    "first_name", "last_name", "player_id", "position",
    "pass_2pt", "pass_att", "pass_cmp", "cmp_pct", "pass_cmp_40p", "pass_fd",
    "pass_inc", "pass_int", "pass_sack", "pass_td", "pass_yd",
    "rec", "rec_10_19", "rec_20_29", "rec_2pt", "rec_30_39", "rec_40p",
    "rec_5_9", "rec_fd", "rec_td", "rec_tgt", "rec_yd",
    "rush_2pt", "rush_40p", "rush_att", "rush_fd", "rush_td", "rush_yd",
    "idp_int", "idp_pass_def", "idp_qb_hit", "idp_sack", "idp_sack_yd",
    "idp_tkl", "idp_tkl_ast", "idp_tkl_loss", "idp_tkl_solo",
]

DEF_PROJ_COLS = ["sack", "int", "yds_allow", "tkl_loss", "pts_allow"]

VALID_POSITIONS = ["QB", "RB", "WR", "TE", "DL", "DE", "DT", "LB", "DB", "CB"]


def _rename_cols(cols, prefix, join_cols):
    return [
        f"{prefix}{c}" if c not in join_cols and not c.startswith(prefix) else c
        for c in cols
    ]


def fetch_sleeper_data_multi_week(
    season=2025, end_week=18, n_weeks=1, season_type="regular", stat_type="stats"
):
    """Fetch Sleeper API data for the past n weeks. stat_type: 'stats' or 'projections'."""
    start_week = max(1, end_week - n_weeks + 1)
    print(f"Fetching weeks {start_week} through {end_week} for {season} {season_type}...")

    all_weeks_data = []
    for week in range(start_week, end_week + 1):
        print(f"  Week {week}...", end=" ")
        try:
            url = (
                f"https://api.sleeper.app/{stat_type}/nfl/{season}/{week}"
                f"?season_type={season_type}"
            )
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                print("No data")
                continue

            df = pd.DataFrame(data).rename(columns={"stats": stat_type})
            stats_expanded = pd.json_normalize(df[stat_type])
            full = pd.concat([df, stats_expanded], axis=1)

            player_details = pd.json_normalize(df["player"])
            player_details = pd.concat([
                player_details,
                df[["date", "week", "sport", "season", "season_type", "player_id", "game_id", "opponent"]]
            ], axis=1).drop(columns=["fantasy_positions", "news_updated", "team_abbr", "team_changed_at"], errors="ignore")
            player_details = player_details.rename(columns={"name": "player_name"})

            merged = pd.merge(
                player_details,
                full,
                on=["date", "week", "sport", "season", "season_type", "player_id", "game_id", "team", "opponent"],
                how="left",
            )
            merged = merged[merged["gp"] > 0]
            all_weeks_data.append(merged)
            print(f"{len(merged)} players")
            time.sleep(0.5)
        except Exception as e:
            print(f"Error: {e}")
            continue

    if not all_weeks_data:
        return pd.DataFrame()

    out = pd.concat(all_weeks_data, ignore_index=True)
    print(f"\n✓ Fetched {len(out)} rows, {out['player_id'].nunique()} players")
    return out


def _process_sleeper_stats(df):
    """Process stats response into player, team (off), and def DataFrames."""
    player_cols = JOIN_COLS + PLAYER_STAT_COLS
    player_df = df[df["position"].isin(VALID_POSITIONS)].copy()[player_cols]
    player_df.columns = _rename_cols(player_df.columns, "player_", JOIN_COLS)

    team_cols = JOIN_COLS + TEAM_STAT_COLS
    team_df = df[df["position"] == "TEAM"].copy()[team_cols]
    team_df.columns = _rename_cols(team_df.columns, "off_", JOIN_COLS)

    def_cols = JOIN_COLS + DEF_STAT_COLS
    def_df = df[df["position"] == "DEF"].copy()[def_cols]
    def_df.columns = _rename_cols(def_df.columns, "def_", JOIN_COLS)

    return player_df, team_df, def_df


def _process_sleeper_projections(df):
    """Process projections response. No TEAM row; derive offense from DEF rows."""
    player_cols = JOIN_COLS + PLAYER_PROJ_COLS
    player_df = df[df["position"].isin(VALID_POSITIONS)].copy()[player_cols]
    player_df.columns = _rename_cols(player_df.columns, "player_", JOIN_COLS)

    def_cols = JOIN_COLS + DEF_PROJ_COLS
    def_df = df[df["position"] == "DEF"].copy()[def_cols]
    def_df.columns = _rename_cols(def_df.columns, "def_", JOIN_COLS)

    # Offense = opponent's defense allowed (swap team/opponent)
    off_df = def_df.rename(columns={"team": "opponent", "opponent": "team"})
    off_df = off_df.rename(columns={
        "def_sack": "off_sack", "def_int": "off_int", "def_yds_allow": "off_yds",
        "def_tkl_loss": "off_tkl_loss", "def_pts_allow": "off_pts",
    })
    team_df = off_df.merge(def_df, on=JOIN_COLS, how="left")
    return player_df, team_df, def_df


def _combine_team_def(team_df, def_df):
    """Merge team (offensive) and def (defensive) into one team-level table."""
    return team_df.merge(def_df, on=JOIN_COLS, how="left")


def _merge_actuals_projections(actual_df, proj_df, join_cols, stat_cols_actual, stat_cols_proj):
    """Merge actual and projection DataFrames, suffixing projection cols with _proj."""
    # Keep identity + actual stats from actual_df
    actual_subset = actual_df[join_cols + stat_cols_actual].copy()
    # Get projection stats only (avoid duplicate identity cols)
    proj_subset = proj_df[join_cols + stat_cols_proj].copy()
    proj_subset = proj_subset.rename(columns={c: f"{c}_proj" for c in stat_cols_proj if c in proj_subset.columns})
    merged = actual_subset.merge(
        proj_subset.drop(columns=[c for c in join_cols if c in proj_subset.columns and c != "game_id"], errors="ignore"),
        on=join_cols,
        how="left",
    )
    return merged


# =============================================================================
# ESPN + WEATHER - Schedule
# =============================================================================

NFL_DIVISIONS = {
    "AFC East": ["BUF", "MIA", "NE", "NYJ"],
    "AFC North": ["BAL", "CIN", "CLE", "PIT"],
    "AFC South": ["HOU", "IND", "JAX", "TEN"],
    "AFC West": ["DEN", "KC", "LV", "LAC"],
    "NFC East": ["DAL", "NYG", "PHI", "WSH"],
    "NFC North": ["CHI", "DET", "GB", "MIN"],
    "NFC South": ["ATL", "CAR", "NO", "TB"],
    "NFC West": ["ARI", "LAR", "SF", "SEA"],
}
TEAM_DIVISIONS = {t: div for div, teams in NFL_DIVISIONS.items() for t in teams}

STADIUM_COORDINATES = {
    "ARI": (33.5276, -112.2626), "ATL": (33.7554, -84.4006), "BAL": (39.2780, -76.6227),
    "BUF": (42.7738, -78.7870), "CAR": (35.2258, -80.8529), "CHI": (41.8623, -87.6167),
    "CIN": (39.0954, -84.5160), "CLE": (41.5061, -81.6995), "DAL": (32.7473, -97.0945),
    "DEN": (39.7439, -105.0201), "DET": (42.3400, -83.0456), "GB": (44.5013, -88.0622),
    "HOU": (29.6847, -95.4107), "IND": (39.7601, -86.1639), "JAX": (30.3239, -81.6373),
    "KC": (39.0489, -94.4839), "LV": (36.0908, -115.1834), "LAC": (33.8643, -118.3396),
    "LAR": (33.8643, -118.3396), "MIA": (25.9580, -80.2389), "MIN": (44.9738, -93.2577),
    "NE": (42.0909, -71.2643), "NO": (29.9511, -90.0812), "NYG": (40.8128, -74.0742),
    "NYJ": (40.8128, -74.0742), "PHI": (39.9008, -75.1675), "PIT": (40.4468, -80.0158),
    "SF": (37.4032, -121.9696), "SEA": (47.5952, -122.3316), "TB": (27.9759, -82.5033),
    "TEN": (36.1665, -86.7713), "WSH": (38.9076, -76.8645),
}


def _haversine_miles(lat1, lon1, lat2, lon2):
    """Great circle distance in miles."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return round(3959 * c, 1)


def _degrees_to_cardinal(deg):
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return directions[round(deg / 22.5) % 16]


def _weather_description(code):
    codes = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Depositing rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
        55: "Dense drizzle", 61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow", 80: "Slight rain showers",
        81: "Moderate rain showers", 82: "Violent rain showers", 95: "Thunderstorm",
    }
    return codes.get(code, "Unknown")


def get_schedule(season=2025, week=None):
    """Fetch NFL schedule from ESPN with venue, travel, broadcast info."""
    print(f"Fetching NFL schedule for {season}...")
    weeks = [week] if week else range(1, 19)
    all_games = []

    for wk in weeks:
        print(f"  Week {wk:2d}...", end=" ", flush=True)
        try:
            resp = requests.get(
                "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
                params={"dates": season, "seasontype": 2, "week": wk},
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}")
                continue

            data = resp.json()
            events = data.get("events", [])
            if not events:
                print("No games")
                continue

            eastern = pytz.timezone("US/Eastern")
            for event in events:
                game = _parse_espn_game(event, season, wk, eastern)
                if game:
                    all_games.append(game)
            print(f"✓ {len(events)} games")
            time.sleep(0.5)
        except Exception as e:
            print(f"Error: {e}")
            continue

    if not all_games:
        return pd.DataFrame()

    df = pd.DataFrame(all_games)
    print(f"\n✓ Total: {len(df)} games")
    return df


def _parse_espn_game(event, season, week, eastern):
    try:
        game_id = event["id"]
        comp = event["competitions"][0]
        competitors = comp["competitors"]
        home = next(c for c in competitors if c["homeAway"] == "home")
        away = next(c for c in competitors if c["homeAway"] == "away")

        home_abbr = home["team"]["abbreviation"]
        away_abbr = away["team"]["abbreviation"]

        dt_utc = datetime.fromisoformat(event["date"].replace("Z", "+00:00"))
        dt_est = dt_utc.astimezone(eastern)

        venue = comp.get("venue", {})
        hour = dt_est.hour
        if 13 <= hour < 16:
            time_slot = "early_afternoon"
        elif 16 <= hour < 20:
            time_slot = "late_afternoon"
        elif 20 <= hour < 24:
            time_slot = "primetime"
        else:
            time_slot = "other"

        home_div = TEAM_DIVISIONS.get(home_abbr, "Unknown")
        away_div = TEAM_DIVISIONS.get(away_abbr, "Unknown")
        is_divisional = home_div == away_div and home_div != "Unknown"
        same_conf = home_div.split()[0] == away_div.split()[0] if "Unknown" not in [home_div, away_div] else 0

        travel = None
        if away_abbr in STADIUM_COORDINATES and home_abbr in STADIUM_COORDINATES:
            travel = _haversine_miles(
                *STADIUM_COORDINATES[away_abbr],
                *STADIUM_COORDINATES[home_abbr],
            )

        home_coords = STADIUM_COORDINATES.get(home_abbr, (None, None))
        broadcasts = comp.get("broadcasts", [])
        broadcast_net = broadcasts[0].get("names", [""])[0] if broadcasts else ""

        return {
            "season": season,
            "week": week,
            "game_id": game_id,
            "sleeper_game_id": f"{season}{week}{game_id}",
            "game_date": dt_est.strftime("%Y-%m-%d"),
            "game_time_est": dt_est.strftime("%H:%M"),
            "game_datetime_est": dt_est.isoformat(),
            "day_of_week": dt_est.strftime("%A"),
            "hour_est": hour,
            "time_slot": time_slot,
            "is_primetime": int(time_slot == "primetime"),
            "is_thursday": int(dt_est.strftime("%A") == "Thursday"),
            "is_monday": int(dt_est.strftime("%A") == "Monday"),
            "is_sunday": int(dt_est.strftime("%A") == "Sunday"),
            "away_team": away_abbr,
            "away_team_name": away["team"]["displayName"],
            "away_team_location": away["team"].get("location", ""),
            "home_team": home_abbr,
            "home_team_name": home["team"]["displayName"],
            "home_team_location": home["team"].get("location", ""),
            "home_lat": home_coords[0],
            "home_lon": home_coords[1],
            "away_division": away_div,
            "home_division": home_div,
            "is_divisional_game": int(is_divisional),
            "same_conference": same_conf,
            "travel_distance_miles": travel,
            "stadium": venue.get("fullName", ""),
            "city": venue.get("address", {}).get("city", ""),
            "state": venue.get("address", {}).get("state", ""),
            "is_indoor": venue.get("indoor", False),
            "broadcast_network": broadcast_net,
            "attendance": comp.get("attendance"),
            "away_record": away.get("records", [{}])[0].get("summary", "") if away.get("records") else "",
            "home_record": home.get("records", [{}])[0].get("summary", "") if home.get("records") else "",
        }
    except Exception as e:
        print(f"\nParse error {event.get('id', '?')}: {e}")
        return None


def get_gametime_weather(lat, lon, game_datetime_str, tz="America/New_York"):
    """Get hourly weather at game time from Open-Meteo archive API."""
    try:
        dt = datetime.fromisoformat(game_datetime_str.replace("Z", "+00:00"))
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("%Y-%m-%dT%H:00")

        resp = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": date_str,
                "end_date": date_str,
                "hourly": "temperature_2m,relativehumidity_2m,dewpoint_2m,apparent_temperature,precipitation,rain,snowfall,weathercode,cloudcover,windspeed_10m,windgusts_10m,winddirection_10m,visibility,surface_pressure",
                "temperature_unit": "fahrenheit",
                "windspeed_unit": "mph",
                "precipitation_unit": "inch",
                "timezone": tz,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return {"error": f"API {resp.status_code}"}

        data = resp.json()
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        try:
            idx = times.index(hour_str)
        except ValueError:
            return {"error": f"Time {hour_str} not in data"}

        w = {
            "game_datetime": game_datetime_str,
            "weather_datetime": times[idx],
            "temperature_f": hourly["temperature_2m"][idx],
            "feels_like_f": hourly["apparent_temperature"][idx],
            "dewpoint_f": hourly["dewpoint_2m"][idx],
            "humidity_percent": hourly["relativehumidity_2m"][idx],
            "precipitation_inch": hourly["precipitation"][idx],
            "rain_inch": hourly["rain"][idx],
            "snowfall_inch": hourly["snowfall"][idx],
            "wind_speed_mph": hourly["windspeed_10m"][idx],
            "wind_gust_mph": hourly["windgusts_10m"][idx],
            "wind_direction_degrees": hourly["winddirection_10m"][idx],
            "cloud_cover_percent": hourly["cloudcover"][idx],
            "visibility_feet": hourly["visibility"][idx] * 3.28084 if hourly["visibility"][idx] else None,
            "surface_pressure_hpa": hourly["surface_pressure"][idx],
            "weather_code": hourly["weathercode"][idx],
        }
        w.update(_football_conditions(w))
        return w
    except Exception as e:
        return {"error": str(e)}


def _football_conditions(w):
    temp = w.get("temperature_f")
    wind = w.get("wind_speed_mph", 0)
    wind_gust = w.get("wind_gust_mph", 0)
    precip = w.get("precipitation_inch", 0)
    snow = w.get("snowfall_inch", 0)

    if temp is not None:
        if temp < 20:
            cat = "extreme_cold"
        elif temp < 32:
            cat = "freezing"
        elif temp < 45:
            cat = "cold"
        elif temp < 60:
            cat = "cool"
        elif temp < 75:
            cat = "comfortable"
        elif temp < 85:
            cat = "warm"
        elif temp < 95:
            cat = "hot"
        else:
            cat = "extreme_heat"
    else:
        cat = None

    if wind_gust and wind_gust > 20:
        wind_impact = "severe"
    elif wind_gust and wind_gust > 15 or wind > 15:
        wind_impact = "high"
    elif wind and wind > 10:
        wind_impact = "moderate"
    else:
        wind_impact = "low"

    if snow and snow > 0.5:
        precip_impact = "heavy_snow"
    elif snow and snow > 0:
        precip_impact = "light_snow"
    elif precip > 0.2:
        precip_impact = "heavy_rain"
    elif precip > 0:
        precip_impact = "light_rain"
    else:
        precip_impact = "none"

    rating = 10
    if temp is not None:
        if temp < 20 or temp > 95:
            rating -= 4
        elif temp < 32 or temp > 85:
            rating -= 2
        elif temp < 45 or temp > 80:
            rating -= 1
    if wind_gust and wind_gust > 20:
        rating -= 3
    elif wind and wind > 15:
        rating -= 2
    elif wind and wind > 10:
        rating -= 1
    if snow and snow > 0.5:
        rating -= 3
    elif snow and snow > 0:
        rating -= 2
    elif precip > 0.2:
        rating -= 2
    elif precip > 0:
        rating -= 1
    rating = max(1, rating)

    out = {
        "temp_category": cat,
        "wind_impact": wind_impact,
        "precipitation_impact": precip_impact,
        "playing_conditions_rating": rating,
        "weather_description": _weather_description(w.get("weather_code")),
    }
    if w.get("wind_direction_degrees") is not None:
        out["wind_direction_cardinal"] = _degrees_to_cardinal(w["wind_direction_degrees"])
    return out


def add_weather_to_schedule(schedule_df):
    """Add weather columns to schedule DataFrame."""
    print("Fetching weather...")
    weather_rows = []
    for idx, row in schedule_df.iterrows():
        lat, lon = row.get("home_lat"), row.get("home_lon")
        if lat is None or lon is None:
            weather_rows.append({})
            continue

        w = get_gametime_weather(lat, lon, row["game_datetime_est"])
        weather_rows.append(w if "error" not in w else {})
        time.sleep(0.2)

    weather_df = pd.DataFrame(weather_rows)
    return pd.concat([schedule_df.reset_index(drop=True), weather_df], axis=1)


# =============================================================================
# MAIN: Fetch and return 3 consolidated tables
# =============================================================================


def fetch_tables(season=2025, week=18, n_weeks=1, schedule_week=None, add_weather=True):
    """
    Fetch and return a single master DataFrame with player, team, and schedule joined.
    - Player + team merge on: sport, season, season_type, week, date, game_id, team, opponent
    - Schedule merge on game_key (season_week_team1_team2) since Sleeper game_id != ESPN game_id
    """
    # 1. Sleeper stats (actuals)
    stats_raw = fetch_sleeper_data_multi_week(
        season=season, end_week=week, n_weeks=n_weeks, season_type="regular", stat_type="stats"
    )
    if stats_raw.empty:
        raise ValueError("No stats data returned")

    player_stats, team_stats, def_stats = _process_sleeper_stats(stats_raw)
    team_actuals = _combine_team_def(team_stats, def_stats)

    # 2. Sleeper projections
    proj_raw = fetch_sleeper_data_multi_week(
        season=season, end_week=week, n_weeks=n_weeks, season_type="regular", stat_type="projections"
    )
    if proj_raw.empty:
        raise ValueError("No projections data returned")

    player_proj, team_proj, _ = _process_sleeper_projections(proj_raw)

    # 3. Merge player actuals + projections
    player_identity = [c for c in PLAYER_JOIN_COLS if c in player_stats.columns]
    player_stat_cols = [c for c in player_stats.columns if c not in PLAYER_JOIN_COLS and c.startswith("player_")]
    player_proj_cols = [c for c in player_proj.columns if c not in PLAYER_JOIN_COLS and c.startswith("player_")]

    player_df = player_stats.merge(
        player_proj.rename(columns={c: f"{c}_proj" for c in player_proj_cols}),
        on=player_identity,
        how="left",
        suffixes=("", "_proj"),
    )
    # Drop duplicate _proj suffix cols if any
    player_df = player_df.loc[:, ~player_df.columns.duplicated()]

    # 4. Merge team actuals + projections (off + def already combined in team_actuals)
    team_identity = [c for c in JOIN_COLS if c in team_actuals.columns]
    team_actual_cols = [c for c in team_actuals.columns if c not in JOIN_COLS]
    team_proj_cols = [c for c in team_proj.columns if c not in JOIN_COLS]

    team_df = team_actuals.merge(
        team_proj.rename(columns={c: f"{c}_proj" for c in team_proj_cols}),
        on=team_identity,
        how="left",
    )
    team_df = team_df.loc[:, ~team_df.columns.duplicated()]

    # 5. Schedule + weather
    sw = schedule_week if schedule_week is not None else week
    schedule_df = get_schedule(season=season, week=sw)
    if schedule_df.empty:
        raise ValueError("No schedule data returned")

    if add_weather:
        schedule_df = add_weather_to_schedule(schedule_df)

    for col in ["home_team", "away_team"]:
        if col in schedule_df.columns:
            schedule_df[col] = schedule_df[col].replace("WSH", "WAS")    

    # 6. Join all tables: player + team on JOIN_COLS, then + schedule on game_key
    # (schedule game_id/sleeper_game_id don't align with Sleeper game_id)
    player_team_df = player_df.merge(
        team_df,
        on=JOIN_COLS,
        how="left",
        suffixes=("", "_team"),  # team cols get _team if overlap (shouldn't be)
    )
    player_team_df = player_team_df.loc[:, ~player_team_df.columns.duplicated()]

    # Normalize season/week dtypes for merge (Sleeper may return object, schedule has int64)
    for col in ["season", "week"]:
        if col in player_team_df.columns:
            player_team_df[col] = pd.to_numeric(player_team_df[col], errors="coerce").astype("Int64")
        if col in schedule_df.columns:
            schedule_df[col] = pd.to_numeric(schedule_df[col], errors="coerce").astype("Int64")

    # Create game_key for schedule join: season_week_team1_team2 (sorted teams)
    def _game_key(row, team_a, team_b):
        return f"{row['season']}_{row['week']}_{'_'.join(sorted([str(row[team_a]), str(row[team_b])]))}"

    player_team_df["game_key"] = player_team_df.apply(
        lambda r: _game_key(r, "team", "opponent"), axis=1
    )
    schedule_df["game_key"] = schedule_df.apply(
        lambda r: _game_key(r, "home_team", "away_team"), axis=1
    )

    # Merge schedule (rename game_id to espn_game_id to avoid clash with Sleeper game_id)
    schedule_merge = schedule_df.copy()
    if "game_id" in schedule_merge.columns:
        schedule_merge = schedule_merge.rename(columns={"game_id": "espn_game_id"})
    schedule_cols = [c for c in schedule_merge.columns if c not in ["season", "week", "game_key"]]
    master_df = player_team_df.merge(
        schedule_merge[["season", "week", "game_key"] + schedule_cols],
        on=["season", "week", "game_key"],
        how="left",
    )

    master_df = _cleanup_master_df(master_df)
    return master_df


def _cleanup_master_df(df):
    """Rename, drop redundant columns, consolidate player_name, add is_home, simplify date/time."""
    df = df.copy()

    # Rename: off_int -> def_int, off_int_proj -> def_int_proj
    rename_map = {}
    if "off_int" in df.columns:
        rename_map["off_int"] = "def_int"
    if "off_int_proj" in df.columns:
        rename_map["off_int_proj"] = "def_int_proj"
    if "off_qb_hit" in df.columns:
        rename_map["off_qb_hit"] = "def_qb_hit"
    if "off_sack" in df.columns:
        rename_map["off_sack"] = "def_sack"
    df = df.rename(columns=rename_map)

    # Drop redundant team columns
    drop_cols = [
        "off_rec", "off_rec_td", "off_yd", "off_yd_per_play", "off_td", "off_opp_off_yd_per_play",
        "stadium", "city", "state", "temperature_f", "precipitation_inch", 
        "wind_direction_degrees", "visibility_feet", "wind_direction_cardinal"
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    # Combine player_first_name + player_last_name -> player_name
    if "player_first_name" in df.columns and "player_last_name" in df.columns:
        df["player_name"] = (
            df["player_first_name"].fillna("").astype(str).str.strip()
            + " "
            + df["player_last_name"].fillna("").astype(str).str.strip()
        ).str.strip()
        df = df.drop(columns=["player_first_name", "player_last_name"], errors="ignore")

    # Add is_home before dropping home/away cols
    if "team" in df.columns and "home_team" in df.columns:
        df["is_home"] = (df["team"] == df["home_team"]).astype(int)

    # Drop home/away verbose columns (keep home_team, away_team, is_home)
    df = df.drop(
        columns=[
            "away_team_name", "home_team_name",
            "away_team_location", "home_team_location",
            "player_metadata.channel_id","player_metadata.genius_id",
            "player_metadata.channel_id"
        ],
        errors="ignore",
    )

    # Date/time: keep week, month, is_primetime, day_of_week, time_est
    if "game_date" in df.columns:
        df["month"] = pd.to_datetime(df["game_date"], errors="coerce").dt.month
    if "game_time_est" in df.columns:
        df["time_est"] = df["game_time_est"]
    drop_datetime = [
        "date", "game_date", "game_datetime_est", "game_datetime", "weather_datetime",
        "game_time_est", "hour_est", "time_slot", "is_thursday", "is_monday", "is_sunday",
    ]
    df = df.drop(columns=[c for c in drop_datetime if c in df.columns], errors="ignore")

    # Keep only player_id and game_key for IDs; drop game_id, espn_game_id, sleeper_game_id
    df = df.drop(columns=["game_id", "espn_game_id", "sleeper_game_id"], errors="ignore")

    return df


def get_all_tables(season=2025, week=18, n_weeks=1, schedule_week=None, add_weather=True):
    """Return single master DataFrame with player, team, and schedule (weather) joined."""
    master_df = fetch_tables(
        season=season, week=week, n_weeks=n_weeks,
        schedule_week=schedule_week, add_weather=add_weather,
    )
    return {"master": master_df}


if __name__ == "__main__":
    tables = get_all_tables(season=2025, week=18, n_weeks=1, schedule_week=18, add_weather=True)
    master_df = tables["master"]
    print(f"master: {master_df.shape}")