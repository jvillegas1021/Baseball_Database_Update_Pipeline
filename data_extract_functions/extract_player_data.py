from datetime import datetime, timedelta
import pandas as pd
import requests
import statsapi


def extract_pitcher_recent_form(pitcher_id: int) -> pd.DataFrame:

    url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats?stats=gameLog&group=pitching"
    pitcher_data = requests.get(url).json()

    stats_list = pitcher_data.get("stats", [])

    # No stats → return None
    if not stats_list or "splits" not in stats_list[0] or not stats_list[0]["splits"]:
        return None

    splits = stats_list[0]["splits"]
    num_starts = len(splits)

    # Last 3 appearances
    start_index = max(0, num_starts - 3)
    recent_starts = splits[start_index:]
    number_of_starts = len(recent_starts)

    strike_outs = 0
    batters_faced = 0
    walks = 0
    ip = 0
    hits = 0
    earned_runs = 0
    strikes = 0
    pitches = 0
    ground_outs = 0
    fly_outs = 0
    homeruns = 0

    for s in recent_starts:
        stat = s['stat']
        strike_outs += stat.get('strikeOuts', 0)
        batters_faced += stat.get('battersFaced', 0)
        walks += stat.get('baseOnBalls', 0)
        ip += float(stat.get('inningsPitched', 0))
        hits += stat.get('hits', 0)
        earned_runs += stat.get('earnedRuns', 0)
        strikes += stat.get('strikes', 0)
        pitches += stat.get('numberOfPitches', 0)
        ground_outs += stat.get('groundOuts', 0)
        fly_outs += stat.get('flyOuts', 0)
        homeruns += stat.get('homeRuns', 0)

    pitcher_df = pd.DataFrame({
        'xMLBAMID': [pitcher_id],
        'K%': strike_outs / batters_faced if batters_faced > 0 else 0,
        'BB%': walks / batters_faced if batters_faced > 0 else 0,
        'WHIP': (walks + hits) / ip if ip > 0 else 0,
        'ERA': (earned_runs / ip) * 9 if ip > 0 else 0,
        'Str%': strikes / pitches if pitches > 0 else 0,
        'GB%': ground_outs / (ground_outs + fly_outs) if (ground_outs + fly_outs) > 0 else 0,
        'HR/FB': homeruns / fly_outs if fly_outs > 0 else 0,
        'IP Per Start': ip / number_of_starts if number_of_starts > 0 else 0,
        'Number of Starts': number_of_starts
    })

    return pitcher_df
    
def batter_splits(pitch_hand: int, stat_type: int):
    today = datetime.today().strftime("%Y-%m-%d")

    url = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
    
    payload = {
        "strPlayerId": "all",
        "strSplitArr": [pitch_hand],          # 2 = vs RHP, 1 = vs LHP
        "strGroup": "season",
        "strPosition": "B",
        "strType": f"{stat_type}",                # 1,2,3 = 1(Basic) 2 (Advanced stats) 3 (Batted Ball)
        "strStartDate": "2025-01-01",
        "strEndDate": today,
        "strSplitTeams": False,
        "dctFilters": [
            {
                "stat": "PA",
                "low": "100",
                "high": -99,
                "comp": "gt",
                "auto": False,
                "pending": True,
                "label": "PA ≥ 100",
                "value": 0
            }
        ],
        "strStatType": "player",
        "strAutoPt": "false",
        "arrPlayerId": [],
        "strSplitArrPitch": [],
        "arrWxTemperature": None,
        "arrWxPressure": None,
        "arrWxAirDensity": None,
        "arrWxElevation": None,
        "arrWxWindSpeed": None
    }
    
    response = requests.post(url, json=payload).json()
    data = pd.json_normalize(response["data"])
    
    return(data)


def get_current_player_ids():
    pitcher_ids = set()
    batter_ids = set()

    # 1. Get all MLB teams
    teams = requests.get("https://statsapi.mlb.com/api/v1/teams?sportId=1").json()["teams"]

    for team in teams:
        team_id = team["id"]

        # 2. Get roster for each team
        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
        roster = requests.get(roster_url).json()["roster"]

        # 3. Filter pitchers
        for player in roster:
            if player["position"]["type"] == "Pitcher":
                pitcher_ids.add(player["person"]["id"])
            else:
                batter_ids.add(player["person"]["id"])

    return (pitcher_ids, batter_ids)

def get_pitcher_info_and_stats_season(pitcher_id, season=2026):
    url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats"
    params = {
        "stats": "season",
        "group": "pitching",
        "season": season
    }

    request = requests.get(url, params=params).json()

    # If request failed or empty
    if not request:
        return None

    # Guard: stats list missing or empty
    stats_list = request.get('stats', [])
    if not stats_list:
        return None

    # Guard: splits missing or empty
    splits = stats_list[0].get('splits', [])
    if not splits:
        return None

    # Safe to access now
    pitcher_details = splits[0]

    # Base info
    pitcher_df = pd.DataFrame({
        'xMLBAMID': [pitcher_details['player']['id']],
        'player_name': [pitcher_details['player']['fullName']],
        'season': [pitcher_details['season']]
    })

    # Normalize stat block
    pitcher_details_normalize = pd.json_normalize(pitcher_details['stat'])

    # Combine horizontally
    pitcher_df = pd.concat([pitcher_df, pitcher_details_normalize], axis=1)

    return pitcher_df

def get_batter_info_and_stats_season(batter_id, season=2026):
    url = f"https://statsapi.mlb.com/api/v1/people/{batter_id}/stats"
    params = {
        "stats": "season",
        "group": "hitting",
        "season": season
    }

    request = requests.get(url, params=params).json()

    # If request failed or empty
    if not request:
        return None

    # Guard: stats list missing or empty
    stats_list = request.get('stats', [])
    if not stats_list:
        return None

    # Guard: splits missing or empty
    splits = stats_list[0].get('splits', [])
    if not splits:
        return None

    # Safe to access now
    batter_details = splits[0]

    # Base info
    batter_df = pd.DataFrame({
        'xMLBAMID': [batter_details['player']['id']],
        'player_name': [batter_details['player']['fullName']],
        'season': [batter_details['season']]
    })

    # Normalize stat block
    batter_details_normalize = pd.json_normalize(batter_details['stat'])

    # Combine horizontally
    batter_df = pd.concat([batter_df, batter_details_normalize], axis=1)

    return batter_df
