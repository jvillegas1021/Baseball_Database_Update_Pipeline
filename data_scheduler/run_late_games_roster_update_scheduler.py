# scheduler/run_today_schedule.py

import time
import pandas as pd
from datetime import datetime, date
from zoneinfo import ZoneInfo

from data_extract_functions.extract_mlb_games_info import games_today
from pipelines.daily_roster_update import run_daily_roster_update
from pipelines.daily_pitcher_recent_form_update import run_starting_pitchers_recent_form_update
from pipelines.daily_team_travel_update import run_daily_team_travel_update


def roster_stats_update():
    # 1. Get today's date
    today = date.today().strftime("%Y-%m-%d")

    # 2. Pull today's games
    games = games_today(today)

    games = games[~games['gameType'].isin(['A', 'E'])]

    if games.empty:
        print("No MLB games today. Scheduler exiting.")
        return

    eastern = ZoneInfo("America/New_York")

    morning_games_cutoff_time = datetime.now(eastern).replace(
        hour=16, minute=00, second=0, microsecond=0
    )

    # 3. Convert to datetime, subtract 45 min, convert to Eastern
    games['gameDate'] = pd.to_datetime(games['gameDate'])
    games['gameDate'] = games['gameDate'].dt.tz_convert('US/Eastern')

    games_before_cutoff = games['gameDate'] > morning_games_cutoff_time

    filtered_game_times = games[games_before_cutoff]

    filtered_game_times['gameDate'] = filtered_game_times['gameDate'] - pd.Timedelta(minutes=45)

    # 4. Convert to Python datetime objects
    run_times = sorted({pd.Timestamp(t).to_pydatetime() for t in filtered_game_times['gameDate']})

    # 5. Loop through run times
    for rt in run_times:
        print(f"Waiting for {rt}...")

        now = datetime.now(ZoneInfo("US/Eastern"))
        seconds_to_wait = (rt - now).total_seconds()

        if seconds_to_wait > 0:
            time.sleep(seconds_to_wait)

        print(f"Running update at {rt}")
        run_daily_roster_update()
        run_starting_pitchers_recent_form_update()


if __name__ == "__main__":
    roster_stats_update()
