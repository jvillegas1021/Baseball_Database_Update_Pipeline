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

    # 3. Convert to datetime, subtract 45 min, convert to Eastern
    games['gameDate'] = pd.to_datetime(games['gameDate'])
    games['gameDate'] = games['gameDate'] - pd.Timedelta(minutes=45)
    games['gameDate'] = games['gameDate'].dt.tz_convert('US/Eastern')

    # 4. Convert to Python datetime objects
    run_times = sorted({pd.Timestamp(t).to_pydatetime() for t in games['gameDate']})

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
