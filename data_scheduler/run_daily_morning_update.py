from pipelines.daily_pitcher_statcast_update  import run_daily_pitcher_statcast_update
from pipelines.daily_pitcher_statsapi_update  import run_daily_pitcher_statsapi_update
from pipelines.daily_batter_statcast_update  import run_daily_batter_statcast_update
from pipelines.daily_batter_statsapi_update  import run_daily_batter_statsapi_update
from pipelines.daily_pitcher_stats_update  import run_daily_pitcher_stats_update
from pipelines.daily_pitcher_current_season_stats_update import run_daily_pitcher_current_season_stats_update
from pipelines.daily_pitcher_recent_form_update import run_starting_pitchers_recent_form_update

def run_one_time_stats_update():
    
    run_daily_pitcher_statcast_update()
    run_daily_pitcher_statsapi_update()
    run_daily_batter_statcast_update()
    run_daily_batter_statsapi_update()
    run_daily_pitcher_stats_update()
    run_daily_pitcher_current_season_stats_update()
    run_starting_pitchers_recent_form_update()

if __name__ == "__main__":
    run_one_time_stats_update()

