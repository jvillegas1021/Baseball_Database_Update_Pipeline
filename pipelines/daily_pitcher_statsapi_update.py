from data_extract_functions.extract_mlb_games_info import get_current_pitcher_ids, get_pitcher_info_and_stats_season
from data_load_functions.load_data_to_database import push_pitcher_data_to_sql_upsert
import pandas as pd
from datetime import datetime
import pytz

def run_daily_pitcher_statsapi_update():

    current_year = datetime.now().year

    current_pitcher_ids = get_current_pitcher_ids()

    all_current_pitchers_df_list = []

    for pitcher in current_pitcher_ids:
        current_pitcher_df = get_pitcher_info_and_stats_season(pitcher, season=current_year)
        if current_pitcher_df is None or current_pitcher_df.empty:
            continue
        all_current_pitchers_df_list.append(current_pitcher_df)

    final_pitcher_df = pd.concat(all_current_pitchers_df_list)

    final_pitcher_df['season'] = final_pitcher_df['season'].astype(int)
    final_pitcher_df['update_date'] = datetime.now(pytz.timezone("America/New_York"))

    data_table_name = 'pitcher_seasonal_data_statsapi'

    push_pitcher_data_to_sql_upsert(data_table_name, final_pitcher_df)

if __name__ == "__main__":
    run_daily_pitcher_statsapi_update()