from data_extract_functions.extract_mlb_games_info import get_current_batter_ids, get_batter_info_and_stats_season
from data_load_functions.load_data_to_database import push_batter_data_to_sql_upsert
import pandas as pd
from datetime import datetime
import pytz

def run_daily_batter_statsapi_update():

    current_year = datetime.now().year

    current_batter_ids = get_current_batter_ids()

    all_current_batters_df_list = []

    for batter in current_batter_ids:
        current_batter_df = get_batter_info_and_stats_season(batter, season=current_year)
        if current_batter_df is None or current_batter_df.empty:
            continue
        all_current_batters_df_list.append(current_batter_df)

    final_batter_df = pd.concat(all_current_batters_df_list)

    final_batter_df['season'] = final_batter_df['season'].astype(int)
    final_batter_df['update_date'] = datetime.now(pytz.timezone("America/New_York"))

    data_table_name = 'batter_seasonal_data_statsapi'

    push_batter_data_to_sql_upsert(data_table_name, final_batter_df)

if __name__ == "__main__":
    run_daily_batter_statsapi_update()
