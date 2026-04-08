from data_extract_functions.extract_data_from_database import pitcher_seasonal_data_statcast, pitcher_seasonal_data_statsapi
from data_transform_functions.process_functions import process_starting_pitcher_current_year_stats
from data_load_functions.load_data_to_database import push_pitcher_data_to_sql_upsert_player_id


def run_daily_pitcher_current_season_stats_update():
    #extract
    pitcher_statsapi = pitcher_seasonal_data_statsapi()
    pitcher_statcast = pitcher_seasonal_data_statcast()
    #transform
    pitcher_current_year_stats_df = process_starting_pitcher_current_year_stats(pitcher_statsapi, pitcher_statcast)
    #load
    table_name = 'active_pitcher_stats_current_year'
    push_pitcher_data_to_sql_upsert_player_id(table_name, pitcher_current_year_stats_df)

if __name__ == "__main__":
    run_daily_pitcher_current_season_stats_update()