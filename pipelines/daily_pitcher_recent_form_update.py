import pandas as pd
from datetime import datetime
import pytz


from data_extract_functions.extract_mlb_games_info import games_today_with_probable_pitchers_ids
from data_extract_functions.extract_player_data import extract_pitcher_recent_form

from data_load_functions.load_data_to_database import push_data_to_sql_replace

def run_starting_pitchers_recent_form_update(game_date=None):
    
    # grab todays games and team lists with lineup ids
    if game_date is None:
        game_date = datetime.today().strftime('%Y-%m-%d')

    starting_pitcher_ids = games_today_with_probable_pitchers_ids(game_date)

    pitcher_df_list = []

    for pitcher in starting_pitcher_ids:
        pitcher_df = extract_pitcher_recent_form(pitcher)
        if pitcher_df is not None:
            pitcher_df_list.append(pitcher_df)
    
    if not pitcher_df_list:
        return

    final_pitcher_df = pd.concat(pitcher_df_list, ignore_index=False)

    final_pitcher_df['update date'] = datetime.now(pytz.timezone("America/New_York"))
    
    table_name = 'starting_pitchers_recent_form'
    
    push_data_to_sql_replace(table_name, final_pitcher_df)

                            
if __name__ == "__main__":
    run_starting_pitchers_recent_form_update()
