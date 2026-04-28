from datetime import datetime
import pandas as pd

from data_extract_functions.extract_mlb_games_info import games_today_with_teams_and_lineups_and_bullpens
from data_extract_functions.extract_data_from_database import batter_seasonal_data_statsapi, batter_seasonal_data_statcast, pitcher_seasonal_data_statcast, pitcher_seasonal_data_statsapi, pull_data_from_neon_sql_database

from data_transform_functions.process_functions import process_team_batting_df, process_team_pitching_df

from data_load_functions.load_data_to_database import push_active_team_data_to_sql, push_historical_team_data_to_sql


def run_daily_roster_update(game_date=None):
    
    # grab todays games and team lists with lineup ids
    if game_date is None:
        game_date = datetime.today().strftime('%Y-%m-%d')
    

    teams_playing = games_today_with_teams_and_lineups_and_bullpens(game_date)

    if not teams_playing:
        return


    batting_df_statsapi = batter_seasonal_data_statsapi()
    batting_df_statcast = batter_seasonal_data_statcast()
    
    pitching_df_statsapi = pitcher_seasonal_data_statsapi()
    pitching_df_statcast = pitcher_seasonal_data_statcast() 

    try:
        historical_team_batting_df = pull_data_from_neon_sql_database(
            """SELECT "gamePk", "team_id" FROM historical_team_batting_stats"""
            )
    except Exception:
        historical_team_batting_df = pd.DataFrame(columns=["gamePk", "team_id"])
    
    historical_batting_idx = set(
        tuple(x) for x in historical_team_batting_df[['gamePk','team_id']].values
    )

    try:
        historical_team_pitching_df = pull_data_from_neon_sql_database(
            """SELECT "gamePk", "team_id" FROM historical_team_pitching_stats"""
        )
    except Exception:
        historical_team_pitching_df = pd.DataFrame(columns=["gamePk", "team_id"])
    
    historical_pitching_idx = set(
        tuple(x) for x in historical_team_pitching_df[['gamePk','team_id']].values
    )


    all_team_batting_df_list = []
    all_team_pitching_df_list = []
    
    # test function
    for game_id, game_official_date, team_name, team_id, batter_list, pitcher_list in teams_playing:

        batting_completed = (game_id, team_id) in historical_batting_idx
        pitching_completed = (game_id, team_id) in historical_pitching_idx
        
        if batting_completed and pitching_completed:
            continue

        if not batting_completed:
            
            team_batting_df = process_team_batting_df(game_id,
                                                      game_official_date,
                                                      team_name,
                                                      team_id,
                                                      batter_list,
                                                      batting_df_statsapi,
                                                      batting_df_statcast)
        
            if team_batting_df is not None:
                all_team_batting_df_list.append(team_batting_df)
            
        if not pitching_completed :
            
            team_pitching_df = process_team_pitching_df(game_id,
                                                        game_official_date,
                                                        team_name,
                                                        team_id,
                                                        pitcher_list,
                                                        pitching_df_statsapi,
                                                        pitching_df_statcast)

            if team_pitching_df is not None:
                all_team_pitching_df_list.append(team_pitching_df)

    
    if all_team_batting_df_list:
        active_team_batting_df = pd.concat(all_team_batting_df_list, ignore_index=True)
    
        if not active_team_batting_df.empty:
            push_active_team_data_to_sql(
                'active_team_batting_stats',
                active_team_batting_df
            )
    
            # Everything here is new → push directly to historical
            push_historical_team_data_to_sql(
                'historical_team_batting_stats',
                active_team_batting_df
            )

    

    if all_team_pitching_df_list:
        active_team_pitching_df = pd.concat(all_team_pitching_df_list, ignore_index=True)
    
        if not active_team_pitching_df.empty:
            push_active_team_data_to_sql(
                'active_team_pitching_stats',
                active_team_pitching_df
            )

            push_historical_team_data_to_sql(
                'historical_team_pitching_stats',
                active_team_pitching_df
            )
    

if __name__ == "__main__":
    run_daily_roster_update()

        
