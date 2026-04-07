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

    all_team_batting_df_list = []
    all_team_pitching_df_list = []
    
    # test function
    for game_id, game_official_date, team_name, team_id, batter_list, pitcher_list in teams_playing:
        
        team_batting_df = process_team_batting_df(game_id,
                                                  game_official_date,
                                                  team_name,
                                                  team_id,
                                                  batter_list,
                                                  batting_df_statsapi,
                                                  batting_df_statcast)
        
        if team_batting_df is not None:
            all_team_batting_df_list.append(team_batting_df)
            
        
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
            # Update active table
            push_active_team_data_to_sql(
                'active_team_batting_stats',
                active_team_batting_df
            )
    
            # Pull historical
            historical_team_batting_df = pull_data_from_neon_sql_database(
                """SELECT "gamePk", "team_id" FROM historical_team_batting_stats"""
            )
    
            # Build indexes
            active_idx = active_team_batting_df.set_index(['gamePk', 'team_id']).index
            historical_idx = historical_team_batting_df.set_index(['gamePk', 'team_id']).index
    
            # Find new rows
            new_idx = active_idx.difference(historical_idx)
    
            # Extract only new rows
            complete_historical_batting_df = (
                active_team_batting_df
                .set_index(['gamePk', 'team_id'])
                .loc[new_idx]
                .reset_index()
            )
    
            # Push only if non-empty
            if not complete_historical_batting_df.empty:
                push_historical_team_data_to_sql(
                    'historical_team_batting_stats',
                    complete_historical_batting_df
                )

        

    if all_team_pitching_df_list:

        active_team_pitching_df = pd.concat(all_team_pitching_df_list, ignore_index=True)
    
        if not active_team_pitching_df.empty:
            push_active_team_data_to_sql(
                'active_team_pitching_stats',
                active_team_pitching_df
            )
    
            historical_team_pitching_df = pull_data_from_neon_sql_database(
                """SELECT "gamePk", "team_id" FROM historical_team_pitching_stats"""
            )
    
            active_idx = active_team_pitching_df.set_index(['gamePk', 'team_id']).index
            historical_idx = historical_team_pitching_df.set_index(['gamePk', 'team_id']).index
    
            new_idx = active_idx.difference(historical_idx)
    
            complete_historical_pitching_df = (
                active_team_pitching_df
                .set_index(['gamePk', 'team_id'])
                .loc[new_idx]
                .reset_index()
            )
    
            if not complete_historical_pitching_df.empty:
                push_historical_team_data_to_sql(
                    'historical_team_pitching_stats',
                    complete_historical_pitching_df
                )

if __name__ == "__main__":
    run_daily_roster_update()

        
