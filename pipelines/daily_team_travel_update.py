from datetime import datetime
from sqlalchemy import text
import pandas as pd

from data_extract_functions.extract_mlb_games_info import extract_todays_games_schedule
from data_extract_functions.extract_data_from_files import team_venue_data

from data_transform_functions.utility_functions import compute_travel_distance_around_earth
from data_load_functions.utility_functions import get_engine

from data_load_functions.load_data_to_database import push_data_to_sql_replace



def run_daily_team_travel_update(game_date=None):
    # grab todays games
    current_team_travel_df = extract_todays_games_schedule(game_date)
    
    if current_team_travel_df is None or current_team_travel_df.empty:
        return
        
    engine = get_engine()
    
    query = """ 
    SELECT * FROM team_travel_data 
    """

    with engine.connect() as conn: 
        past_team_travel_df = pd.read_sql(text(query), conn)
    
    # SHIFT yesterday's current → today's last
    past_team_travel_df['last_game_date'] = past_team_travel_df['current_game_date']
    past_team_travel_df['last_game_time'] = past_team_travel_df['current_game_time']
    past_team_travel_df['last_venue'] = past_team_travel_df['current_venue']
    
    past_team_travel_df = past_team_travel_df.set_index('team_id')
    
    current_team_travel_df = current_team_travel_df.rename(columns={
        'game_datetime': 'current_game_time',
        'game_date': 'current_game_date',
        'venue_name': 'current_venue'
    })
    
    current_team_travel_df['current_venue'] = current_team_travel_df['current_venue'].astype('string')
    
    current_team_travel_df['current_game_time'] = pd.to_datetime(
        current_team_travel_df['current_game_time'], utc=True
    ).dt.tz_localize(None)
    
    current_team_travel_df = current_team_travel_df.set_index('team_id')
    
    past_team_travel_df.update(current_team_travel_df)
    
    past_team_travel_df = past_team_travel_df.reset_index()
    
    updated_team_travel_df = past_team_travel_df
    
    # Fix dtypes after update
    updated_team_travel_df['current_game_date'] = pd.to_datetime(
        updated_team_travel_df['current_game_date'], errors='coerce'
    )
    
    updated_team_travel_df['current_game_time'] = pd.to_datetime(
        updated_team_travel_df['current_game_time'], errors='coerce'
    )


    tz_numeric_map = {
    "America/New_York": 0,     # Eastern
    "America/Toronto": 0,
    "America/Detroit": 0,

    "America/Chicago": -1,     # Central

    "America/Denver": -2,      # Mountain

    "America/Phoenix": -2,     # Arizona (no DST but same offset most of year)

    "America/Los_Angeles": -3  # Pacific
    }

    # Fix dtypes after update
    updated_team_travel_df['current_game_date'] = pd.to_datetime(
        updated_team_travel_df['current_game_date'], errors='coerce'
    )
    
    updated_team_travel_df['current_game_time'] = pd.to_datetime(
        updated_team_travel_df['current_game_time'], errors='coerce'
    )
    
    # update last venue data
    
    updated_team_travel_df['last_venue_timezone'] = (
        updated_team_travel_df['last_venue']
        .map(lambda v: team_venue_data.get(v, {}).get("team_home_timezone"))
    )
    
    updated_team_travel_df['last_venue_longitude'] = (
        updated_team_travel_df['last_venue']
        .map(lambda v: team_venue_data.get(v, {}).get("team_home_venue_longitude"))
    )
    
    updated_team_travel_df['last_venue_latitude'] = (
        updated_team_travel_df['last_venue']
        .map(lambda v: team_venue_data.get(v, {}).get("team_home_venue_latitude"))
    )
    
    updated_team_travel_df['current_venue_timezone'] = (
        updated_team_travel_df['current_venue']
        .map(lambda v: team_venue_data.get(v, {}).get("team_home_timezone"))
    )
    
    updated_team_travel_df['current_venue_longitude'] = (
        updated_team_travel_df['current_venue']
        .map(lambda v: team_venue_data.get(v, {}).get("team_home_venue_longitude"))
    )
    
    updated_team_travel_df['current_venue_latitude'] = (
        updated_team_travel_df['current_venue']
        .map(lambda v: team_venue_data.get(v, {}).get("team_home_venue_latitude"))
    )
    
    # create logic for traveleling
    updated_team_travel_df['traveling'] = (
        updated_team_travel_df['last_venue'] != updated_team_travel_df['current_venue']
    )
    
    updated_team_travel_df['days_since_last_game'] = (
        updated_team_travel_df['current_game_date'] - updated_team_travel_df['last_game_date']
    )
    
    
    # create logic for traveleling
    updated_team_travel_df['traveling'] = (
        updated_team_travel_df['last_venue'] != updated_team_travel_df['current_venue']
    )
    
    updated_team_travel_df['days_since_last_game'] = (
        updated_team_travel_df['current_game_date'] - updated_team_travel_df['last_game_date']
    )
    
    updated_team_travel_df['west_to_east'] = updated_team_travel_df['last_venue_longitude'] < updated_team_travel_df['current_venue_longitude']
    
    updated_team_travel_df['last_tz_num'] = (
        updated_team_travel_df['last_venue_timezone'].map(tz_numeric_map)
    )
    
    updated_team_travel_df['current_tz_num'] = (
        updated_team_travel_df['current_venue_timezone'].map(tz_numeric_map)
    )
    
    updated_team_travel_df['travel_time_zones'] = (
        updated_team_travel_df['current_tz_num'] - updated_team_travel_df['last_tz_num']
    )

    updated_team_travel_df = compute_travel_distance_around_earth(updated_team_travel_df)

    updated_team_travel_df['road_trip_days'] = (
    pd.to_numeric(updated_team_travel_df['road_trip_days'], errors='coerce')
        .fillna(0)
        .astype(int)
    )

    
    
    updated_team_travel_df['road_trip_days'] = (
        updated_team_travel_df['road_trip_days'] +
        (updated_team_travel_df['home_away'] == 'away').astype(int)
    )
    
    updated_team_travel_df.loc[updated_team_travel_df['home_away'] == ' home', 'road_trip_days'] = 0

    updated_team_travel_df['rest_days'] = (
    pd.to_numeric(updated_team_travel_df['rest_days'], errors='coerce')
        .fillna(0)
        .astype(int)
    )


    updated_team_travel_df['days_since_last_game'] = (
    updated_team_travel_df['current_game_date'] - updated_team_travel_df['last_game_date']
    ).dt.days
    
    no_rest = updated_team_travel_df['days_since_last_game'] <= 1
    at_home = updated_team_travel_df['team_home_venue'] == updated_team_travel_df['current_venue']
    rest_condition = (~no_rest) & (at_home)
    updated_team_travel_df.loc[no_rest, 'rest_days'] = 0
    updated_team_travel_df.loc[rest_condition, 'rest_days'] = updated_team_travel_df['days_since_last_game'] - 1

    updated_team_travel_df['fatigue_score'] = (
    updated_team_travel_df['traveling'] * 0.05 +
    updated_team_travel_df['west_to_east'] * 0.05 + 
    updated_team_travel_df['travel_time_zones'] * 0.03 +
    (updated_team_travel_df['travel_distance_km'] / 500 * .03) + 
    updated_team_travel_df['road_trip_days'] * 0.02 -
    updated_team_travel_df['rest_days'] * .03
    ).round(4)

    updated_team_travel_df['update_date'] = datetime.today()
    table_name = 'team_travel_data'
    
    push_data_to_sql_replace(table_name, updated_team_travel_df)


if __name__ == "__main__":
    run_daily_team_travel_update()
