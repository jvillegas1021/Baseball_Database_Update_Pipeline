from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def safe_div(n, d):
    return n / d if d > 0 else 0

def subtract_minutes_from_times(time_list, minutes=45):
    updated_times = []
    for t in time_list:
        dt = datetime.combine(datetime.today(), t)
        new_dt = dt - timedelta(minutes=minutes)
        updated_times.append(new_dt.time())
    return updated_times

def filter_relievers(df):
    df = df.copy()
    df = df[df["games_played"] > 0]
    df["IP_per_G"] = df["IP"] / df["games_played"]
    return df[(df["IP_per_G"] < 3.0) & (df["IP"] >= 5)]

def convert_ip(ip_series):
    ip_series = ip_series.astype(float)
    whole = ip_series.astype(int)
    decimal = ip_series - whole
    outs = (decimal * 10).round().astype(int)
    return whole + outs / 3


def compute_travel_distance_around_earth(team_travel_df):
    columns_to_radians = ['last_venue_longitude', 'last_venue_latitude', 'current_venue_longitude',
                          'current_venue_latitude']

    for column in columns_to_radians:
        team_travel_df[column + '_rad'] = np.radians(team_travel_df[column])
    
    # haversine equation
    difference_in_longitudes = team_travel_df['current_venue_longitude_rad'] - team_travel_df['last_venue_longitude_rad']
    difference_in_latitudes = team_travel_df['current_venue_latitude_rad'] - team_travel_df['last_venue_latitude_rad']
    a_value = np.sin(difference_in_longitudes / 2)** 2 + np.cos(team_travel_df['last_venue_latitude_rad']) * np.cos(team_travel_df['current_venue_latitude_rad']) * np.sin(difference_in_latitudes / 2) ** 2
    c_value = 2 * np.arctan2(np.sqrt(a_value), np.sqrt(1 - a_value))
    distance_traveled = c_value * 6378.1

    team_travel_df['travel_distance_km'] = distance_traveled

    return team_travel_df


