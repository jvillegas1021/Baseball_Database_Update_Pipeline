from data_transform_functions.process_functions import compute_count_stats_pitcher_statcast
from data_load_functions.load_data_to_database import push_pitcher_data_to_sql_upsert

from pybaseball import statcast
import pandas as pd
from datetime import datetime
import pytz

def run_daily_pitcher_statcast_update():

    current_year = datetime.now().year

    # grab statcast data FOR NOW CSV LATER DATA LAKE # THIS YEAR 2026 DATES = "2026-03-01", "2026-11-30"
    statcast_data = statcast("2026-03-01", "2026-11-30")

    final_pitcher_df = compute_count_stats_pitcher_statcast(statcast_data)
    
    final_pitcher_df['update_date'] = datetime.now(pytz.timezone("America/New_York"))

    data_table_name = 'pitcher_seasonal_data_statcast'

    push_pitcher_data_to_sql_upsert(data_table_name, final_pitcher_df)

if __name__ == "__main__":
    run_daily_pitcher_statcast_update()