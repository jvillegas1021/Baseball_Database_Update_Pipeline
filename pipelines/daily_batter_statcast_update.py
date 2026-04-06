from data_transform_functions.process_functions import compute_count_stats_batter_statcast
from data_load_functions.load_data_to_database import push_batter_data_to_sql_upsert

from pybaseball import statcast
import pandas as pd
from datetime import datetime
import pytz

def run_daily_batter_statcast_update():

    current_year = datetime.now().year

    # grab statcast data FOR NOW CSV LATER DATA LAKE # THIS YEAR 2026 DATES = "2026-03-01", "2026-11-30"
    statcast_data = statcast("2026-03-01", "2026-11-30")

    final_batter_df = compute_count_stats_batter_statcast(statcast_data)
    
    final_batter_df['update_date'] = datetime.now(pytz.timezone("America/New_York"))

    data_table_name = 'batter_seasonal_data_statcast'

    push_batter_data_to_sql_upsert(data_table_name, final_batter_df)

if __name__ == "__main__":
    run_daily_batter_statcast_update()