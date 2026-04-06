import pandas as pd
from data_load_functions.utility_functions import get_engine


def pull_data_from_neon_sql_database(query: str):

    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df

def batter_seasonal_data_statcast():
    query = """
    select *
    from batter_seasonal_data_statcast
    """
    
    df = pull_data_from_neon_sql_database(query)
    
    return df

def batter_seasonal_data_statsapi():
    query = """
    select *
    from batter_seasonal_data_statsapi
    """
    
    df = pull_data_from_neon_sql_database(query)
    
    return df

def pitching_data():
    query = """
    select *
    from pitcher_seasonal_data
    """
    
    df = pull_data_from_neon_sql_database(query)
    
    return df
