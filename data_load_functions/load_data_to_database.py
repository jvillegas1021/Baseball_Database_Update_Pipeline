from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

from data_load_functions.utility_functions import get_engine


def push_active_team_data_to_sql(table_name: str, team_data_df: pd.DataFrame):
    
    engine = get_engine()

    # 1. Create table if it doesn't exist
    team_data_df.head(0).to_sql(table_name, con=engine, if_exists="append", index=False)

    # 2. Reflect table
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # 3. UPSERT each row
    with engine.begin() as conn:
        for _, row in team_data_df.iterrows():
            stmt = insert(table).values(row.to_dict())
            stmt = stmt.on_conflict_do_update(
                index_elements=['team_id'],   # your unique key
                set_=row.to_dict()           # update all columns
            )
            conn.execute(stmt)

def push_historical_team_data_to_sql(table_name: str, team_data_df: pd.DataFrame):
    engine = get_engine()

    # 1. Create table if it doesn't exist
    team_data_df.head(0).to_sql(table_name, con=engine, if_exists="append", index=False)

    # 2. Reflect table
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    # 3. UPSERT each row
    with engine.begin() as conn:
        for _, row in team_data_df.iterrows():
            stmt = insert(table).values(row.to_dict())
            stmt = stmt.on_conflict_do_update(
                index_elements=['gamePk', 'team_id'],   # your unique key
                set_=row.to_dict()           # update all columns
            )
            conn.execute(stmt)

def push_data_to_sql_replace(table_name: str, data_df: pd.DataFrame):
    engine = get_engine()
    
    data_df.to_sql(table_name, con=engine, if_exists="replace", index=False)

def push_data_to_sql_append(table_name: str, data_df: pd.DataFrame):
    engine = get_engine()
    
    data_df.to_sql(table_name, con=engine, if_exists="append", index=False)
