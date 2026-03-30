from constants import fact_const_columns
from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError
import pandas as pd
import re
import time



def load_silver(path: str) -> pd.DataFrame:
    """Load transformed data from data/silver"""
    # Accessing transformed (silver) data
    return pd.read_parquet(path)


def clean_column_name(col):
    """clean symbols from columns that would interfere with the databse"""
    col = col.lower()
    col = col.replace("($)", "usd")
    col = col.replace("(%)", "prct")
    col = col.replace("(mt)", "mt")
    col = col.replace("(°c)", "degrees_c")
    col = col.replace("(people)", "people")
    col = col.replace("(t/person)", "t_per_person")
    col = col.replace("(kg/$)", "kg_per_usd")
    col = col.replace("(kg/kwh)", "kg_per_kwh")

    # Replace ANY non-alphanumeric character (like $, %, /, (, ), spaces) with an underscore
    col = re.sub(r'[^a-z0-9]+', '_', col)

    # 3. Clean up: Remove leading/trailing underscores and change '___' to '_'
    col = col.strip('_')
    col = re.sub(r'_+', '_', col)

    return col


def logical_split(df: pd.DataFrame) -> list:
    """Split table according to Gold layer business logic"""

    # Sanitize the column names for SQL query
    df.columns = [clean_column_name(c) for c in df.columns]
    try:
        df_emissions_main = df[fact_const_columns.fact_emissions_main].copy()
        df_co2_consumption_main = df[fact_const_columns.fact_consumption_co2_main].copy()
        df_emission_sources = df[fact_const_columns.fact_emission_sources].copy()
        df_non_ghg = df[fact_const_columns.fact_non_co2_ghg].copy()
        df_climate_impact = df[fact_const_columns.fact_climate_impact].copy()
    except KeyError as e:
        print(f"Still missing columns: {e}")
        print(df.columns)

    return df_emissions_main, df_co2_consumption_main, df_emission_sources, df_non_ghg, df_climate_impact


def gold_filtering(sliced_df: pd.DataFrame) -> pd.DataFrame:
    """Remove Nan values from table"""
    # Using as base the backbone of the columns
    backbone = ["country", "year", "iso_code", "population_people"]
    actual_backbone = [col for col in backbone if col in sliced_df.columns]
    remaining_columns = sliced_df.columns.difference(actual_backbone)
    # Remove lines where everything is Nan except for the backbone.
    filtered_df = sliced_df.dropna(subset=remaining_columns, how='all')
    return filtered_df


def push_to_db(df: pd.DataFrame, table_name: str, engine: Engine, schema: str) -> None:
    """Push table do Postgress Database"""

    # 1. Empty the table but keep the structure and types
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
        conn.commit()

    try:
        print("Starting upload...")
        df.to_sql(
            name=table_name,
            con=engine,
            schema="co2_project",
            if_exists="append",
            index=False,
            chunksize=10000
        )
        print("Success ! Your Gold Layer is now populated with data.")
    except Exception as e:
        print(f"Error: {e}")


def run_query(sql_query: str, engine: Engine, params=None) -> pd.DataFrame:
    """Run SQL query to retrieve data from database"""
    with engine.connect() as conn:
        return pd.read_sql(sql_query, conn, params=params)
    

def get_connection(engine: Engine, max_retries: int=5, retry_delay: int=2):
    """Attempt database connection with retries."""
    for attempt in range(max_retries):
        try:
            conn = engine.connect()
            print("Database connection established")
            return conn
        except OperationalError as e:
            if attempt <  max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect after {max_retries} attempts") from e