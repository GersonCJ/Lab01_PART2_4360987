from constants import path_strings
from data_quality import gx
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
import os
import src.extraction as ext
import src.load as ld
import src.transformation as trf
import urllib.parse

load_dotenv()

def main():

    # Path and requirements

    url = path_strings.url_main
    url_metadata = path_strings.metadata_url
    bronze_path = Path(path_strings.bronze_path)
    silver_path = Path(path_strings.silver_path)

    # ------------------ Extraction using commit a499dd34c1372468f2335a370c5dd13cc3a72d90
    
    if not any(bronze_path.iterdir()):
        print("Starting extraction ...")
        ext.extract(url, url_metadata)
    else:
        print("Data already available. Skipping extraction ...")

    raw_data = trf.load_bronze(path_strings.raw_main_path)
    metadata = trf.load_bronze(path_strings.raw_metadata_path)

    # Validation Layer on raw data

    gx.run_validation()

    # ------------------- Transformations

    # 1. Drop duplicates and Drop Data where Country, Year and Population are Nan.
    cleaned_data = trf.data_cleanse(raw_data)

    # 2. Standardize data - Correct types, add to columns the units from metadata
    standardized_data = trf.standardizing_data(
        cleaned_data, trf.obtaining_units_from_meta(metadata)
    )

    # 3. Give Kosovo a Fake Iso, so it is included with the other nations in the Nations dataframe.
    before_split_df = trf.input_special_isos(standardized_data)

    # 4. Split the dataset into Two Datasets - One containing the aggregates, One containing only countries.
    national_df, aggregate_df = trf.silver_split(before_split_df)

    # 5. If no data in data/silver, save both the national and aggregate dfs in the silver_path in parquet, otherwise skip.
    if not any(silver_path.iterdir()):
        print("Starting to save ...")
        trf.save_to_silver(national_df, "National_table_parquet", silver_path)
        trf.save_to_silver(aggregate_df, "Aggregate_table_parquet", silver_path)
    else:
        print("Data already available. Saving skipped ...")

    # ------------------- Gold Layer Load

    # 1. Load both of the Parquets file
    national_silver_df = ld.load_silver(path_strings.silver_national_path)
    aggregate_silver_df = ld.load_silver(path_strings.silver_aggregate_path)

    # 2. Make the Logical Split of the Datasets

    emissions_main, consumptions_main, emission_sources_main, non_co2_ghg_main, climate_impact_main = ld.logical_split(
        national_silver_df
    )
    emissions_agg, consumptions_agg, emission_sources_agg, non_co2_ghg_agg, climate_impact_agg = ld.logical_split(
        aggregate_silver_df
    )

    tables_to_filter = {
        "fact_emissions": emissions_main,
        "fact_consumption": consumptions_main,
        "fact_emission_sources": emission_sources_main,
        "fact_non_co2_ghg": non_co2_ghg_main,
        "fact_climate_impact": climate_impact_main,
        "agg_emissions": emissions_agg,
        "agg_consumption": consumptions_agg,
        "agg_emission_sources": emission_sources_agg,
        "agg_non_co2_ghg": non_co2_ghg_agg,
        "agg_climate_impact": climate_impact_agg
    }

    filtered_gold_tables = {}

    for table_name, df in tables_to_filter.items():
        print(f"Filtering {table_name}...")
        filtered_gold_tables[table_name] = ld.gold_filtering(df)

    # 3. Get .env endpoints

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # 4. Encode the password to handle special characters (@, !, #, etc...)
    encoded_password = urllib.parse.quote_plus(password)

    # 5. Create the engine to connect to Postgres Database

    engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}")

    # 6. Push filtered DFs to Postgres databases

    for table_name, df in filtered_gold_tables.items():
        ld.push_to_db(df, table_name, engine, schema="co2_project")

    # 7. Query test
    query_test = """SELECT * FROM co2_project.fact_emissions LIMIT 5;"""
    print(ld.run_query(query_test, engine))


if __name__ == "__main__":
    main()
