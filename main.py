from constants import path_strings
from pathlib import Path
import src.extraction as ext
import src.transformation as trf


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


if __name__ == "__main__":
    main()
