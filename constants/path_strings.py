from pathlib import Path

bronze_path = "/app/data/raw"
silver_path = "/app/data/silver"
url_main = "https://raw.githubusercontent.com/owid/co2-data/a499dd34c1372468f2335a370c5dd13cc3a72d90/owid-co2-data.csv"
metadata_url = "https://raw.githubusercontent.com/owid/co2-data/a499dd34c1372468f2335a370c5dd13cc3a72d90/owid-co2-codebook.csv"
raw_main_path = Path(bronze_path) / "owid_co2_raw_data.csv"
raw_metadata_path = Path(bronze_path) / "owid_co2_codebook.csv"
silver_national_path = Path(silver_path) / "National_table_parquet.parquet"
silver_aggregate_path = Path(silver_path) / "Aggregate_table_parquet.parquet"
gx_location = Path("/app/gx")