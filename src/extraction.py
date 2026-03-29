# --------- Extract the raw data out of GitHub repo: https://github.com/owid/co2-data
from constants import path_strings
import pandas as pd


def extract(url: str, url_meta: str):
    """Extract data from url and save it as csv file"""
    raw_df = pd.read_csv(url)
    raw_df.to_csv(path_strings.raw_main_path, index=False)

    raw_meta = pd.read_csv(url_meta)
    raw_meta.to_csv(path_strings.raw_metadata_path, index=False)
