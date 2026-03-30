import pandas as pd

def load_bronze(path: str) -> pd.DataFrame:
    """Load .csv file from data/raw"""
    # -------- Accessing the Raw data
    return pd.read_csv(path)


def get_profile(df: pd.DataFrame, stage_name: str) -> dict:
    """Returns a dictionary with key and metrics for comparison"""
    return {
        "stage": stage_name,
        "rows": df.shape[0],
        "cols": df.shape[1],
        "null_sum": df.isna().sum().sum(),
        "memory_mb": df.memory_usage(deep=True).sum() / (1024 ** 2),
        "dtypes": df.dtypes.value_counts().to_dict()
    }


def data_cleanse(df: pd.DataFrame) -> pd.DataFrame:
    """Perform a first clean on the raw data"""
    # Drop duplicates
    removed_dups = df.drop_duplicates()
    # Drop Data population, year and country are NaN
    backbone = ["country", "year", "population"]
    cleansed_df = removed_dups.dropna(subset=backbone, how="any")
    return cleansed_df


def obtaining_units_from_meta(df: pd.DataFrame) -> pd.DataFrame:
    """Load units from codebook to insert them into raw data columns"""
    units_df = df[["column", "unit"]].copy()
    units_df = units_df.astype({"column": "string", "unit": "string"})

    def extract_units(value):
        if pd.isna(value):
            return value
        if " " in value:
            return value.split(" ")[-1]
        return f"({value})"

    units_df["unit"] = units_df["unit"].apply(extract_units)
    return units_df


def standardizing_data(raw_df: pd.DataFrame, units_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the raw data - Changing types and column names"""
    # -------- Fixing Columns types:
    # Country and Iso_code to String and Year to int32 (for memory efficiency):
    df_type_formated = raw_df.astype(
        {
            "iso_code": "string",
            "country": "string",
            "year": "int32",
            "population": "int64"
        }
    )
    # Making sure all the columns are camel case
    df_formated = df_type_formated.rename(columns=lambda col: col.lower().replace(" ", "_"))

    # 1. Create the new name: "column_unit"
    # Only add if it is not NA
    def build_new_name(row):
        col_name = row["column"]
        unit_suffix = row["unit"]

        if pd.isna(unit_suffix) or unit_suffix == "":
            return col_name
        return f"{col_name}_{unit_suffix}"

    # 2. Generate the mapping dictionary
    units_df["new_column_name"] = units_df.apply(build_new_name, axis=1)
    rename_map = dict(zip(units_df["column"], units_df["new_column_name"]))

    return df_formated.rename(columns=rename_map)


def input_special_isos(df: pd.DataFrame) -> pd.DataFrame:
    """Give fake ISOs to countries that don't have one"""
    # In the dataset there is some elements that do not have iso_codes.
    # Most of them are Aggregate values that already are in the dataset reunited by several organizations
    # But there are two special cases:
    # Kosovo (That doesn't have full diplomatic recognition) - Exists since 2008;
    # The Ryukyu Islands - Integrate Japan since 1879 - This won't be considered for the National Dataframes

    mapping = {"Kosovo": "XKS"}  # Define mapping for the country without ISO
    for country, fake_iso in mapping.items():
        mask = (df["country"] == country) & (df["iso_code"].isna())
        df.loc[mask, "iso_code"] = fake_iso

    return df


def silver_split(final_df: pd.DataFrame) -> tuple:
    """Split data into only country fact emissions, and aggregate fact emissions"""
    # 1. Aggregates DF - Rows with missing ISO
    aggregates_df = final_df[final_df["iso_code"].isna()].copy()

    # 2. National Table (Individual Countries data)
    national_df = final_df[final_df["iso_code"].notna()].copy()
    national_df = national_df[national_df["iso_code"].str.len() == 3]  # Sanity check -> Guarantees ISO_code

    return national_df, aggregates_df


def save_to_silver(df: pd.DataFrame, table_name: str, base_path) -> None:
    """Save transformed data to silver table"""
    path = f"{base_path}/{table_name}.parquet"
    df.to_parquet(path, index=False, compression="snappy")
    print(f"Saved {table_name} to {path} | Rows: {len(df)}")




