"""Commandline script to rechunk data to easier-to-read parquet file. First run main.py"""

import polars as pl
from pathlib import Path
import datetime
from scipy import stats

INPUT_FOLDER = Path("processed_data/database_flat_v1.0")
OUTPUT_FOLDER = Path("processed_data/database_v1.2")
OUTPUT_FOLDER.mkdir(exist_ok=True)

print(datetime.datetime.now(), "| Reading data in memory...")
df = pl.read_parquet(INPUT_FOLDER / "**" / "*.parquet", allow_missing_columns=True)
print(datetime.datetime.now(), "| Finished reading data in memory.")

print(datetime.datetime.now(), "| Creating completed dataset.")
df_full = (
    df
    .select(pl.col(["disease", "year", "month", "cbscode"]).unique().implode())
    .explode("disease")
    .explode("year")
    .explode("month")
    .explode("cbscode")
    .join(df, on=["disease", "year", "month", "cbscode"], how="left")
    .select(["disease", "year", "month", "cbscode", "n_location", "n_both"])
    .fill_null(0)
)
print(datetime.datetime.now(), "| Finished completing dataset.")

print(datetime.datetime.now(), "| Cleaning dataset.")
df_clean = (
    df_full.with_columns(
        pl.when(pl.col("n_location").eq(0)).then(pl.lit(0.0)).otherwise(pl.col("n_both") / pl.col("n_location")).alias("mention_rate")
    )
    .sort(["disease", "year", "month", "cbscode"])
    .with_columns(pl.col("disease").str.to_lowercase().cast(pl.Categorical))
    .select(
        [
            "disease",
            "year",
            "month",
            "cbscode",
            "mention_rate",
            "n_location",
            "n_both",
        ]
    )
)

print(datetime.datetime.now(), "| Writing data.")
df_clean.write_parquet(
    OUTPUT_FOLDER / "disease_database_v1.2.parquet",
    statistics="full",
    # partition_by="disease",
    # partition_chunk_size_bytes=1_000_000_000,
)
