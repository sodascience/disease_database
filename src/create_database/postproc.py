"""Commandline script to rechunk data to easier-to-read parquet file. First run main.py"""

import polars as pl
from pathlib import Path
from tqdm import tqdm
import datetime
from scipy import stats
import numpy as np


INPUT_FOLDER = Path("processed_data/database")
OUTPUT_FOLDER = Path("processed_data/database_pq")
OUTPUT_FOLDER.mkdir(exist_ok=True)

print(datetime.datetime.now(), "| Reading data in memory...")
df = pl.read_parquet(INPUT_FOLDER / "**" / "*.parquet", allow_missing_columns=True)
print(datetime.datetime.now(), "| Finished reading data in memory.")

dists = stats.beta(df["n_both"] + 0.5, df["n_location"] + 0.5)
df_clean = (
    df.with_columns(
        (pl.col("n_both") / pl.col("n_location")).alias("normalized_mentions")
    )
    .with_columns(lower=dists.ppf(0.025), upper=dists.ppf(0.975))
    .with_columns(
        pl.when(pl.col("n_both") == 0)
        .then(0)
        .otherwise(pl.col("lower"))
        .alias("lower"),
        pl.when(pl.col("normalized_mentions") == 1)
        .then(1)
        .otherwise(pl.col("upper"))
        .alias("upper"),
    )
    .sort(["disease", "year", "month", "municipality"])
    .with_columns(pl.col("disease").str.to_lowercase())
    .select(
        [
            "disease",
            "year",
            "month",
            "municipality",
            "cbscode",
            "normalized_mentions",
            "lower",
            "upper",
            "n_location",
            "n_both",
        ]
    )
)

df_clean.write_parquet(
    OUTPUT_FOLDER,
    statistics="full",
    partition_by="disease",
    partition_chunk_size_bytes=1_000_000_000,
)


df_clean.filter(pl.col("municipality") == "Amsterdam", pl.col("disease") == "cholera")
