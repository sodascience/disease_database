"""Commandline script to transform hive data to summary parquet file. First run main.py"""

import polars as pl
from pathlib import Path
from tqdm import tqdm
import datetime
from scipy.stats import beta
import numpy as np


INPUT_FOLDER = Path("processed_data/database")
OUTPUT_FOLDER = Path("processed_data/database_pq")
OUTPUT_FOLDER.mkdir(exist_ok=True)

print(datetime.datetime.now(), "| Reading data in memory...")
df = pl.read_parquet("processed_data/database/**/*.parquet", allow_missing_columns=True)
print(datetime.datetime.now(), "| Finished reading data in memory.")


def compute_binomial_interval(
    successes: pl.Series, tries: pl.Series, alpha: float = 0.95
):
    "Function to compute Jeffrey's binomial interval for a series of successes and tries"
    return pl.DataFrame(
        np.stack(
            [
                beta(s + 0.5, t + 0.5).ppf([(1 - alpha) / 2, 1 - (1 - alpha) / 2])
                for s, t in zip(successes, tries)
            ]
        ),
        schema=["lower", "upper"],
    )


df.group_by(["year", "municipality", "disease"]).agg((pl.col("n_both")/pl.col("n_location")).mean().alias("pressure"), pl.col("cbscode").first()).select(["year", "municipality", "cbscode", "disease", "pressure"]).sort(["year", "municipality"])