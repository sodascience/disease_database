"""Commandline script to rechunk data to easier-to-read parquet file. First run main.py"""

import polars as pl
from pathlib import Path
import datetime

DB_VERSION = "v1.2"
INPUT_FOLDER = Path("processed_data", "database_flat")
OUTPUT_FOLDER = Path("processed_data", "database")
OUTPUT_FOLDER.mkdir(exist_ok=True)


def main():
    print(datetime.datetime.now(), "| Reading data in memory...")
    df = pl.read_parquet(INPUT_FOLDER / "**" / "*.parquet", missing_columns="insert")
    print(datetime.datetime.now(), "| Finished reading data in memory.")

    print(datetime.datetime.now(), "| Creating completed dataset.")
    df_full = (
        df.select(pl.col(["disease", "year", "month", "cbscode"]).unique().implode())
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
            pl.when(pl.col("n_location").eq(0))
            .then(pl.lit(0.0))
            .otherwise(pl.col("n_both") / pl.col("n_location"))
            .alias("mention_rate")
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
        OUTPUT_FOLDER / f"disease_database_{DB_VERSION}.parquet",
        statistics="full",
    )

    print(datetime.datetime.now(), "| Producing yearly dataset.")
    df_yearly = (
        df_clean.group_by(["disease", "year", "cbscode"])
        .agg(pl.col.n_location.sum(), pl.col.n_both.sum())
        .with_columns(
            pl.when(pl.col("n_location").eq(0))
            .then(pl.lit(0.0))
            .otherwise(pl.col("n_both") / pl.col("n_location"))
            .alias("mention_rate")
        )
        .sort(["disease", "year", "cbscode"])
        .select(
            [
                "disease",
                "year",
                "cbscode",
                "mention_rate",
                "n_location",
                "n_both",
            ]
        )
    )

    print(datetime.datetime.now(), "| Writing data.")
    df_yearly.write_parquet(
        OUTPUT_FOLDER / f"disease_database_yearly_{DB_VERSION}.parquet",
        statistics="full",
    )



if __name__ == "__main__":
    main()
