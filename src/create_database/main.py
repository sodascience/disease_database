"""Commandline script to compute entire dataset. First run preproc.py"""

import polars as pl
from pathlib import Path
from tqdm import tqdm
import datetime

OUTPUT_FOLDER = Path("processed_data/database")
OUTPUT_FOLDER.mkdir(exist_ok=True)
DISEASES_TABLE = pl.read_excel("raw_data/manual_input/disease_search_terms.xlsx")
LOCATIONS_TABLE = pl.read_excel("raw_data/manual_input/municipalities_1869.xlsx")

print(datetime.datetime.now(), "| Reading data in memory...")
df = pl.read_parquet(
    "processed_data/partitioned/**/*.parquet", allow_missing_columns=True
)
print(datetime.datetime.now(), "| Finished reading data in memory.")

print(datetime.datetime.now(), "| Starting iterations.")

# iteration
iteration = 0
for dis in tqdm(DISEASES_TABLE.iter_rows(named=True), total=len(DISEASES_TABLE)):
    if dis["Include"] == "No":
        print(datetime.datetime.now(), f"| Skipping {dis["Label"]}")
        next
    dis_label = dis["Label"]
    dis_regex = dis["Regex"]
    for loc in tqdm(LOCATIONS_TABLE.iter_rows(named=True), total=len(LOCATIONS_TABLE)):
        loc_label = loc["Municipality"]
        loc_regex = loc["Regex"]
        loc_cbscode = loc["cbscode"]

        (
            df.filter(pl.col("article_text").str.contains("amsterdam"))
            .group_by(["year", pl.col("newspaper_date").dt.month().alias("month")])
            .agg(
                pl.len().alias("n_location"),
                pl.col("article_text").str.contains("cholera").sum().alias("n_both"),
            )
            .with_columns(
                pl.lit(loc_label).alias("municipality"),
                pl.lit(loc_cbscode).alias("cbscode").cast(pl.Int32),
                pl.lit(dis_label).alias("disease"),
            )
            .write_parquet(OUTPUT_FOLDER / f"{iteration:08}.parquet")
        )
        iteration += 1
