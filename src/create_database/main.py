"""Commandline script to compute entire dataset. First run preproc.py"""

import polars as pl
from pathlib import Path
from tqdm import tqdm
import datetime

OUTPUT_FOLDER = Path("processed_data/database_flat")
OUTPUT_FOLDER.mkdir(exist_ok=True)
DISEASES_TABLE = pl.read_excel("raw_data/manual_input/disease_search_terms.xlsx")
LOCATIONS_TABLE = pl.read_excel("raw_data/manual_input/location_search_terms.xlsx")

# number of characters distance from location to disease mention in text
# set to 0 for infinite distance
CHARDIST = 0


print(datetime.datetime.now(), "| Reading data in memory...")
df = pl.read_parquet(
    "processed_data/partitioned/**/*.parquet", allow_missing_columns=True
)
print(datetime.datetime.now(), "| Finished reading data in memory.")

print(datetime.datetime.now(), "| Starting iterations.")

# iteration
iteration = 0
for loc in tqdm(LOCATIONS_TABLE.iter_rows(named=True), total=len(LOCATIONS_TABLE)):
    loc_label = loc["name"]
    loc_regex = loc["Regex"]
    location_query = rf"(?i){loc_regex}"
    df_loc = df.filter(pl.col("article_text").str.contains(location_query))

    for dis in tqdm(DISEASES_TABLE.iter_rows(named=True), total=len(DISEASES_TABLE), leave=False):
        dis_label = dis["Label"]
        dis_regex = dis["Regex"]
        if CHARDIST != 0:
            # use text proximity in disease
            disease_query = rf"(?i)({dis_regex})(?:.{{0,{CHARDIST}}}{loc_regex})|(?:{loc_regex}.{{0,{CHARDIST}}})({dis_regex})"
        else:
            disease_query = rf"(?i){dis_regex}"

        (
            df_loc.group_by(["year", pl.col("newspaper_date").dt.month().alias("month")])
            .agg(
                pl.len().alias("n_location"),
                pl.col("article_text")
                .str.contains(disease_query)
                .sum()
                .alias("n_both"),
            )
            .with_columns(
                pl.lit(loc_label).alias("location"),
                pl.lit(loc["cbscode"]).alias("cbscode").cast(pl.Int32),
                pl.lit(loc["amsterdamcode"]).alias("amsterdamcode").cast(pl.Int32),
                pl.lit(dis_label).alias("disease").str.to_lowercase(),
            )
            .write_parquet(OUTPUT_FOLDER / f"{iteration:08}.parquet")
        )
        iteration += 1
