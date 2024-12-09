"""Commandline script to compute entire dataset. First run preproc.py"""

import polars as pl
from pathlib import Path
from tqdm import tqdm


OUTPUT_FOLDER = Path("processed_data/database")
OUTPUT_FOLDER.mkdir(exist_ok=True)
DISEASES_TABLE = pl.read_excel("raw_data/manual_input/disease_search_terms.xlsx")
LOCATIONS_TABLE = pl.read_excel("raw_data/manual_input/municipalities_1869.xlsx")

lf = pl.scan_parquet("processed_data/partitioned/", hive_partitioning=True)

def query_disease_location_year(
    df_lazy: pl.LazyFrame, disease: str, location: str, year: int
):
    return (
        df_lazy.filter(
            pl.col("newspaper_date").dt.year() >= year,
            pl.col("newspaper_date").dt.year() <= year,
            pl.col("article_text").str.contains(location),
        )
        .with_columns(
            pl.col("article_text").str.contains(disease).alias("disease"),
        )
        .sort(pl.col("newspaper_date"))
        .with_columns(
            pl.col("newspaper_date").dt.year().alias("yr"),
            pl.col("newspaper_date").dt.month().alias("mo"),
        )
        .group_by(["yr", "mo"])
        .agg(
            pl.len().alias("n_location"),
            pl.col("disease").sum().alias("n_both"),
        )
        .collect()
    )

for yr in tqdm(range(1830, 1838)):
    for dis in tqdm(
        DISEASES_TABLE.iter_rows(named=True), total=len(DISEASES_TABLE), leave=False
    ):
        if dis["Include"] == "No":
            next
        dis_label = dis["Disease"]
        dis_regex = dis["Regex"]
        for loc in tqdm(
            LOCATIONS_TABLE.iter_rows(named=True),
            total=len(LOCATIONS_TABLE),
            leave=False,
        ):
            loc_label = loc["Municipality"]
            loc_regex = loc["Regex"]
            loc_cbscode = loc["cbscode"]
            query_disease_location_year(lf, dis_regex, loc_regex, yr).with_columns(
                pl.lit(loc_label).alias("municipality"),
                pl.lit(dis_label).alias("disease"),
            ).write_parquet(OUTPUT_FOLDER, partition_by=["yr", "disease", "municipality"])
