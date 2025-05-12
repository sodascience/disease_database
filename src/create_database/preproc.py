"""Pre-processing combined data into a hive-partitioned dataset"""

import polars as pl
from pathlib import Path
from tqdm import tqdm

COMBINED_DATA_FOLDER = Path("processed_data", "combined")
OUTPUT_FOLDER = Path("processed_data", "partitioned")


def main():
    # create partitioned data folder
    OUTPUT_FOLDER.mkdir(exist_ok=True)

    # for each parquet file, create partitioned data by year
    pqfiles = COMBINED_DATA_FOLDER.glob("*.parquet")
    pbar = tqdm(list(pqfiles))
    for file in pbar:
        pbar.set_description(f"Processing {file.name}")
        pl.read_parquet(file).with_columns(
            pl.col("newspaper_date").dt.year().alias("year")
        ).write_parquet(
            OUTPUT_FOLDER, partition_by="year", partition_chunk_size_bytes=1_000_000_000
        )


if __name__ == "__main__":
    main()
