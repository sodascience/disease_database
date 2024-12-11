"""Pre-processing combined data into a hive-partitioned dataset"""

import polars as pl
from pathlib import Path
from tqdm import tqdm

BASE_PATH = Path(".")
COMBINED_DATA_FOLDER = Path("processed_data/combined")
OUTPUT_FOLDER = Path("processed_data/partitioned")

(BASE_PATH / OUTPUT_FOLDER).mkdir(exist_ok=True)

pqfiles = (BASE_PATH / COMBINED_DATA_FOLDER).glob("*.parquet")
pbar = tqdm(list(pqfiles))
for file in pbar:
    pbar.set_description(f"Processing {file.name}")
    pl.read_parquet(file).with_columns(
        pl.col("newspaper_date").dt.year().alias("year")
    ).write_parquet(
        OUTPUT_FOLDER, partition_by="year", partition_chunk_size_bytes=1_000_000_000
    )
