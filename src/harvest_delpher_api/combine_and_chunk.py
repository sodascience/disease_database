"""
Combine the different extracted files and create a chunked parquet
folder for performing data analysis.
"""

import polars as pl
from tqdm import tqdm
from pathlib import Path
import argparse

ARTICLE_TEXT_FOLDER = Path("processed_data", "texts", "api_harvest")
ARTICLE_META_FOLDER = Path("processed_data", "metadata", "articles", "api_harvest")
NEWSPAPER_META_FOLDER = Path("processed_data", "metadata", "newspapers", "api_harvest")

OUTPUT_FOLDER = Path("processed_data", "combined")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1900)
    parser.add_argument("--end_year", type=int, default=1919)

    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    print(f"Combining and chunking from {start_year} to {end_year}.")

        # read chunks and write to chunked parquet files
    year_chunksize = 1
    for start_year in tqdm(range(start_year, end_year, year_chunksize)):
        out_path = (
            OUTPUT_FOLDER
            / f"combined_{start_year}_{start_year + year_chunksize}.parquet"
        )
        if out_path.exists():
            print(f"\n {out_path} already exists! Skipping...")
            continue

        # Check if the required parquet files exist
        article_text_file_path = ARTICLE_TEXT_FOLDER / f"article_texts_{start_year}*.parquet"
        article_meta_file_path = ARTICLE_META_FOLDER / f"article_meta_{start_year}*.parquet"
        newspaper_meta_file_path = NEWSPAPER_META_FOLDER / f"newspaper_meta_{start_year}*.parquet"

        if not any(Path(article_text_file_path.parent).glob(article_text_file_path.name)) or \
           not any(Path(article_meta_file_path.parent).glob(article_meta_file_path.name)) or \
           not any(Path(newspaper_meta_file_path.parent).glob(newspaper_meta_file_path.name)):
            print(f"\n Required parquet files for year {start_year} do not exist. Skipping...")
            continue

        # Read the parquet files
        article_text_df = pl.scan_parquet(article_text_file_path)
        article_meta_df = pl.scan_parquet(article_meta_file_path)
        newspaper_meta_df = pl.scan_parquet(newspaper_meta_file_path)

        # create master df with everything needed
        final_df = article_meta_df.join(
            article_text_df,
            on="article_id",
            how="left",
        ).join(newspaper_meta_df, on="newspaper_id", how="left")

        final_df.filter(
            pl.col("newspaper_date").dt.year() >= start_year,
            pl.col("newspaper_date").dt.year() < end_year,
        ).sink_parquet(out_path)


if __name__ == "__main__":
    main()
