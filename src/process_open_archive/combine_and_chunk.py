"""
Combine the different extracted files and create a chunked parquet
folder for performing data analysis.
"""

import polars as pl
from tqdm import tqdm
from pathlib import Path
import argparse


ARTICLE_TEXT_FOLDER = Path("processed_data", "texts", "open_archive")
ARTICLE_META_FOLDER = Path("processed_data", "metadata", "articles", "open_archive")
NEWSPAPER_META_FOLDER = Path("processed_data", "metadata", "newspapers", "open_archive")

OUTPUT_FOLDER = Path("processed_data", "combined")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1830)
    parser.add_argument("--end_year", type=int, default=1879)

    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    print(f"Combining and chunking from {start_year} to {end_year}.")

    # write to chunked parquet files
    year_chunksize = 1
    for start_year in tqdm(range(start_year, end_year, year_chunksize)):
        out_path = (
            OUTPUT_FOLDER
            / f"combined_{start_year}_{start_year + year_chunksize}.parquet"
        )
        if out_path.exists():
            print(f"\n {out_path} already exists! Skipping...")
            continue

        article_text_df = pl.scan_parquet(
            ARTICLE_TEXT_FOLDER / f"article_texts_{str(start_year)[:-1]}*.parquet"
        )
        article_meta_df = pl.scan_parquet(
            ARTICLE_META_FOLDER / f"article_meta_{str(start_year)[:-1]}*.parquet"
        )
        newspaper_meta_df = pl.scan_parquet(
            NEWSPAPER_META_FOLDER / f"newspaper_meta_{str(start_year)[:-1]}*.parquet"
        )

        # article_text_df.head().collect()
        # article_meta_df.head().collect()
        # newspaper_meta_df.head().collect()

        # create master df with everything needed
        final_df = article_meta_df.join(
            article_text_df,
            on="article_id",
            how="left",
        ).join(newspaper_meta_df, on="newspaper_id", how="left")

        final_df.filter(
            pl.col("newspaper_date").dt.year() >= start_year,
            pl.col("newspaper_date").dt.year() < end_year,
        ).sink_parquet(out_path, engine="streaming")


if __name__ == "__main__":
    main()
