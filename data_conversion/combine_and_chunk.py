"""
Combine the different extracted files and create a chunked parquet
folder for performing data analysis.
"""

import polars as pl
from tqdm import tqdm
import argparse
from pathlib import Path

ARTICLE_TEXT_FOLDER = Path("processed_data", "texts")
ARTICLE_META_FOLDER = Path("processed_data", "metadata", "articles")
JOURNAL_META_FOLDER = Path("processed_data", "metadata", "newspapers")
OUT_FOLDER = Path("processed_data", "combined")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1830)
    parser.add_argument("--end_year", type=int, default=1879)

    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    article_text_df = (
        pl.scan_parquet(ARTICLE_TEXT_FOLDER / "*.parquet")
        .with_columns(
            pl.col("file_name")
            .str.split_exact("_", 2)
            .struct.rename_fields(["ddd", "journal_id", "article_num"])
            .alias("fields")
        )
        .unnest("fields")
        .with_columns(
            (pl.col("journal_id") + "_" + pl.col("article_num")).alias("article_id")
        )
        .select("journal_id", "article_id", "title", "text")
    )

    # for article meta
    article_meta_df = (
        pl.scan_parquet(ARTICLE_META_FOLDER / "*.parquet")
        .with_columns(
            pl.col("item_filename")
            .str.split_exact("_", 2)
            .struct.rename_fields(["ddd", "journal_id", "article_num"])
            .alias("fields")
        )
        .unnest("fields")
        .with_columns(
            (pl.col("journal_id") + "_" + pl.col("article_num")).alias("article_id")
        )
        .filter(pl.col("item_filename").is_not_null())
        .select("journal_id", "article_id", "item_subject", "item_type")
    )

    # for journal meta
    journal_meta_df = (
        pl.scan_parquet(JOURNAL_META_FOLDER / "*.parquet")
        .with_columns(
            pl.col("newspaper_id")
            .str.split_exact(":", 1)
            .struct.rename_fields(["ddd", "journal_id"])
            .alias("fields")
        )
        .unnest("fields")
        .with_columns(pl.col("newspaper_date").alias("date"))
        .select(pl.exclude("newspaper_id"))
        .select("journal_id", "date", pl.selectors.starts_with("newspaper"), "pdf_link")
    )

    article_text_df.head().collect()
    article_meta_df.head().collect()
    journal_meta_df.head().collect()

    # create master df with everything needed
    final_df = article_text_df.join(
        article_meta_df.select(pl.exclude("journal_id")),
        on="article_id",
        how="left",
    ).join(journal_meta_df, on="journal_id", how="left")

    # write to chunked parquet files
    year_chunksize = 1
    for start_year in tqdm(range(start_year, end_year, year_chunksize)):
        end_year = start_year + year_chunksize
        final_df.filter(
            pl.col("newspaper_date").dt.year() >= start_year,
            pl.col("newspaper_date").dt.year() < end_year,
        ).sink_parquet(OUT_FOLDER / f"combined_{start_year}_{end_year}.parquet")

if __name__ == "__main__":
    main()
