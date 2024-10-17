"""This file"""

import polars as pl
from tqdm import tqdm
import argparse
from utils_delpher_api import harvest_article_content
from pathlib import Path

ARTICLE_ID_FOLDER = Path("processed_data", "metadata", "articles", "api_harvest")
ARTICLE_TEXT_FOLDER = Path("processed_data", "texts", "api_harvest")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1880)
    parser.add_argument("--end_year", type=int, default=1940)
    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    ARTICLE_TEXT_FOLDER.mkdir(exist_ok=True)

    for year in tqdm(range(start_year, end_year + 1)):
        for month in tqdm(range(1, 13)):
            out_file_name = f"article_texts_{year}_{month}.parquet"
            out_file_path = ARTICLE_TEXT_FOLDER / out_file_name
            if out_file_path.exists():
                print(f"Data already harvested for {year}-{month}!")
                continue

            # Load the article metadata
            article_meta_df = pl.read_parquet(
                ARTICLE_ID_FOLDER / f"article_meta_{year}_{month}.parquet"
            )
            article_ids = article_meta_df["article_id"].to_list()

            results_ls = []
            for article_id in tqdm(article_ids):
                results = harvest_article_content(article_id)
                results_ls.append(results)

            # The `results` list now contains all harvested articles
            records_df = pl.DataFrame(results_ls)
            records_df.write_parquet(out_file_path)


if __name__ == "__main__":
    main()
