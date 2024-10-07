import polars as pl
from tqdm import tqdm
import os
import argparse
from utils_delpher_api import harvest_article_content

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1880)
    parser.add_argument("--end_year", type=int, default=1940)
    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    in_folder_path = f"processed_data/metadata/articles/from_1880_to_1940/"
    out_folder_path = f"processed_data/texts/from_{start_year}_to_{end_year}/"
    if os.path.exists(out_folder_path) is False:
        os.makedirs(out_folder_path)

    for year in tqdm(range(start_year, end_year+1)):
        for month in range(1, 13):
            out_file_name = f"article_texts_{year}_{month}.parquet"
            out_file_path = out_folder_path+out_file_name
            if os.path.exists(out_file_path):
                print(f"Data already harvested for {year}-{month}!")
                continue

            # Load the article metadata
            article_meta_df = pl.scan_parquet(in_folder_path+f"article_meta_{year}_{month}.parquet")
            article_ids = article_meta_df.collect()["article_id"].to_list()

            # Parallel harvesting using ThreadPoolExecutor
            results_ls = []
            for article_id in tqdm(article_ids):
                results = harvest_article_content(article_id)
                results_ls.append(results)

            # The `results` list now contains all harvested articles
            records_df = pl.DataFrame(results_ls)
            records_df.write_parquet(out_file_path)

if __name__ == "__main__":
    main()
