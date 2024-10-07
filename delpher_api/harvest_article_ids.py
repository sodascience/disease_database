"""
In this file:
Querying the search API of delpher
"""

from tqdm import tqdm
import polars as pl
import os
import argparse
from utils_delpher_api import harvest_article_ids, standardize_values, find_num_records

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1880)
    parser.add_argument("--end_year", type=int, default=1940)
    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    start_month = 1
    end_month = 12
    query = "*"
    max_records = 1000
    columns_to_keep_and_rename = {"OaiPmhIdentifier": "newspaper_id",
                                  "identifier": "article_id",
                                  "type": "article_subject"}

    for year in tqdm(range(start_year, end_year+1)):
        for month in range(start_month, end_month+1):
            output_path = f"processed_data/metadata/articles/from_{start_year}_to_{end_year}/article_meta_{year}_{month}.parquet"
            if os.path.exists(output_path):
                print(f"Data already harvested for {year}-{month}!")
                continue

            records_ls = []
            try:
                total_num_records = find_num_records(query=query,
                                                     year=year,
                                                     month=month)

            except Exception as e:
                print(f"For {year}-{month}: An error occurred: {str(e)}")
                continue

            n_pagination = int(total_num_records/max_records) + 1
            for i in tqdm(range(n_pagination)):
                try:
                    records = harvest_article_ids(query=query,
                                               year=year,
                                               month=month,
                                               max_records=max_records,
                                               start_record=i*max_records+1)
                    records = list(records)
                    records = [standardize_values(record) for record in records]

                    records_ls.extend(records)

                except Exception as e:
                    print(f"For {year}-{month}: An error occurred: {str(e)}")
                    continue

            # Convert the results to a Polars DataFrame
            records_df = ((pl.DataFrame(records_ls)
                          .select(list(columns_to_keep_and_rename.keys())))
                          .rename(columns_to_keep_and_rename))
            records_df = records_df.with_columns(
                pl.col("article_id").str.replace(r"http://resolver.kb.nl/resolve\?urn=", ""),
                pl.col("article_id").str.replace(r":ocr", ""),
                pl.col("newspaper_id").str.to_lowercase()
            )
            # Save the DataFrame as a Parquet file
            records_df.write_parquet(output_path)

            print(f"Data harvesting completed for {year}-{month}!")

if __name__ == "__main__":
    main()