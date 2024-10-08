"""
Combine the different extracted files and create a chunked parquet
folder for performing data analysis.
"""
import polars as pl
from tqdm import tqdm
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1830)
    parser.add_argument("--end_year", type=int, default=1879)

    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    out_folder = "processed_data/combined"

    article_text_df = pl.scan_parquet(f"processed_data/texts/from_{start_year}_to_{end_year}/*.parquet")
    article_meta_df = pl.scan_parquet(f"processed_data/metadata/articles/from_{start_year}_to_{end_year}/*.parquet")
    newspaper_meta_df = pl.scan_parquet(f"processed_data/metadata/newspapers/from_{start_year}_to_{end_year}/*.parquet")

    article_text_df.head().collect()
    article_meta_df.head().collect()
    newspaper_meta_df.head().collect()

    # create master df with everything needed
    final_df = article_meta_df.join(
        article_text_df,
        on="article_id",
        how="left",
    ).join(newspaper_meta_df, on="newspaper_id", how="left")

    # write to chunked parquet files
    year_chunksize = 1
    for start_year in tqdm(range(start_year, end_year, year_chunksize)):
        end_year = start_year + year_chunksize
        final_df.filter(
            pl.col("newspaper_date").dt.year() >= start_year, pl.col("newspaper_date").dt.year() < end_year
        ).sink_parquet(f"{out_folder}/combined_{start_year}_{end_year}.parquet")


    df_final = pl.scan_parquet("processed_data/combined/*.parquet")
    df_final.head().collect()
    df_final.collect_schema()

if __name__ == "__main__":
    main()