"""
Combine the different extracted files and create a chunked parquet
folder for performing data analysis.
"""
import polars as pl
from tqdm import tqdm

out_folder = "processed_data/combined"

article_text_df = pl.scan_parquet("processed_data/texts/*.parquet")
article_meta_df = pl.scan_parquet("processed_data/metadata/articles/*.parquet")
newspaper_meta_df = pl.scan_parquet("processed_data/metadata/newspapers/*.parquet")

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
for startyr in tqdm(range(1830, 1840, year_chunksize)):
    endyr = startyr + year_chunksize
    final_df.filter(
        pl.col("newspaper_date").dt.year() >= startyr, pl.col("newspaper_date").dt.year() < endyr
    ).sink_parquet(f"{out_folder}/combined_{startyr}_{endyr}.parquet")


df_final = pl.scan_parquet("processed_data/combined/*.parquet")
df_final.head().collect()
df_final.collect_schema()