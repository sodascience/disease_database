"""
Combine the different extracted files and create a chunked parquet
folder for performing data analysis.
"""
import polars as pl
from tqdm import tqdm

out_folder = "processed_data/combined"

article_text_df = pl.scan_parquet("processed_data/texts/*.parquet")
article_meta_df = pl.scan_parquet("processed_data/metadata/articles/*.parquet")
journal_meta_df = pl.scan_parquet("processed_data/metadata/journals/*.parquet")


article_text_df.head().collect()
article_meta_df.head().collect()
journal_meta_df.head().collect()

# compute journal_id and article_id
# for article text
article_text_df = (
    article_text_df.with_columns(
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
    article_meta_df.with_columns(
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
    journal_meta_df.with_columns(
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


# create master df with everything needed
final_df = article_text_df.join(
    article_meta_df.select(pl.exclude("journal_id")),
    on="article_id",
    how="left",
).join(journal_meta_df, on="journal_id", how="left")

# write to chunked parquet files
year_chunksize = 1
for startyr in tqdm(range(1830, 1880, year_chunksize)):
    endyr = startyr + year_chunksize
    final_df.filter(
        pl.col("date").dt.year() >= startyr, pl.col("date").dt.year() < endyr
    ).sink_parquet(f"{out_folder}/combined_{startyr}_{endyr}.parquet")


df_final = pl.scan_parquet("processed_data/combined/*.parquet")
df_final.head().collect()
df_final.collect_schema()