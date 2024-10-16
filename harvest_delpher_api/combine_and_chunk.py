import polars as pl
from pathlib import Path

ARTICLE_ID_FOLDER = Path("processed_data", "metadata", "articles", "api_harvest")
ARTICLE_TEXT_FOLDER = Path("processed_data", "texts", "api_harvest")
NEWSPAPER_META_FOLDER = Path("processed_data", "metadata", "newspapers", "api_harvest")


article_ids = pl.scan_parquet(ARTICLE_ID_FOLDER / "*.parquet")
article_texts = pl.scan_parquet(ARTICLE_TEXT_FOLDER / "*.parquet")
journal_meta = pl.scan_parquet(NEWSPAPER_META_FOLDER / "*.parquet")

# transform article ids
article_df = (
    article_ids.with_columns(
        pl.col("article_id")
        .str.split_exact(":", 4)
        .struct.rename_fields(
            ["journal_archive", "journal_id", "filetype", "article_num"]
        )
        .alias("fields")
    )
    .unnest("fields")
    .with_columns(
        (
            pl.col("journal_id") + "_" + pl.col("article_num").str.extract("(\\d+)")
        ).alias("article_id")
    )
    .select("journal_id", "article_id", "article_subject")
)

# transform ids in texts
text_df = (
    article_texts.with_columns(
        pl.col("article_id")
        .str.split_exact(":", 4)
        .struct.rename_fields(
            ["journal_archive", "journal_id", "filetype", "article_num"]
        )
        .alias("fields")
    )
    .unnest("fields")
    .with_columns(
        (
            pl.col("journal_id") + "_" + pl.col("article_num").str.extract("(\\d+)")
        ).alias("article_id"),
        pl.col("article_title").alias("title"),
        pl.col("article_text").alias("text"),
    )
    .select("article_id", "title", "text")
)

journal_df = ()

final_df = article_ids.join(
    article_texts, on="article_id", how="left", coalesce=True
).join(journal_meta, on="newspaper_id", how="left", coalesce=True)

pl.scan_parquet("processed_data/combined/*.parquet").head().collect()
