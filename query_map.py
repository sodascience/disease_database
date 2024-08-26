import polars as pl

df = pl.read_excel("raw_data/query_names.xlsx")

# if space before, extract last word
df.with_columns(pl.col("name").str.extract_groups(r"(?i) ?(\w+)$").alias("estract"))

df.with_columns(
    (
        pl.col("name")
        .str.to_lowercase()  # lowercase
        .str.head(-2) + "*"  # replace last 2 chars with wildcard
    ).alias("auto_query")
).with_columns(
    pl.when(pl.col("manual_query").is_not_null())
    .then(pl.col("manual_query"))
    .otherwise(pl.col("auto_query"))
    .alias("query")
)
