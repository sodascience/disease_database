import polars as pl
import plotnine as p9


def query(disease: str = "choler*", location: str = "amst*"):
    DIS = "(?i)" + disease
    LOC = "(?i)" + location
    return (
        pl.scan_parquet("processed_data/combined/*.parquet")
        .with_columns(
            pl.col("text").str.contains(DIS).alias("disease"),
            pl.col("text").str.contains(LOC).alias("location"),
        )
        .sort("date")
        .group_by_dynamic(
            "date", every="1w", period="1mo"
        )  # monthly average every week
        .agg(
            pl.len().alias("n_total"),
            pl.col("disease").sum().alias("n_disease"),
            pl.col("location").sum().alias("n_location"),
            (pl.col("disease") & pl.col("location")).sum().alias("n_both"),
        )
        .collect()
    )

city_df = pl.concat(
    [
        query(location="rotter*").with_columns(pl.lit("Rotterdam").alias("City")),
        query(location="amster*").with_columns(pl.lit("Amsterdam").alias("City")),
        query(location="utrec*").with_columns(pl.lit("Utrecht").alias("City")),
        query(location="maastric*").with_columns(pl.lit("Maastricht").alias("City")),
    ]
)

plt = (
    p9.ggplot(
        city_df.with_columns((pl.col("n_both") / pl.col("n_location")).alias("y")),
        p9.aes(x="date", y="y", colour="City"),
    )
    + p9.geom_line()
    + p9.scale_x_date(date_breaks="5 years", date_labels="%Y")
    + p9.facet_wrap("City")
    + p9.theme_linedraw()
    + p9.theme(legend_position="none")
    + p9.labs(
        title="Cholera in the Netherlands",
        subtitle="Monthly average",
        y="Normalized mentions",
    )
)

p9.ggsave(plt, "img/cholera.png")
