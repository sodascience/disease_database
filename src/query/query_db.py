import polars as pl
import plotnine as p9


df = (
    pl.read_parquet("processed_data/database/**/*.parquet")
    .filter(pl.col("location").is_in(["Amsterdam", "Groningen", "Dordrecht"]))#, pl.col("disease").is_in(["cholera", "smallpox"]))
    .with_columns(pl.date(pl.col("year"), pl.col("month"), 1).alias("date"))
)

plt = (
    p9.ggplot(
        df,
        p9.aes(
            x="date",
            y="normalized_mentions",
            ymin="lower",
            ymax="upper",
            color="disease",
            fill="disease",
        ),
    )
    + p9.geom_ribbon(alpha=0.4, color="none")
    + p9.geom_line()
    + p9.scale_x_date(date_breaks="10 years", date_labels="%Y")
    + p9.facet_grid(rows="disease", cols="location", scales="free")
    + p9.theme_linedraw()
    + p9.theme(axis_text_x=p9.element_text(rotation=90), legend_position="none")
    + p9.labs(
        title="Disease mentions in Amsterdam, Dordrecht, Groningen, 1830 - 1940",
        subtitle="Data from Delpher newspaper archive, Royal Library",
        x="Year",
        y = "Normalized mentions"
    )
)

p9.ggsave(plt, "img/adg_all.png", width=10, height=15, dpi=300)