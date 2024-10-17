import polars as pl
import plotnine as p9
from pathlib import Path

disease_query = r"ty(ph|f)(us|euz.)|febris.?typhoidea|kwaadaardige.*koorts"
location_query = r"amste[rl]da.*|amst\."

COMBINED_DATA_FOLDER = Path("processed_data", "combined")

def get_path_list(year_start: int = 1830, year_end: int = 1880):
    path_list = []
    for yr in range(year_start, year_end):
        path_list.append(COMBINED_DATA_FOLDER / f"combined_{yr}_{yr + 1}.parquet")
    return path_list

def query(disease: str, location: str, year_start: int = 1830, year_end: int = 1880, streaming: bool = True):
    DIS = "(?i)" + disease
    LOC = "(?i)" + location

    df = pl.scan_parquet(get_path_list(year_start, year_end))

    return (
        df
        .filter(pl.col("newspaper_date").dt.year() >= year_start, pl.col("newspaper_date").dt.year() <= year_end)
        .with_columns(
            pl.col("article_text").str.contains(DIS).alias("disease"),
            pl.col("article_text").str.contains(LOC).alias("location"),
        )
        .sort(pl.col("newspaper_date"))
        .with_columns(
            pl.col("newspaper_date").dt.year().alias("yr"),
            pl.col("newspaper_date").dt.month().alias("mo"),
        )
        .group_by(["yr", "mo"])
        .agg(
            pl.len().alias("n_total"),
            pl.col("disease").sum().alias("n_disease"),
            pl.col("location").sum().alias("n_location"),
            (pl.col("disease") & pl.col("location")).sum().alias("n_both"),
        )
        .collect(streaming=streaming)
    )

df = query(disease_query, location_query, 1900, 1910)
plt = (
    p9.ggplot(
        df.with_columns(
            pl.date(pl.col("yr"), pl.col("mo"), 1).alias("date"),
            (pl.col("n_both") / pl.col("n_total")).alias("y")
        ),
        p9.aes(x="date", y="y"),
    )
    # + p9.geom_vline(data=key_dates, mapping=p9.aes(xintercept="date"), linetype="dashed", color="grey")
    # + p9.geom_label(data=key_dates, mapping=p9.aes(y="yloc", label="label"), ha="left")
    + p9.geom_line()
    + p9.scale_x_date(date_breaks="5 years", date_labels="%Y")
    # + p9.facet_wrap("City", scales="free_y")
    + p9.theme_linedraw()
    + p9.theme(legend_position="none", axis_text_x=p9.element_text(rotation="vertical"))
    + p9.labs(
        title="Cholera in Amsterdam",
        y="Monthly normalized mentions",
    )
)

city_df = pl.concat(
    [
        query(disease="cholera",location="rotter*").with_columns(pl.lit("Rotterdam").alias("City")),
        query(disease="cholera",location="amster*").with_columns(pl.lit("Amsterdam").alias("City")),
        query(disease="cholera",location="u[i]trec*").with_columns(pl.lit("Utrecht").alias("City")),
        query(disease="cholera",location="maastric*").with_columns(pl.lit("Maastricht").alias("City")),
    ]
)

key_dates = pl.DataFrame({
  "date": ["1832-07-01", "1849-07-01", "1853-10-16", "1866-07-01"],
  "label": ["1832 cholera epidemic", "1849 cholera epidemic", "Crimean war", "1866 cholera epidemic"],
  "yloc": [.045, .045, .04, .045],
}).with_columns(pl.col("date").str.to_date())

plt = (
    p9.ggplot(
        city_df.with_columns(
            pl.date(pl.col("yr"), pl.col("mo"), 1).alias("date"),
            (pl.col("n_both") / pl.col("n_total")).alias("y")
        ),
        p9.aes(x="date", y="y"),
    )
    + p9.geom_vline(data=key_dates, mapping=p9.aes(xintercept="date"), linetype="dashed", color="grey")
    + p9.geom_label(data=key_dates, mapping=p9.aes(y="yloc", label="label"), ha="left")
    + p9.geom_line(p9.aes(color="City"))
    + p9.scale_x_date(date_breaks="5 years", date_labels="%Y")
    + p9.facet_wrap("City", scales="free_y")
    + p9.theme_linedraw()
    + p9.theme(legend_position="none", axis_text_x=p9.element_text(rotation="vertical"))
    + p9.labs(
        title="Cholera in Dutch cities",
        y="Monthly normalized mentions",
    )
)

plt.show()

p9.ggsave(plt, "img/cholera.png", width=9, height=6, dpi=300)


# qdf = query(disease="cholera", location="le[iy]den")
#
# key_dates = pl.DataFrame({
#   "date": ["1832-07-01", "1849-07-01", "1853-10-16", "1866-07-01"],
#   "label": ["1832 cholera epidemic", "1849 cholera epidemic", "Crimean war", "1866 cholera epidemic"],
#   "yloc": [.045, .045, .04, .045],
# }).with_columns(pl.col("date").str.to_date())
#
# plt2 = (
#     p9.ggplot(
#         qdf.with_columns(
#             pl.date(pl.col("yr"), pl.col("mo"), 1).alias("date"),
#             (pl.col("n_both") / pl.col("n_total")).alias("y")
#         ),
#         p9.aes(x="date", y="y"),
#     )
#     + p9.geom_vline(data=key_dates, mapping=p9.aes(xintercept="date"), linetype="dashed", color="grey")
#     + p9.geom_label(data=key_dates, mapping=p9.aes(y="yloc", label="label"), ha="left")
#     + p9.geom_line(colour="darkblue")
#     + p9.scale_x_date(date_breaks="5 years", date_labels="%Y")
#     + p9.theme_linedraw()
#     + p9.theme(legend_position="none")
#     + p9.labs(
#         title="Cholera mentions in Leiden",
#         y="Monthly normalized mentions",
#         x=""
#     )
# )
#
# p9.ggsave(plt2, "img/leiden.png", width=8, height=5, dpi=300)
