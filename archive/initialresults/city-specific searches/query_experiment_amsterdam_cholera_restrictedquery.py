import polars as pl
import plotnine as p9

# Define the regular expression for disease (cholera and Crimean fever) and location (Amsterdam)
disease_query = r"choler.*|krim.?koorts"
location_query = r"amste.?dam|amst\.?"

# Construct the combined regex to match disease and location within 20 characters
combined_query = rf"(?i)(({disease_query}).{{0,40}}({location_query})|({location_query}).{{0,40}}({disease_query}))"

# Query function for filtering the data with proximity check
def query(disease: str, location: str, year_start: int = 1830, year_end: int = 1880):
    df = pl.scan_parquet("processed_data/combined/*.parquet")

    return (
        df
        .filter((pl.col("date").dt.year() >= year_start) & (pl.col("date").dt.year() <= year_end))
        .with_columns(
            pl.col("text").str.contains(combined_query).alias("disease_location_near")
        )
        .sort(pl.col("date"))
        .with_columns(
            pl.col("date").dt.year().alias("yr"),
            pl.col("date").dt.month().alias("mo"),
        )
        .group_by(["yr", "mo"])
        .agg(
            pl.len().alias("n_total"),
            pl.col("disease_location_near").sum().alias("n_near_matches")
        )
        .collect(streaming=True)
    )

# Querying for mentions of cholera and Crimean fever near Amsterdam
qdf = query(disease=disease_query, location=location_query)

# Key historical dates
key_dates = pl.DataFrame({
  "date": ["1832-07-01", "1849-07-01", "1853-10-16", "1866-07-01"],
  "label": ["1832 cholera epidemic", "1849 cholera epidemic", "Crimean war", "1866 cholera epidemic"],
  "yloc": [.045, .045, .04, .045],
}).with_columns(pl.col("date").str.to_date())

# Plot for Amsterdam mentions with key dates and smaller labels
plt = (
    p9.ggplot(
        qdf.with_columns(
            pl.date(pl.col("yr"), pl.col("mo"), 1).alias("date"),
            (pl.col("n_near_matches") / pl.col("n_total")).alias("y")
        ),
        p9.aes(x="date", y="y"),
    )
    + p9.geom_vline(data=key_dates, mapping=p9.aes(xintercept="date"), linetype="dashed", color="grey")
    + p9.geom_label(data=key_dates, mapping=p9.aes(y="yloc", label="label"), ha="left", size=8)  # Smaller label size
    + p9.geom_line(colour="darkblue")
    + p9.scale_x_date(date_breaks="5 years", date_labels="%Y")
    + p9.coord_cartesian(ylim=(0, 0.08))  # Force the y-axis range from 0 to 0.1
    + p9.theme_linedraw()
    + p9.theme(legend_position="none")
    + p9.labs(
        title="Cholera Mentions Near Amsterdam (Within 40 Characters)",
        y="Monthly normalized mentions",
        x=""
    )
)

# Show the plot
plt.show()

# Save the plot as an image
p9.ggsave(plt, "img/amsterdam_cholera_krimkoorts_near.png", width=8, height=5, dpi=300)
