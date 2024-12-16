import polars as pl
from pathlib import Path
from tqdm import tqdm

COMBINED_FOLDER = Path("processed_data", "combined")
INPUT_FOLDER = Path("raw_data", "manual_input")
locations = pl.read_excel(INPUT_FOLDER / "municipalities_1869.xlsx")
diseases = pl.read_excel(INPUT_FOLDER / "disease_search_terms.xlsx")

disease_regex = (
    diseases.filter(pl.col("Disease").str.contains("Cholera"))
    .select("Regex search")
    .item()
)

df = pl.read_parquet(COMBINED_FOLDER / "combined_1866_1867.parquet")

def get_map(disease_regex, year: int = 1866):
    DIS = "(?i)" + disease_regex
    df = pl.read_parquet(
        COMBINED_FOLDER / f"combined_{year}_{year + 1}.parquet"
    ).filter(
        pl.col("newspaper_date").dt.year() >= year,
        pl.col("newspaper_date").dt.year() <= year,
    )

    df_list = []
    for location_regex in tqdm(locations["Regex"]):
        LOC = "(?i)" + location_regex
        try:
            df_list.append(
                df.with_columns(
                    pl.col("article_text").str.contains(DIS).alias("disease"),
                    pl.col("article_text").str.contains(LOC).alias("location"),
                ).select(
                    pl.col("disease").sum().alias("n_disease"),
                    pl.col("location").sum().alias("n_location"),
                    (pl.col("disease") & pl.col("location")).sum().alias("n_both"),
                )
            )
        except Exception as e:
            print(e)
    return pl.concat(df_list)


res = get_map(disease_regex=disease_regex, year=1864)
pl.concat([locations, res], how = "horizontal").write_parquet("maps/cholera_1864.parquet")

res = get_map(disease_regex=disease_regex, year=1865)
pl.concat([locations, res], how = "horizontal").write_parquet("maps/cholera_1865.parquet")

res = get_map(disease_regex=disease_regex, year=1866)
pl.concat([locations, res], how = "horizontal").write_parquet("maps/cholera_1866.parquet")

res = get_map(disease_regex=disease_regex, year=1867)
pl.concat([locations, res], how = "horizontal").write_parquet("maps/cholera_1867.parquet")

res = get_map(disease_regex=disease_regex, year=1868)
pl.concat([locations, res], how = "horizontal").write_parquet("maps/cholera_1868.parquet")
