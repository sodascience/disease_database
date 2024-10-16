# Disease database 
[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

Creating a historical disease database (19th-20th century) for municipalities in the Netherlands.

![](img/cholera.png)

## Preparation

This project uses [pyproject.toml](pyproject.toml) to handle its dependencies. You can install them using pip like so:

```
pip install .
```

We recommend using [uv](https://github.com/astral-sh/uv) to manage the environment. First, install uv, then clone / download this repo, then run:

``` 
uv sync
```

this will automatically install the right python version, create a virtual environment, and install the required packages.

Note that if you encountered `error: command 'cmake' failed: No such file or directory`, you need to install [cmake](https://cmake.org/download/) first.
On macOS, run `brew install cmake`. Similarly, you may have to install `apache-arrow` separately as well (e.g., on macOS `brew install apache-arrow`).

Once these dependency issues are solved, run `uv sync` one more time.

## Data extraction (1830-1879)
Between 1830 and 1879, Delpher historical news article data can be downloaded manually from [here](https://www.delpher.nl/over-delpher/delpher-open-krantenarchief/download-teksten-kranten-1618-1879#b1741).
The downloaded files, which are zip folders, take up lots of disk space because of inefficient data format.

The `src/process_open_archive/extract_article_data.py` script extracts the titles and texts from the zip folder for each article.
Then, it stores all extracted data as a polars dataframe with three columns `article_id`, `article_title` and `article_text`.
Finally, it is saved as a parquet file (`article_data_{start_year}_{end_year}.parquet`), with a much smaller size under `processed_data/texts/from_1830_to_1879/`.

With the `src/process_open_archive/extract_meta_data.py` script, we extract meta information about both the newspapers and the individual articles.
This results in two kinds of polars dataframes saved in parquet format under `processed_data/metadata/newspapers/from_1830_to_1879` and `processed_data/metadata/articles/from_1830_to_1879`, respectively.

1) `newspaper_meta_data_{start_year}_{end_year}.parquet` includes these columns: `newspaper_name`, `newspaper_location`,
   `newspaper_date`, `newspaper_years_digitalised`, `newspaper_years_issued`, `newspaper_language`, `newspaper_temporal`,
   `newspaper_publisher` and `newspaper_spatial`.
2) `article_meta_data_{start_year}_{end_year}.parquet` includes these columns: `newspaper_id`, `article_id` and `article_subject`.

Before you run the following script, make sure to put all the Delpher zip files under `raw_data/open_archive`.

```
python src/process_open_archive/extract_article_data.py
python src/process_open_archive/extract_meta_data.py
```

Then, run 

```
python src/process_open_archive/combine_and_chunk.py
``` 
to join all the available datasets and create a yearly-chunked series of parquet files in the folder `processed_data/combined`.

## Data harvesting (1880-1940)
After 1880, the data is not public and can only be obtained through the Delpher API: 

1. Obtain an api key (which looks like this `df2e02aa-8504-4af2-b3d9-64d107f4479a`) from Delpher, then put the api key in the file `harvest_delpher_api/apikey.txt`.
2. Harvest the data following readme in the delpher api folder: [src/harvest_delpher_api/readme.md](./src/harvest_delpher_api/README.md)

## Data analysis
The script `src/query/query.py` uses the prepared combined data to search for mentions of diseases and locations in articles. The file produces the plot shown above. It also produces this plot about Leiden:

![](img/leiden.png)

This plot aligns quite nicely with the google ngram viewer, querying "cholera" in an English, German, and French corpus ([click here to see interactively](https://books.google.com/ngrams/graph?content=cholera%3Aeng%2CCholera%3Ager%2Cchol%C3%A9ra%3Afre&year_start=1830&year_end=1880&corpus=en&smoothing=0))

![](img/ngram_cholera.png)

## Contact
<img src="./img/soda_logo.png" alt="SoDa logo" width="250px"/>

This project is developed and maintained by the [ODISSEI Social Data
Science (SoDa)](https://odissei-soda.nl) team.

Do you have questions, suggestions, or remarks? File an issue in the
issue tracker or feel free to contact the team at [`odissei-soda.nl`](https://odissei-soda.nl)

