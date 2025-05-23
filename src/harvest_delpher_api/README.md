# Data Harvesting with Delpher API (1880-1940)
Delpher historical news article data up to 1879 can be downloaded manually from [here](https://www.delpher.nl/over-delpher/delpher-open-krantenarchief/download-teksten-kranten-1618-1879#b1741).
For the years 1880 and onward, they need to be harvested via the [Delpher API](https://www.kb.nl/en/research-find/for-researchers/data-services-apis-and-downloads).
Note that you need an API key for this, which should be specified as a `apikey.txt` file under `src/harvest_delpher_api`.

## Step 1: Harvest article and newspaper ids

```
uv run src/harvest_delpher_api/harvest_article_ids.py --start_year 1880 --end_year 1940
```

This script will harvest all article ids and their respective newspaper ids between 1880 and 1940, and save them as polars dataframes (with columns `article_id`, `newspaper_id` and `article_subject`) in parquet format under `processed_data/metadata/articles/api_harvest/`.
Note that `--start_year` and `--end_year` are two parameters that you can set. The default values are 1880 and 1940.

## Step 2: Harvest article content

```
uv run src/harvest_delpher_api/harvest_article_content.py --start_year 1880 --end_year 1940
```

This script will harvest all article titles and texts (if available) given the article ids we harvested in Step 1.
The harvested data will be saved under `processed_data/texts/api_harvest/` as polars dataframes in parquet format.
Three columns are included: `article_id`,  `article_title`, and `article_text`.

## Step 3: Harvest article and newspaper metadata

```
uv run src/harvest_delpher_api/harvest_meta_data.py --start_year 1880 --end_year 1940
```

This script will harvest all article and newspaper metadata based on the newspaper ids we got from Step 1. The harvested data will be saved under `processed_data/metadata/newspapers/api_harvest/`. Included columns are: `newspaper_name`, `newspaper_location`, `newspaper_date`, `newspaper_years_digitalised`, `newspaper_years_issued`, `newspaper_language`, `newspaper_temporal`, `newspaper_publisher` and `newspaper_spatial`.

## Step 4: Combine and chunk data

In the last step, we join all three sources of data to create combined datafiles of one row per article. To run this part, run:

```
uv run src/harvest_delpher_api/combine_and_chunk.py
```

This will create sets of data files in `processed_data/combined` with the following columns:

```
'newspaper_id'
'article_id'
'article_subject'
'article_title'
'article_text'
'newspaper_name'
'newspaper_location'
'newspaper_date'
'newspaper_years_digitalised'
'newspaper_years_issued'
'newspaper_language'
'newspaper_temporal'
'newspaper_publisher'
'newspaper_spatial'
```

This is still quite a lot of information, which will be further pruned in the `create_database` workflow.