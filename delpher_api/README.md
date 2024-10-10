# Data Harvesting with Delpher API (1880-1940)
Delpher historical news article data up to 1879 can be downloaded manually from [here](https://www.delpher.nl/over-delpher/delpher-open-krantenarchief/download-teksten-kranten-1618-1879#b1741).
For the years 1880 and onward, they need to be harvested via the [Delpher API](https://www.kb.nl/en/research-find/for-researchers/data-services-apis-and-downloads).
Note that you need an API key for this, which should be specified as a `keys.txt` file under `delpher_api`.

## Step 1: Harvest article and newspaper ids
Run:
```
python delpher_api/harvest_article_ids.py --start_year 1880 --end_year 1940
```

This script will harvest all article ids and their respective newspaper ids between 1880 and 1940,
and save them as polar dataframes (with columns `article_id`, `newspaper_id` and `article_subject`) in parquet format under `processed_data/metadata/articles/from_1880_to_1940/`.
Note that `--start_year` and `--end_year` are two parameters that you can set. The default values are 1880 and 1940.

## Step 2: Harvest article content
Run:
```
python delpher_api/harvest_article_content.py --start_year 1880 --end_year 1940
```

This script will harvest all article titles and texts (if available) given the article ids we harvested in Step 1.
The harvested data will be saved under `processed_data/texts/from_{start_year}_to_{end_year}/` as polars dataframes in parquet format.
Three columns are included: `article_id`,  `article_title`, and `article_text`.

## Step 3: Harvest article and newspaper metadata
Run
```
python delpher_api/harvest_meta_data.py --start_year 1880 --end_year 1940
```

This script will harvest all article and newspaper metadata based on the newspaper ids we got from Step 1.
The harvested data will be saved under `processed_data/metadata/newspapers/from_1880_to_1940/`.
Included columns are: `newspaper_name`, `newspaper_location`, `newspaper_date`, `newspaper_years_digitalised`, `newspaper_years_issued`, `newspaper_language`, `newspaper_temporal`, `newspaper_publisher` and `newspaper_spatial`.

## Step 4: Combine and chunk data
Run
```
python data_conversion/combine_and_chunk.py --start_year 1880 --end_year 1940
```