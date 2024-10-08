import os
import polars as pl
from tqdm import tqdm
import argparse
from utils_delpher_api import get_metadata_url, retrieve_newspaper_metadata

# Define namespaces
namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    "didl": "urn:mpeg:mpeg21:2002:02-DIDL-NS",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "dcx": "http://krait.kb.nl/coop/tel/handbook/telterms.html",
    "srw_dc": "info:srw/schema/1/dc-v1.1",
    "ddd": "http://www.kb.nl/namespaces/ddd",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "xs": "http://www.w3.org/2001/XMLSchema",
}

with open("delpher_api/keys.txt") as f:
    apikey = f.readline()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", type=int, default=1880)
    parser.add_argument("--end_year", type=int, default=1940)
    args = parser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    in_folder_path = f"processed_data/metadata/articles/from_1880_to_1940/"
    out_folder_path = f"processed_data/metadata/newspapers/from_{start_year}_to_{end_year}/"
    if not os.path.exists(out_folder_path):
       os.makedirs(out_folder_path)

    for year in tqdm(range(start_year, end_year+1)):
        for month in tqdm(range(1, 13)):
            article_meta_df = pl.scan_parquet(in_folder_path + f"article_meta_{year}_{month}.parquet")
            newspaper_ids = article_meta_df.collect()["newspaper_id"].unique().to_list()
            out_file_name = f"newspaper_meta_{year}_{month}.parquet"

            out_file_path = out_folder_path + out_file_name
            if os.path.exists(out_file_path):
                print(f"Data already harvested for {year}-{month}!")
                continue

            metadata_ls = []
            for newspaper_id in tqdm(newspaper_ids):
                prefix = newspaper_id.split(":")[1]
                identifier = ":".join(newspaper_id.split(":")[-2:])
                url = get_metadata_url(apikey, prefix, identifier)
                newspaper_metadata = retrieve_newspaper_metadata(url, namespaces)
                newspaper_metadata["newspaper_id"] = newspaper_id
                metadata_ls.append(newspaper_metadata)

            metadata_df = pl.DataFrame(metadata_ls)
            metadata_df.write_parquet(out_folder_path+out_file_name)

if __name__ == "__main__":
    main()