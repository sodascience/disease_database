import zipfile
from tqdm import tqdm
import xml.etree.ElementTree as ET
import polars as pl
from utils_data_conversion import extract_meta_data, namespaces, zip_file_names_conversion_dict, make_dir
import os

# Specify the directory you want to scan
source_folder_path = "raw_data/"
# List comprehension to scan for all zip files in the specified directory
zip_files = [file for file in os.listdir(source_folder_path) if file.endswith(".zip")]

for zip_file in zip_files:
    # Path to delpher zip folder (don't unzip!)
    source_file_path = f"{source_folder_path}/{zip_file}"
    out_file_suffix = zip_file_names_conversion_dict[zip_file]
    output_folder_path_article = "processed_data/metadata/articles/from_1830_to_1879"
    make_dir(output_folder_path_article)
    output_file_path_article = f"{output_folder_path_article}/article_meta_{out_file_suffix}"
    output_folder_path_newspaper = "processed_data/metadata/newspapers/from_1830_to_1879"
    make_dir(output_folder_path_newspaper)
    output_file_path_newspaper = f"{output_folder_path_newspaper}/newspaper_meta_{out_file_suffix}"

    newspapers_meta_data = []
    items_meta_data = []

    # Open the zip file
    with zipfile.ZipFile(source_file_path, "r") as zip_ref:
        # Iterate through each file in the zip archive
        for file_info in tqdm(zip_ref.infolist()):
            filename = file_info.filename
            if filename.endswith("didl.xml"):
                with zip_ref.open(file_info) as file:
                    content = file.read().decode("utf8")
                    # This following if statement is necessary, otherwise Python cannot parse the xml file.
                    if "http://www.w3.org/2001/XMLSchema-instance" not in content:
                        content = content.replace(
                            "<didl:DIDL ",
                            '<didl:DIDL xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ',
                        )
                    root = ET.fromstring(content)
                    newspaper_meta_data, item_meta_data = extract_meta_data(
                        root, namespaces
                    )
                    newspapers_meta_data.append(newspaper_meta_data)
                    items_meta_data.extend(item_meta_data)

    # Convert the results to Polars DataFrames
    df_newspapers = pl.DataFrame(newspapers_meta_data)
    df_items = pl.DataFrame(items_meta_data)

    # Save the DataFrames as Parquet files
    df_newspapers.write_parquet(output_file_path_newspaper)
    df_items.write_parquet(output_file_path_article)
