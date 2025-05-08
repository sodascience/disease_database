import zipfile
from tqdm import tqdm
import xml.etree.ElementTree as ET
import polars as pl
from utils_data_conversion import (
    extract_meta_data,
    namespaces,
    zip_file_names_conversion_dict,
)
from pathlib import Path

# specify paths for source and output folders
SOURCE_FOLDER = Path("raw_data", "open_archive")
NEWSPAPER_META_FOLDER = Path("processed_data", "metadata", "newspapers", "open_archive")
ARTICLE_META_FOLDER = Path("processed_data", "metadata", "articles", "open_archive")


# find all zip files
zip_paths = list(SOURCE_FOLDER.glob("*.zip"))
for zip_path in zip_paths:
    # Path to delpher zip folder (don't unzip!)
    out_file_suffix = zip_file_names_conversion_dict[zip_path.name]
    ARTICLE_META_FOLDER.mkdir(exist_ok=True)
    output_file_path_article = ARTICLE_META_FOLDER / f"article_meta_{out_file_suffix}"
    NEWSPAPER_META_FOLDER.mkdir(exist_ok=True)
    output_file_path_newspaper = (
        NEWSPAPER_META_FOLDER / f"newspaper_meta_{out_file_suffix}"
    )

    if output_file_path_article.exists() and output_file_path_newspaper.exists():
        print(
            f"\n {output_file_path_article} and {output_file_path_newspaper} already exist! Skipping..."
        )
        continue

    newspapers_meta_data = []
    items_meta_data = []

    # Open the zip file
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
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
