from tqdm import tqdm
from utils_data_conversion import (
    convert_articles_from_zip_to_parquet,
    zip_file_names_conversion_dict,
)
from pathlib import Path


# specify paths for source and output folders
SOURCE_FOLDER = Path("raw_data", "open_archive")
ARTICLE_TEXT_FOLDER = Path("processed_data", "texts", "open_archive")


def main():
    # find all zip files
    zip_paths = list(SOURCE_FOLDER.glob("*.zip"))

    # create output folder if it does not exist
    ARTICLE_TEXT_FOLDER.mkdir(exist_ok=True)

    for zip_path in tqdm(zip_paths):
        out_file_suffix = zip_file_names_conversion_dict[zip_path.name]
        out_path = ARTICLE_TEXT_FOLDER / f"article_texts_{out_file_suffix}"
        convert_articles_from_zip_to_parquet(zip_path, out_path)


if __name__ == "__main__":
    main()
