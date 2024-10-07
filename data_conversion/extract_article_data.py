from tqdm import tqdm
import os
from utils_data_conversion import convert_articles_from_zip_to_parquet

def main():
    # Specify the directory you want to scan
    folder_path = "raw_data/"
    # List comprehension to scan for all zip files in the specified directory
    zip_files = [file for file in os.listdir(folder_path) if file.endswith(".zip")]
    for zip_file in tqdm(zip_files):
        convert_articles_from_zip_to_parquet(folder_path, zip_file)

if __name__ == "__main__":
    main()