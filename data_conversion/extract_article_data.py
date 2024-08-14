import zipfile
import polars as pl
import xml.etree.ElementTree as ET
from tqdm import tqdm
import os

# # Path to delpher zip folder (don't unzip!)
# source_path = "raw_data/kranten_pd_1850-4.zip"
# output_path = "processed_data/texts/article_texts_1850_1854.parquet"

source_path = "D:/disease_database_data/kranten_pd_1877-9.zip"
output_path = "processed_data/texts/article_texts_1877_1879.parquet"

# Results object to write to
results = []

# Open the zip file
with zipfile.ZipFile(source_path, "r") as zip_ref:
    # Iterate through each file in the zip archive
    for file_info in tqdm(zip_ref.infolist()):
        filename = os.path.basename(file_info.filename)
        publication_date = filename[:10]
        if filename.endswith("articletext.xml"):
            with zip_ref.open(file_info) as file:
                try:
                    # Parse the XML content
                    content = file.read().decode("utf8")
                    root = ET.fromstring(content)
                    title_content = root.find("title").text
                    p_contents = [p.text for p in root.findall("p")]
                    p_contents = " ".join(p_contents)
                    results.append(
                        {
                            "file_name": filename,
                            "title": title_content,
                            "text": p_contents,
                        }
                    )
                except Exception as e:
                    results.append(
                        {
                            "file_name": file_info.filename,
                            "title": f"An error occurred: {str(e)}",
                            "text": f"An error occurred: {str(e)}",
                        }
                    )

# Convert the results to a Polars DataFrame
df = pl.DataFrame(results)

# Save the DataFrame as a Parquet file
df.write_parquet(output_path)
