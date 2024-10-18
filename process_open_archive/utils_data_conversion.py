import os
import zipfile
from os import makedirs

import polars as pl
import xml.etree.ElementTree as ET
from tqdm import tqdm
from datetime import datetime
from pathlib import Path

zip_file_names_conversion_dict = {
    "kranten_pd_183x.zip": "1830_1839.parquet",
    "kranten_pd_184x.zip": "1840_1849.parquet",
    "kranten_pd_1850-4.zip": "1850_1854.parquet",
    "kranten_pd_1855-9.zip": "1855_1859.parquet",
    "kranten_pd_1860-4.zip": "1860_1864.parquet",
    "kranten_pd_1865-9.zip": "1865_1869.parquet",
    "kranten_pd_1870-4.zip": "1870_1874.parquet",
    "kranten_pd_1875-6.zip": "1875_1876.parquet",
    "kranten_pd_1877-9.zip": "1877_1879.parquet",
}

# Define namespaces
namespaces = {
    "didl": "urn:mpeg:mpeg21:2002:02-DIDL-NS",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "dcx": "http://krait.kb.nl/coop/tel/handbook/telterms.html",
    "srw_dc": "info:srw/schema/1/dc-v1.1",
    "ddd": "http://www.kb.nl/namespaces/ddd",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "xs": "http://www.w3.org/2001/XMLSchema",
}

def convert_articles_from_zip_to_parquet(
    zip_path: Path = Path("raw_data", "kranten_pd_183x.zip"),
    out_path: Path = Path(
        "processed_data", "texts", "open_archive", "article_texts_1830_1839.parquet"
    ),
):
    "Function to read the article zip file, extract article ids and texts, and save as parquet"

    # Results object to write to
    results = []

    # Open the zip file
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        # Iterate through each file in the zip archive
        for file_info in tqdm(zip_ref.infolist()):
            filename = os.path.basename(file_info.filename)
            if filename.endswith("articletext.xml"):
                filename_split = filename.lower().split("_")
                filename_split[3] = "a" + filename_split[2]
                filename_split[2] = "mpeg21"
                article_id = ":".join(filename_split)

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
                                "article_id": article_id,
                                "article_title": title_content,
                                "article_text": p_contents,
                            }
                        )
                    except Exception as e:
                        results.append(
                            {
                                "article_id": article_id,
                                "article_title": f"An error occurred: {str(e)}",
                                "article_text": f"An error occurred: {str(e)}",
                            }
                        )

    # Convert the results to a Polars DataFrame
    df = pl.DataFrame(results)

    # Save the DataFrame as a Parquet file
    df.write_parquet(out_path)


def extract_meta_data(root, namespaces):
    newspaper_meta_data = {}
    item_meta_data = []

    # Get first level element of the newspaper
    newspaper_element = root.find("didl:Item", namespaces)
    newspaper_id = newspaper_element.get(f'{{{namespaces.get("dc")}}}identifier')
    newspaper_meta_data["newspaper_id"] = newspaper_id

    # Search for all didl:Component and didl:Item elements
    components = root.findall("didl:Item/didl:Component", namespaces)
    items = root.findall("didl:Item/didl:Item", namespaces)

    for component in components:
        component_identifier = component.get(f'{{{namespaces.get("dc")}}}identifier')
        if "meta" in component_identifier:
            try:
                newspaper_meta_data["newspaper_name"] = component.find(
                    ".//dc:title", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_name"] = None
            try:
                newspaper_meta_data["newspaper_location"] = component.find(
                    ".//dcterms:spatial[@xsi:type='dcx:creation']", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_location"] = None
            try:
                newspaper_date = root.find(".//dc:date", namespaces).text
                newspaper_meta_data["newspaper_date"] = datetime.strptime(
                    newspaper_date, "%Y-%m-%d"
                ).date()
            except AttributeError:
                newspaper_meta_data["newspaper_date"] = None
            try:
                newspaper_meta_data["newspaper_years_digitalised"] = root.find(
                    ".//ddd:yearsDigitized", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_years_digitalised"] = None
            try:
                newspaper_meta_data["newspaper_years_issued"] = root.find(
                    ".//dcterms:issued", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_years_issued"] = None
            try:
                newspaper_meta_data["newspaper_language"] = root.find(
                    ".//dc:language[@xsi:type='dcterms:ISO639-1']", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_language"] = None
            try:
                newspaper_meta_data["newspaper_temporal"] = root.find(
                    ".//dcterms:temporal", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_temporal"] = None
            try:
                newspaper_meta_data["newspaper_publisher"] = root.find(
                    ".//dc:publisher", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_publisher"] = None
            try:
                newspaper_meta_data["newspaper_spatial"] = root.find(
                    ".//dcterms:spatial", namespaces
                ).text
            except AttributeError:
                newspaper_meta_data["newspaper_spatial"] = None

    for item in items:
        item_id = item.get(f'{{{namespaces.get("dc")}}}identifier')
        if item_id.split(":")[-1].startswith("p"):
            continue
        try:
            article_subject = item.find(".//dc:subject", namespaces).text
        except AttributeError:
            article_subject = None
        item_meta_data.append(
            {
                "newspaper_id": newspaper_id,
                "article_id": item_id,
                "article_subject": article_subject,
            }
        )

    return newspaper_meta_data, item_meta_data
