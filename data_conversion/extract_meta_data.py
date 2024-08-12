import zipfile
from tqdm import tqdm
import xml.etree.ElementTree as ET
import polars as pl
from datetime import datetime

# Path to delpher zip folder (don't unzip!)
source_path = "raw_data/kranten_pd_1850-4.zip"
output_path_article = "processed_data/metadata/articles/article_meta_1850_1854.parquet"
output_path_journal = "processed_data/metadata/journals/journal_meta_1850_1854.parquet"


def extract_meta_data(root, namespaces):
    newspaper_meta_data = {}
    item_meta_data = []

    # Get first level element of the newspaper
    newspaper_element = root.find('didl:Item', namespaces)
    newspaper_id = newspaper_element.get(f'{{{namespaces.get("dc")}}}identifier')
    newspaper_meta_data['newspaper_id'] = newspaper_id

    # Search for all didl:Component and didl:Item elements
    components = root.findall('didl:Item/didl:Component', namespaces)
    items = root.findall('didl:Item/didl:Item', namespaces)

    for component in components:
        component_identifier = component.get(f'{{{namespaces.get("dc")}}}identifier')
        if "meta" in component_identifier:
            try:
                newspaper_meta_data['newspaper_name'] = component.find(".//dc:title", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_name'] = None
            try:
                newspaper_meta_data['newspaper_location'] = component.find(".//dcterms:spatial[@xsi:type='dcx:creation']", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_location'] = None
            try:
                newspaper_date = root.find(".//dc:date", namespaces).text
                newspaper_meta_data['newspaper_date'] = datetime.strptime(newspaper_date, "%Y-%m-%d").date()
            except AttributeError:
                newspaper_meta_data['newspaper_date'] = None
            try:
                newspaper_meta_data['newspaper_years_digitalised'] = root.find(".//ddd:yearsDigitized", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_years_digitalised'] = None
            try:
                newspaper_meta_data['newspaper_years_issued'] = root.find(".//dcterms:issued", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_years_issued'] = None
            try:
                newspaper_meta_data['newspaper_language'] = root.find(".//dc:language[@xsi:type='dcterms:ISO639-1']", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_language'] = None
            try:
                newspaper_meta_data['newspaper_temporal'] = root.find(".//dcterms:temporal", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_temporal'] = None
            try:
                newspaper_meta_data['newspaper_publisher'] = root.find(".//dc:publisher", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_publisher'] = None
            try:
                newspaper_meta_data['newspaper_spatial'] = root.find(".//dcterms:spatial", namespaces).text
            except AttributeError:
                newspaper_meta_data['newspaper_spatial'] = None

        elif "pdf" in component_identifier:
            try:
                pdf_link_element = component.find(".//didl:Resource", namespaces)
                newspaper_meta_data['pdf_link'] = pdf_link_element.get('ref')
            except AttributeError:
                newspaper_meta_data['pdf_link'] = None

    for item in items:
        item_identifier = item.get(f'{{{namespaces.get("dc")}}}identifier')
        try:
            item_subject = item.find(".//dc:subject", namespaces).text
        except AttributeError:
            item_subject = None
        item_type = item.find(".//dc:type[@xsi:type='dcterms:DCMIType']", namespaces).text
        try:
            item_filename_element = item.find(".//didl:Resource", namespaces)
            item_filename = item_filename_element.get(f'{{{namespaces.get("dcx")}}}filename')
        except AttributeError:
            item_filename = None
        item_meta_data.append({'newspaper_id': newspaper_id,
                               'item_id': item_identifier,
                               'item_subject': item_subject,
                               'item_filename': item_filename,
                               'item_type': item_type})

    return newspaper_meta_data, item_meta_data


# Define namespaces
namespaces = {
    'didl': 'urn:mpeg:mpeg21:2002:02-DIDL-NS',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    'dcx': 'http://krait.kb.nl/coop/tel/handbook/telterms.html',
    'srw_dc': 'info:srw/schema/1/dc-v1.1',
    'ddd': 'http://www.kb.nl/namespaces/ddd',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xs': "http://www.w3.org/2001/XMLSchema"
}

newspapers_meta_data = []
items_meta_data = []

# Open the zip file
with zipfile.ZipFile(source_path, 'r') as zip_ref:
    # Iterate through each file in the zip archive
    for file_info in tqdm(zip_ref.infolist()):
        filename = file_info.filename
        if filename.endswith('didl.xml'):
            with zip_ref.open(file_info) as file:
                content = file.read().decode('utf8')
                # This following if statement is necessary, otherwise Python cannot parse the xml file.
                if "http://www.w3.org/2001/XMLSchema-instance" not in content:
                    content = content.replace("<didl:DIDL ", '<didl:DIDL xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ')
                root = ET.fromstring(content)
                newspaper_meta_data, item_meta_data = extract_meta_data(root, namespaces)
                newspapers_meta_data.append(newspaper_meta_data)
                items_meta_data.extend(item_meta_data)

# Convert the results to Polars DataFrames
df_newspapers = pl.DataFrame(newspapers_meta_data)
df_items = pl.DataFrame(items_meta_data)

# Save the DataFrames as Parquet files
df_newspapers.write_parquet(output_path_journal)
df_items.write_parquet(output_path_article)