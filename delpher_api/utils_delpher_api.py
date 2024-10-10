import requests
from bs4 import BeautifulSoup
import sruthi
import calendar
import xml.etree.ElementTree as ET
from datetime import datetime, date
from pathlib import Path

API_KEY_FILE = Path("delpher_api", "apikey.txt")


def get_api_key():
    if not API_KEY_FILE.is_file():
        raise FileNotFoundError(f"Put your API key in the file {API_KEY_FILE}")
    with API_KEY_FILE.open() as f:
        key = f.readline()
    if len(key) == 0:
        raise Exception("API key is empty")
    return key.rstrip()


def harvest_article_content(article_id):
    "Function to harvest title and main text for a single article given its id"
    try:
        url = requests.get("http://resolver.kb.nl/resolve?urn=" + article_id + ":ocr")
        soup = BeautifulSoup(url.content, "xml")
        article_text = [p.text for p in soup.findAll("p")]
        article_text = " ".join(article_text)
        article_title = soup.find("title").text
        return {"article_id": article_id, "article_title": article_title, "article_text": article_text}

    except Exception as e:
        return {"article_id": article_id, "article_title": repr(e), "article_text": None}

def standardize_values(record):
    "Function to ensure all values are strings"
    for key, value in record.items():
        if isinstance(value, list):
            record[key] = ", ".join(value)
    return record

def add_query_date(query: str, year: int, month: int):
    "Function to append a 1 month date range to a query"
    _, last_day = calendar.monthrange(year, month)
    start = date(year, month, 1)
    end = date(year, month, last_day)
    return f'{query} AND date within "{start.isoformat()} {end.isoformat()}"'

def harvest_article_ids(query: str="*",
                        year: int=1880,
                        month: int=1,
                        max_records: int=1000,
                        start_record: int=1):
    "Function to harvest articles records for a query in a given month of a year"
    query = add_query_date(query, year, month)

    apikey = get_api_key()

    # Search url is the base url with apikey appended
    search_url = f"https://jsru.kb.nl/sru/sru/{apikey}"

    # customize session
    session = requests.Session()
    session.params = {
        "x-collection": "DDD_artikel"
    }

    # pass the customized session to sruthi
    # and specify other parameters
    client = sruthi.Client(
        url=search_url,
        record_schema='dc',
        sru_version='1.1',
        maximum_records=max_records,
        session=session
    )

    # harvest records
    records = client.searchretrieve(query=query,
                                    start_record=start_record)

    return records



def get_metadata_url(apikey, prefix, identifier):
    "Function to obtain the correct oai url of a newspaper issue"
    if prefix == 'ddd':
        url = f"http://services.kb.nl/mdo/oai/{apikey}?verb=GetRecord&" \
              f"identifier=DDD:ddd:{identifier}&metadataPrefix=didl"
    elif prefix == 'abcddd':
        url = f"http://services.kb.nl/mdo/oai/{apikey}?verb=GetRecord&" \
              f"identifier=KRANTEN:DDD:ddd:{identifier}&metadataPrefix=didl"
    else:
        url = f"http://services.kb.nl/mdo/oai/{apikey}?verb=GetRecord&" \
              f"identifier=KRANTEN:{prefix.upper()}:{prefix.upper()}:{identifier}&metadataPrefix=didl"

    return url

def retrieve_newspaper_metadata(oai_url, namespaces):
    "Function to retrieve the metadata of a newspaper issue"
    # Send a request to the OAI link
    response = requests.get(oai_url)
    response.encoding = 'utf-8'
    content = response.text

    # Check if the request was successful
    # if response.status_code != 200:
    #     print(f"Failed to retrieve XML. HTTP Status Code: {response.status_code}")
    #     exit()

    if "http://www.w3.org/2001/XMLSchema-instance" not in content:
        content = content.replace(
            "<didl:DIDL ",
            '<didl:DIDL xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ',
        )

    # Parse the XML response
    root = ET.fromstring(content)

    newspaper_meta_data = {}
    # Get first level element of the newspaper
    newspaper_component = root.find('oai:GetRecord/oai:record/oai:metadata/didl:DIDL/didl:Item/didl:Component',
                                    namespaces)
    # newspaper_id = newspaper_component.get(f'{{{namespaces.get("dc")}}}identifier')
    # newspaper_meta_data["newspaper_id"] = newspaper_id

    try:
        newspaper_meta_data["newspaper_name"] = newspaper_component.find(
            ".//dc:title", namespaces
        ).text
    except AttributeError:
        newspaper_meta_data["newspaper_name"] = None
    try:
        newspaper_meta_data["newspaper_location"] = newspaper_component.find(
            ".//dcterms:spatial[@xsi:type='dcx:creation']", namespaces
        ).text
    except AttributeError:
        newspaper_meta_data["newspaper_location"] = None
    try:
        newspaper_date = root.find(".//dc:date", namespaces).text
        newspaper_meta_data["newspaper_date"] = datetime.strptime(newspaper_date, "%Y-%m-%d").date()
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

    return newspaper_meta_data


def find_num_records(query: str, year: int = 1860, month: int = 1):
    "Function to find the number of records for a query in a month"
    query = add_query_date(query, year, month)

    apikey = get_api_key()

    # Search url is the base url with apikey appended
    search_url = f"https://jsru.kb.nl/sru/sru/{apikey}"

    # customize session
    session = requests.Session()
    session.params = {
        "x-collection": "DDD_artikel",
        "startRecord": 1,
    }

    # pass the customized session to sruthi
    # and specify other parameters
    client = sruthi.Client(
        url=search_url,
        record_schema='dc',
        sru_version='1.1',
        maximum_records=1,
        session=session
    )

    # harvest records
    records = client.searchretrieve(query=query)
    return int(records.count)