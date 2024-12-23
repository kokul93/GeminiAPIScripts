import os
from google.api_core.client_options import ClientOptions
from google.cloud.documentai_v1.types import Document
import google
import json
import requests
import math
from PyPDF2 import PdfReader, PdfWriter
import io


from google.cloud import documentai_v1 as documentai

base_parent = "/Users/kokularajbaskaran/Documents/Project_paralegal"


file_path = os.path.join(base_parent, "NLR/nlr.jsonl")


para_data = []

with open(file_path, "r") as file:
    for line in file:
        line_dict = json.loads(line.strip())
        para_data.append(line_dict)


project_json_path = "/Users/kokularajbaskaran/Documents/Project_paralegal/google_api_key/paraLegalProcessor.json"

with open(project_json_path) as file:
    project_data = json.load(file)


def get_pdf_page_numbers(pdf_url: str) -> int:
    response = requests.get(pdf_url)
    response.raise_for_status()  # Ensure the request was successful
    pdf_content = response.content

    pdf_reader = PdfReader(io.BytesIO(pdf_content))
    total_page_num = len(pdf_reader.pages)

    return total_page_num


def get_pdf_into_chunks(pdf_url: str, chunk_size: int = 15) -> list:
    pdf_bucket = []
    response = requests.get(pdf_url)
    response.raise_for_status()  # Ensure the request was successful
    pdf_content = response.content

    pdf_reader = PdfReader(io.BytesIO(pdf_content))
    total_page_num = len(pdf_reader.pages)

    for start_page in range(0, total_page_num, chunk_size):
        end_page = min(start_page + chunk_size, total_page_num)
        print(f"\t\tProcess on going from {start_page+1} to {end_page}")
        pdf_writer = PdfWriter()

        for page_num in range(start_page, end_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        chunk_stream = io.BytesIO()

        pdf_writer.write(chunk_stream)
        chunk_stream.seek(0)
        pdf_bucket.append(chunk_stream)

    return pdf_bucket


def get_pdf_pages_into_chuncks(pdf_path: str, chunk_size: int = 15) -> list:
    pdf_bucket = []
    pdf_reader = PdfReader(pdf_path)
    total_page_num = len(pdf_reader.pages)

    for start_page in range(0, total_page_num, chunk_size):
        end_page = min(start_page + chunk_size, total_page_num)
        print(f"\t\tProcess on going from {start_page+1} to {end_page}")
        pdf_writer = PdfWriter()

        for page_num in range(start_page, end_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        chunk_stream = io.BytesIO()

        pdf_writer.write(chunk_stream)
        chunk_stream.seek(0)
        pdf_bucket.append(chunk_stream)

    return pdf_bucket


def get_google_documentai(
    pdf_path: str,
) -> google.cloud.documentai_v1.types.document_processor_service.ProcessResponse:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
        base_parent, "google_api_key/paraLegal_nlr-extraction-a10621317fdc.json"
    )

    global project_data

    response_list = []

    PROJECT_ID = project_data["project_id"]
    LOCATION = project_data["location"]
    PROCESSOR_ID = project_data["processor_id"]

    MIME_TYPE = "application/pdf"

    docai_client = documentai.DocumentProcessorServiceClient(
        client_options=ClientOptions(
            api_endpoint=f"{LOCATION}-documentai.googleapis.com"
        )
    )

    RESOURCE_NAME = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    response_bucket = get_pdf_pages_into_chuncks(pdf_path)

    for response in response_bucket:

        raw_document = documentai.RawDocument(
            content=response.read(), mime_type=MIME_TYPE
        )

        request = documentai.ProcessRequest(
            name=RESOURCE_NAME, raw_document=raw_document
        )

        result = docai_client.process_document(request=request)

        document_object = Document.to_dict(result.document)

        response_list.append(document_object)

    return response_list


def get_google_documentai_s3(
    pdf_url: str, chunk_size: int = 15
) -> google.cloud.documentai_v1.types.document_processor_service.ProcessResponse:
    # Set the Google Application Credentials
    credentials_path = "/Users/kokularajbaskaran/Documents/Project_paralegal/google_api_key/paraLegal_nlr-extraction-a10621317fdc.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    global project_data

    response_list = []

    PROJECT_ID = project_data["project_id"]
    LOCATION = project_data["location"]
    PROCESSOR_ID = project_data["processor_id"]

    MIME_TYPE = "application/pdf"

    # Initialize the Document AI client
    docai_client = documentai.DocumentProcessorServiceClient(
        client_options=ClientOptions(
            api_endpoint=f"{LOCATION}-documentai.googleapis.com"
        )
    )

    RESOURCE_NAME = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)

    # Fetch the PDF file content from the S3 URL
    response_bucket = get_pdf_into_chunks(pdf_url)

    for response in response_bucket:

        raw_document = documentai.RawDocument(
            content=response.read(), mime_type=MIME_TYPE
        )

        request = documentai.ProcessRequest(
            name=RESOURCE_NAME, raw_document=raw_document
        )

        result = docai_client.process_document(request=request)

        document_object = Document.to_dict(result.document)

        response_list.append(document_object)

    return response_list


def extract_pdf_link(id: str, data: list[str] = para_data) -> dict[str, str]:
    for item in data:
        if item["_id"] == id:
            return item["link"]

    return ""


def filter_above_score(
    dict_data: dict[str, float], score: int
) -> list[tuple[str, float]]:
    non_NaN = {}
    NaN_list = []
    for key, value in dict_data.items():
        if value >= score and not math.isnan(value):
            non_NaN[key] = value
        elif math.isnan(value):
            NaN_list.append((key, value))

    sorted_data = sorted(non_NaN.items(), key=lambda item: item[1], reverse=True)

    merge_list = NaN_list + sorted_data

    return merge_list


def filter_above_score(
    dict_data: dict[str, float], score: int
) -> list[tuple[str, float]]:
    non_NaN = {}
    NaN_list = []
    for key, value in dict_data.items():
        if value >= score and not math.isnan(value):
            non_NaN[key] = value
        elif math.isnan(value):
            NaN_list.append((key, value))

    sorted_data = sorted(non_NaN.items(), key=lambda item: item[1], reverse=True)

    merge_list = NaN_list + sorted_data

    return merge_list


with open(os.path.join(base_parent, "OCR_extraction/volume_id.json"), "r") as file:
    volume_id_dict = json.load(file)


def find_volume(id: str) -> str:
    global volume_id_dict

    for volume in volume_id_dict.keys():
        if id in volume_id_dict[volume]:
            return volume

    return ""


def save_data(data: list[str], file_name: str):
    with open(file_name, "w") as file:
        json.dump(data, file, indent=4)


def load_data(file_name):
    if os.path.exists(file_name):
        with open(file_name, "r") as file:
            return json.load(file)
    return []


def save_in_folder(name: str, data: dict[str, any], output_save_folder: str) -> None:
    folder_path = os.path.join(output_save_folder, name)
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{name}.json")

    # Save the name to a .json file
    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)


def load_json_dict(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r") as file:
            return json.load(file)
    else:
        return {}


def response_listDict(response_data) -> list[dict[str, any]]:
    responce_dict_list = []
    for response in response_data:
        reposnse_dict = documentai.Document.to_dict(response)
        responce_dict_list.append(reposnse_dict)

    return responce_dict_list


def break_text_to_pages(response_data: list[dict[str, any]]) -> dict[str, any]:
    page_wise_text: dict[str, str] = {}
    page_num = 0
    for pdf_chunk in response_data:
        text = pdf_chunk["text"]
        for page_data in pdf_chunk["pages"]:
            page_num += 1
            start_index = int(
                page_data["layout"]["text_anchor"]["text_segments"][0]["start_index"]
            )
            end_index = int(
                page_data["layout"]["text_anchor"]["text_segments"][0]["end_index"]
            )

            page_wise_text[f"page_{page_num}"] = text[start_index:end_index]

        return page_wise_text


def extract_decision_text(response_data: list[dict[str, any]]) -> str:
    decision_text = ""
    for response in response_data:
        decision_text += str(response.text)
        decision_text += "\n"

    return decision_text


def get_json_path(_id):
    base_folder = "/Users/kokularajbaskaran/Documents/Project_paralegal/OCR_extraction/Completed_OCR"

    return os.path.join(base_folder, find_volume(_id), _id, _id + ".json")
