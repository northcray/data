import json
import os
import shutil

import requests

from ra.constants import WORKING_DIR


def read_context_file(file_path):
    context_dict = {}
    with open(file_path, 'r') as file:
        for line in file:
            if ": " in line:
                key, value = line.strip().split(": ", 1)
                context_dict[key] = value
    return context_dict

def process_ordnance_survey_package(product, url, zip_name):
    file_path = WORKING_DIR + '/ordnance_survey/' + zip_name
    if not os.path.exists(file_path):
        r = requests.get(url, allow_redirects=True)
        with open(file_path, 'wb') as f:
            f.write(r.content)

    unpack_path = WORKING_DIR + '/ordnance_survey/' + product
    shutil.unpack_archive(file_path, unpack_path)

    versions_txt_path = unpack_path + '/versions.txt'

    if os.path.exists(versions_txt_path):
        context = read_context_file(versions_txt_path)
        product_name = context.get("Product Name")
        file_name = context.get("File Name")
        data_extraction_date = context.get("Data Extraction Date")

        return product_name, file_name, data_extraction_date
    else:
        for file_name in os.listdir(unpack_path):
            if file_name.endswith('_versions.json'):
                print(f"Found: {file_name}")
                with open(unpack_path + '/' + file_name, 'r') as file:
                    data = json.load(file)

                    # Extract values
                    product_name = product
                    file_name = data.get('filename')

                    if data.get('productPublicationDate'):
                        data_extraction_date = data.get('productPublicationDate')
                    if data.get('productCreationDate'):
                        data_extraction_date = data.get('productCreationDate')

                    if data.get('sourceProduct1', {}).get('productName'):
                        product_name = data.get('sourceProduct1', {}).get('productName')
                    if data.get('identifier1Source', {}).get('productName'):
                        product_name = data.get('identifier1Source', {}).get('productName')

                    # product_name = data.get('sourceProduct1', {}).get('productName')
                    # product_publication_date = data.get('sourceProduct1', {}).get('productPublicationDate')
                    # product_version = data.get('sourceProduct1', {}).get('productVersion')

                    return product_name, file_name, data_extraction_date

def clean_ordnance_survey_package(product, file_name):
    product_path = f'{WORKING_DIR}/ordnance_survey/{product}'
    file_path = f'{WORKING_DIR}/ordnance_survey/{file_name}'

    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"File {file_path} has been removed.")
    else:
        print(f"File {file_path} does not exist.")

    if os.path.exists(product_path):
        shutil.rmtree(product_path)
        print(f"Directory {product_path} has been removed.")
    else:
        print(f"Directory {product_path} does not exist.")
