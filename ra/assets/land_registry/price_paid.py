import os

from dagster import asset, Output, AssetExecutionContext
from dagster_duckdb import DuckDBResource

import requests
import shutil

path = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"

def download_csv(csv_url, csv_file):
    with requests.get(csv_url, stream=True) as r:
        with open(csv_file, "wb") as f:
            shutil.copyfileobj(r.raw, f)

@asset(
    description="Identify date of property transfers in order to remove old records and mark a property as a new lead",
    group_name="land_registry",
    deps=["addresses"]
)
def price_paid(context: AssetExecutionContext, duckdb: DuckDBResource) -> Output[None]:
    with duckdb.get_connection() as conn:
        conn.install_extension("httpfs")
        conn.install_extension("spatial")
        conn.load_extension("httpfs")
        conn.load_extension("spatial")

        file_path = "data/price_paid.csv"
        if not os.path.exists(file_path):
            context.log.debug(f"Downloading dataset to {file_path}")
            download_csv(path, file_path)
            context.log.debug(f"Download complete")

        context.log.debug(f"Transform/load data from {file_path}")
        conn.execute(
            f"""
            DROP TABLE IF EXISTS price_paid;
            CREATE TABLE price_paid AS 
            SELECT csv.*
            FROM read_csv('{file_path}',
                delim=',',
                quote='"',
                header = false,
                null_padding = true,
                dateformat = '%Y-%m-%d %H:%M',
                columns = {{
                    'id': 'VARCHAR',
                    'price': 'BIGINT',
                    'date_of_transfer': 'DATE',
                    'postcode': 'VARCHAR',
                    'property_type': 'VARCHAR',
                    'old_new': 'VARCHAR',
                    'duration': 'VARCHAR',
                    'paon': 'VARCHAR',
                    'saon': 'VARCHAR',
                    'street': 'VARCHAR',
                    'locality': 'VARCHAR',
                    'town_city': 'VARCHAR',
                    'district': 'VARCHAR',
                    'county': 'VARCHAR',
                    'ppd_category_type': 'VARCHAR',
                    'record_status': 'VARCHAR'
                }}) csv
            INNER JOIN address_data a ON csv.postcode = a.postcode;
            """
        )

        context.log.debug(f"Load complete")

        if os.path.exists(file_path):
            os.remove(file_path)

        # Log statistics about the import
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT postcode) as unique_postcodes
            FROM price_paid
        """).fetchall()

        context.log.info(
            f"Imported {result[0][0]} records with {result[0][1]} unique postcodes"
        )

        return Output(None)