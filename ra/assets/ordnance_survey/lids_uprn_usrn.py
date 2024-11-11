from dagster import asset, Output, DataVersion, TextMetadataValue
from dagster_duckdb import DuckDBResource
from ra.constants import WORKING_DIR, OS_URL
from ra.utils import process_ordnance_survey_package, clean_ordnance_survey_package


@asset(
    description="",
    group_name="ordnance_survey",
    deps=["e00_geometry"]
)
def lids_uprn_usrn(duckdb: DuckDBResource) -> Output[None]:
    product = 'LIDS'
    with duckdb.get_connection() as conn:
        conn.load_extension("json")
        conn.load_extension("httpfs")
        conn.load_extension("spatial")

        path = f"{OS_URL}/{product}/downloads?area=GB&fileName=lids-2024-09_csv_BLPU-UPRN-Street-USRN-11.zip&format=CSV"
        df = conn.sql(f"SELECT * FROM read_json('{path}');").df()

        url = df['url'].iloc[0]
        zip_name = df['fileName'].iloc[0]

        product_name, file_name, data_extraction_date = process_ordnance_survey_package(product, url, zip_name)

        source = f'{WORKING_DIR}/ordnance_survey/{product}/{file_name}.csv'
        conn.execute(
            f"""
            CREATE TEMP TABLE temp_uprn_usrn_relationships AS
            SELECT IDENTIFIER_1 AS uprn, IDENTIFIER_2 AS usrn
            FROM read_csv('{source}');

            DROP TABLE IF EXISTS lids_uprn_usrns;
            CREATE TABLE lids_uprn_usrns AS
            SELECT temp.uprn, temp.usrn
            FROM temp_uprn_usrn_relationships temp
            INNER JOIN uprns p ON temp.uprn = p.uprn;
            """,
        )

        clean_ordnance_survey_package(product, zip_name)

        conn.sql(f"COPY (from lids_uprn_usrns) TO '{WORKING_DIR}/ordnance_survey/{product}.csv';")

        return Output(
            None,
            metadata={
                'product_name': TextMetadataValue(product_name),
                'file_name': TextMetadataValue(file_name),
                'data_extraction_date': TextMetadataValue(data_extraction_date),
            },
            data_version=DataVersion(str(data_extraction_date))
        )