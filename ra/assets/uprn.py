import os

from dagster import asset, Output, DataVersion, TextMetadataValue, IntMetadataValue
import pandas as pd
from dagster_duckdb import DuckDBResource
from ra.constants import UPRN_URL, WORKING_DIR, H3_6_AREA
import requests
import shutil

from ra.utils import read_context_file


@asset(
    group_name="ordnance_survey",
    deps=["e00_geometry"]
)
def uprn(duckdb: DuckDBResource) -> Output[None]:
    file_path = WORKING_DIR + '/urpn.zip'

    if not os.path.exists(file_path):
        r = requests.get(UPRN_URL, allow_redirects=True)
        with open(file_path, 'wb') as f:
            f.write(r.content)

    unpack_path = WORKING_DIR + '/uprn'
    shutil.unpack_archive(file_path, unpack_path)

    version_path = unpack_path + '/versions.txt'
    context = read_context_file(version_path)
    product_name = context.get("Product Name")
    file_name = context.get("File Name")
    data_extraction_date = context.get("Data Extraction Date")
    source = unpack_path + '/' + file_name + '.csv'

    with duckdb.get_connection() as conn:
        conn.execute("INSTALL h3 FROM community;LOAD h3;")
        conn.load_extension("spatial")
        conn.execute(
            f"""
            drop table if exists uprn;
            create table uprn as
                with point_data AS (
                    SELECT
                        UPRN as uprn,
                        ST_Point(LONGITUDE, LATITUDE) as geom,
                        h3_latlng_to_cell(LATITUDE, LONGITUDE, 6) as h3_6
                    FROM read_csv('{source}')
                    WHERE h3_6 = h3_string_to_h3('{H3_6_AREA}')
                ),
                area as (from e00)
                SELECT 
                    p.uprn,
                    p.geom
                FROM point_data p
                WHERE EXISTS (
                    SELECT 1 
                    FROM area g
                    WHERE ST_Intersects(p.geom, g.geom)
                );
            """,
        )

        conn.sql(f"COPY (from uprn) TO '{WORKING_DIR}/uprn.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');")

        return Output(
            None,
            metadata={
                'product_name': TextMetadataValue(product_name),
                'file_name': TextMetadataValue(file_name),
                'data_extraction_date': TextMetadataValue(data_extraction_date),
            },
            data_version=DataVersion(str(data_extraction_date))
        )