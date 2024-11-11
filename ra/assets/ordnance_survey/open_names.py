import os
import shutil

from dagster import asset, Output, TextMetadataValue
from dagster_duckdb import DuckDBResource
from ra.constants import WORKING_DIR, OS_URL

from ra.utils import process_ordnance_survey_package, clean_ordnance_survey_package


@asset(
    description="A comprehensive dataset of place names, roads numbers and postcodes for Great Britain.",
    group_name="ordnance_survey",
    deps=["e00_geometry"]
)
def open_names(duckdb: DuckDBResource) -> Output[None]:
    product = 'OpenNames'
    with duckdb.get_connection() as conn:
        conn.load_extension("json")
        conn.load_extension("httpfs")
        conn.load_extension("spatial")

        path = f"{OS_URL}/{product}/downloads?area=GB&format=GeoPackage"
        df = conn.sql(f"SELECT * FROM read_json('{path}');").df()

        url = df['url'].iloc[0]
        file_name = df['fileName'].iloc[0]

        process_ordnance_survey_package(product, url, file_name)

        source = f'{WORKING_DIR}/ordnance_survey/{product}/Data/opname_gb.gpkg'

        conn.execute(
            f"""
            DROP TABLE IF EXISTS open_names;
            CREATE TABLE open_names AS
            WITH names_by_zip AS (
                SELECT 
                    id,
                    name1,
                    name2,
                    type,
                    local_type,
                    populated_place,
                    ST_FlipCoordinates(ST_Transform(geometry, 'EPSG:27700', 'EPSG:4326')) AS geometry
                FROM st_read('{source}')
                WHERE postcode_district IN('DA14','DA5')
            ),
            defined_area AS (FROM e00)
            SELECT * FROM names_by_zip p
            WHERE EXISTS (
                SELECT 1 
                FROM defined_area g
                WHERE ST_Intersects(p.geometry, g.geom)
            );
            """,
        )

        clean_ordnance_survey_package(product, file_name)

        conn.sql(f"COPY (from open_names) TO '{WORKING_DIR}/ordnance_survey/{product}.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');")

        return Output(
            None,
            metadata={
                'product_name': TextMetadataValue(product),
                'file_name': TextMetadataValue(file_name),
                'data_extraction_date': TextMetadataValue('unknown'),
            },
        )