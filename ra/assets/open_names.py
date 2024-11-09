import os

from dagster import asset, Output
from dagster_duckdb import DuckDBResource
from ra.constants import OPEN_NAMES_URL, WORKING_DIR
import requests
import shutil

@asset(
    group_name="ordnance_survey",
    deps=["e00_geometry"]
)
def open_names(duckdb: DuckDBResource) -> Output[None]:
    file_path = WORKING_DIR + '/opname_gpkg_gb.zip'

    if not os.path.exists(file_path):
        r = requests.get(OPEN_NAMES_URL, allow_redirects=True)
        with open(file_path, 'wb') as f:
            f.write(r.content)

    unpack_path = WORKING_DIR + '/open_names'
    shutil.unpack_archive(file_path, unpack_path)

    source = unpack_path + '/Data/opname_gb.gpkg'

    with duckdb.get_connection() as conn:
        conn.load_extension("spatial")
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

        conn.sql(f"COPY (from open_names) TO '{WORKING_DIR}/open_names.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');")

        return Output(None)