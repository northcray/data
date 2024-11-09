import os
import subprocess

from dagster import asset, Output
from dagster_duckdb import DuckDBResource
from ra.constants import BEXLEY_INSPIRE_URL, WORKING_DIR
import requests
import shutil


def fetch_inspire_data() -> None:
    file_path = WORKING_DIR + '/London_Borough_of_Bexley.zip'

    r = requests.get(BEXLEY_INSPIRE_URL, allow_redirects=True)
    with open(file_path, 'wb') as f:
        f.write(r.content)

    unpack_path = WORKING_DIR + '/inspire_polygons'
    shutil.unpack_archive(file_path, unpack_path)


@asset(
    group_name="land_registry",
    deps=["overture_buildings"]
)
def inspire_polygons(duckdb: DuckDBResource) -> Output[None]:
    gml_file = WORKING_DIR + '/inspire_polygons/Land_Registry_Cadastral_Parcels.gml'
    if not os.path.exists(gml_file):
        fetch_inspire_data()

    input_file = "data/inspire_polygons/Land_Registry_Cadastral_Parcels.gml"
    output_file = "data/inspire_polygons.geojson"
    clip_file = "data/areas.geojson"

    if not os.path.exists(output_file):
        process = subprocess.Popen(
            f"ogr2ogr -f 'GeoJSON' {output_file} -t_srs 'EPSG:4326' -clipsrc {clip_file} -select INSPIREID {input_file}",
            shell=True,
            stdout=subprocess.PIPE,
        )
        process.wait()

    with duckdb.get_connection() as conn:
        conn.load_extension("spatial")
        conn.sql(f"""
            drop table if exists inspire_polygons;
            create table inspire_polygons as select * from st_read('{output_file}');
        """)

    return Output(None)