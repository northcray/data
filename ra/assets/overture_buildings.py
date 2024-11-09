import subprocess

from dagster import asset, Output
from dagster_duckdb import DuckDBResource

def extract_tiles(source, destination, region_file):
    process = subprocess.Popen(
        f"pmtiles extract {source} {destination} --region={region_file}",
        shell=True,
        stdout=subprocess.PIPE,
    )
    process.wait()

def pmtiles_geojson(source, destination, region_file, params):
    process = subprocess.Popen(
        f"ogr2ogr {destination} {source} -t_srs EPSG:4326 -skipfailures -nln layer -nlt POLYGON -select {params} -clipsrc {region_file}",
        shell=True,
        stdout=subprocess.PIPE,
    )
    process.wait()


@asset(
    group_name="overture",
    deps=["e00_geometry"]
)
def overture_buildings(duckdb: DuckDBResource) -> Output[None]:
    pmtiles_in = "https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2024-08-20/buildings.pmtiles"
    pmtiles_out = "data/overture_buildings.pmtiles"
    region_geojson = "data/e00_geometry.geojson"

    # pmtiles extract https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2024-08-20/buildings.pmtiles data/overture_buildings.pmtiles --region=data/e00_geometry.geojson
    extract_tiles(pmtiles_in, pmtiles_out, region_geojson)

    src = "data/overture_buildings.pmtiles"
    dst = "data/overture_buildings.geojson"
    # ogr2ogr data/overture_buildings.geojson data/overture_buildings.pmtiles -t_srs EPSG:4326 -skipfailures -nln buildings -nlt POLYGON -select @geometry_source,class,height -clipsrc data/e00_geometry.geojson
    pmtiles_geojson(src, dst, region_geojson, params="@geometry_source,class,height")

    with duckdb.get_connection() as conn:
        conn.load_extension("spatial")
        conn.sql(f"""
            drop table if exists overture_buildings;
            create table overture_buildings as select * from st_read('{dst}');
        """)

    return Output(None)