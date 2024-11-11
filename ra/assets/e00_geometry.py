from dagster import asset
from dagster_duckdb import DuckDBResource
import requests

from ra.constants import E00_AREAS, WORKING_DIR


# Function to fetch WKT data from the API
def fetch_wkt_data(area_id):
    url = f"https://statistics.data.gov.uk/resource.json?uri=http%3A%2F%2Fstatistics.data.gov.uk%2Fid%2Fstatistical-geography%2F{area_id}%2Fgeometry"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        wkt_value = data[0].get("http://www.opengis.net/ont/geosparql#asWKT", [{}])[0].get("@value")
        return wkt_value
    else:
        print(f"Failed to retrieve data for {area_id}")
        return None

@asset(
    description="Geometry for select E00 Output Areas",
    group_name="national_statistics",
)
def e00_geometry(duckdb: DuckDBResource) -> None:
    # Dictionary to store the WKT values
    wkt_data = {}

    # Fetch WKT for each area ID and store it in the dictionary
    for area_id in E00_AREAS:
        wkt_value = fetch_wkt_data(area_id)
        if wkt_value:
            wkt_data[area_id] = wkt_value

    # Format the data as a list of lists for executemany
    wkt_data_list = [[area_id, wkt] for area_id, wkt in wkt_data.items()]

    with duckdb.get_connection() as conn:
        conn.load_extension("spatial")

        # Create and populate a temporary table with the WKT data
        conn.execute("drop table if exists e00; CREATE TABLE e00 (id VARCHAR, wkt VARCHAR, geom geometry);")
        conn.executemany("INSERT INTO e00 VALUES (?, ?, NULL)", wkt_data_list)
        conn.execute("UPDATE e00 SET geom = ST_GeomFromText(wkt);")
        conn.execute("ALTER TABLE e00 DROP COLUMN wkt;")

        # Query to transform WKT into GeoJSON-ready format
        geojson_query = f"""
        COPY (FROM e00) TO '{WORKING_DIR}/e00_geometry.geojson' WITH (
          FORMAT GDAL, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES'
        );
        """

        # Execute the query and write to GeoJSON file
        conn.execute(geojson_query)