from dagster import asset, Output, DataVersion
from dagster_duckdb import DuckDBResource
from ra.constants import OS_URL, WORKING_DIR, OS_URL

from ra.utils import process_package


@asset(
    group_name="ordnance_survey",
    deps=["e00_geometry"]
)
def usrn(duckdb: DuckDBResource) -> Output[None]:
    product = 'OpenUSRN'
    with duckdb.get_connection() as conn:
        conn.load_extension("json")
        conn.load_extension("httpfs")
        conn.load_extension("spatial")

        path = f"{OS_URL}/{product}/downloads?area=GB&format=GeoPackage"
        df = conn.sql(f"SELECT * FROM read_json('{path}');").df()

        print(df)

        url = df['url'].iloc[0]
        file_name = df['fileName'].iloc[0]

        print(file_name)

        product_name, file_name, data_extraction_date = process_package(product, url, file_name)

        source = WORKING_DIR + '/' + product + '/' + file_name + '.gpkg'

        with duckdb.get_connection() as conn:
            conn.load_extension("spatial")
            conn.execute(
                f"""
                DROP TABLE IF EXISTS usrns;
                CREATE TABLE usrns AS
                WITH gpkg AS (
                    SELECT
                        USRN as usrn,
                        ST_FlipCoordinates(ST_Transform(geometry, 'EPSG:27700', 'EPSG:4326')) AS geometry
                    FROM st_read('{source}')
                )
                SELECT DISTINCT p.usrn, p.geometry
                FROM gpkg p
                INNER JOIN e00 g ON ST_Intersects(p.geometry, g.geom);
                """,
            )

            conn.sql(f"COPY (select * from usrns) TO '{WORKING_DIR}/usrn.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');")

            return Output(None, data_version=DataVersion(data_extraction_date))