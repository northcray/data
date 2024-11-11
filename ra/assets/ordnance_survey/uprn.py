from dagster import asset, Output, DataVersion, TextMetadataValue
from dagster_duckdb import DuckDBResource
from ra.constants import WORKING_DIR, OS_URL
from ra.utils import process_ordnance_survey_package, clean_ordnance_survey_package


@asset(
    description="Unique Property Reference Numbers (UPRNs) found in AddressBase Premium",
    group_name="ordnance_survey",
    deps=["e00_geometry"]
)
def uprn(duckdb: DuckDBResource) -> Output[None]:
    product = 'OpenUPRN'
    with duckdb.get_connection() as conn:
        conn.load_extension("json")
        conn.load_extension("httpfs")
        conn.load_extension("spatial")

        path = f"{OS_URL}/{product}/downloads?area=GB&format=GeoPackage"
        df = conn.sql(f"SELECT * FROM read_json('{path}');").df()

        url = df['url'].iloc[0]
        zip_name = df['fileName'].iloc[0]

        product_name, file_name, data_extraction_date = process_ordnance_survey_package(product, url, zip_name)

        source = f'{WORKING_DIR}/ordnance_survey/{product}/{file_name}.gpkg'
        conn.execute(
            f"""
            DROP TABLE IF EXISTS uprns;
            CREATE TABLE uprns AS
            WITH gpkg AS (
                SELECT
                    UPRN as uprn,
                    ST_FlipCoordinates(ST_Transform(geom, 'EPSG:27700', 'EPSG:4326')) AS geometry
                FROM st_read('{source}')
            )
            SELECT DISTINCT p.uprn, p.geometry
            FROM gpkg p
            INNER JOIN e00 g ON ST_Intersects(p.geometry, g.geom);
            """,
        )

        clean_ordnance_survey_package(product, zip_name)

        conn.sql(f"COPY (from uprns) TO '{WORKING_DIR}/ordnance_survey/{product}.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');")

        return Output(
            None,
            metadata={
                'product_name': TextMetadataValue(product_name),
                'file_name': TextMetadataValue(file_name),
                'data_extraction_date': TextMetadataValue(data_extraction_date),
            },
            data_version=DataVersion(str(data_extraction_date))
        )