from dagster import Definitions, load_assets_from_package_module
from dagster_duckdb import DuckDBResource

from ra import assets  # noqa: TID252

all_assets = load_assets_from_package_module(assets)

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(database="data/main.duckdb"),
    }
)
