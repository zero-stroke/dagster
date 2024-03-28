import warnings

from dagster import ExperimentalWarning

# squelch experimental warnings since we often include experimental things in toys for development
warnings.filterwarnings("ignore", category=ExperimentalWarning)

import os
from pathlib import Path
from typing import Any, Mapping, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    EnvVar,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
from dagster_gcp import BigQueryResource


bigquery_dbt_dir = Path(__file__).parent / "bigquery_dbt"
bigquery_dbt = DbtCliResource(project_dir=os.fspath(bigquery_dbt_dir))
bigquery_dbt.cli(["deps"]).wait()
bigquery_dbt_parse_invocation = bigquery_dbt.cli(["parse"]).wait()
bigquery_dbt_manifest_path = bigquery_dbt_parse_invocation.target_path.joinpath("manifest.json")

class BigQueryTranslator(DagsterDbtTranslator):
    @classmethod
    def get_group_name(cls, dbt_resource_props) -> Optional[str]:
        return "bigquery_jaffle_shop"

    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return super().get_asset_key(dbt_resource_props).with_prefix("bigquery")

@dbt_assets(manifest=bigquery_dbt_manifest_path, dagster_dbt_translator=BigQueryTranslator())
def jaffle_shop_bigquery_dbt_assets(
    context: AssetExecutionContext,
    bigquery_dbt: DbtCliResource,
    bigquery: BigQueryResource,
):
    dbt_cli_invocation = bigquery_dbt.cli(
        ["build"], context=context, dagster_dbt_translator=BigQueryTranslator()
    )
    yield from dbt_cli_invocation.stream()


defs = Definitions(
    assets=[
        jaffle_shop_bigquery_dbt_assets,
    ],
    resources={
        "bigquery_dbt": bigquery_dbt,
        "bigquery": BigQueryResource(
            project=EnvVar("TOYS_BIGQUERY_PROJECT"),
            gcp_credentials=EnvVar("TOYS_BIGQUERY_CREDENTIALS"),
        ),
    },
)