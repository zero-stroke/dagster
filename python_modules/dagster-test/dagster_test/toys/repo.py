import warnings
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Union, cast

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetObservation,
    ExperimentalWarning,
    JobDefinition,
    OpExecutionContext,
    Output,
)
from dagster_dbt import DbtCliInvocation
from dbt.adapters.bigquery import BigQueryAdapter
from google.cloud import bigquery

OPAQUE_ID_SQL_SIGIL = "bigquery_dagster_dbt_v1_opaque_id"
BIGQUERY_METADATA_BYTES_BILLED = "__bigquery_bytes_billed"
BIGQUERY_METADATA_SLOTS_MS = "__bigquery_slots_ms"
BIGQUERY_METADATA_JOB_IDS = "__bigquery_job_ids"


# squelch experimental warnings since we often include experimental things in toys for development
warnings.filterwarnings("ignore", category=ExperimentalWarning)

import os
from pathlib import Path
from typing import Any, Mapping, Optional

from dagster import (
    Definitions,
    EnvVar,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
from dagster_gcp import BigQueryResource

OUTPUT_NON_ASSET_SIGIL = "__bigquery_query_metadata_"
DEFAULT_BQ_REGION = "region-us"

bigquery_dbt_dir = Path(__file__).parent / "bigquery_dbt"
bigquery_dbt = DbtCliResource(project_dir=os.fspath(bigquery_dbt_dir))
bigquery_dbt.cli(["deps"]).wait()
bigquery_dbt_parse_invocation = bigquery_dbt.cli(["parse"]).wait()
bigquery_dbt_manifest_path = bigquery_dbt_parse_invocation.target_path.joinpath("manifest.json")


class BigQueryTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props) -> Optional[str]:
        return "bigquery_jaffle_shop"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return (
            super(BigQueryTranslator, self)
            .get_asset_key(dbt_resource_props)
            .with_prefix("bigquery")
        )


@dataclass
class BigQueryCostInfo:
    asset_key: AssetKey
    partition: Optional[str]
    job_id: Optional[str]
    slots_ms: int
    bytes_billed: int

    @property
    def asset_partition_key(self) -> str:
        return (
            f"{self.asset_key.to_string()}:{self.partition}"
            if self.partition
            else self.asset_key.to_string()
        )


def marker_asset_key_for_job(job: JobDefinition) -> AssetKey:
    return AssetKey(path=[f"{OUTPUT_NON_ASSET_SIGIL}{job.name}"])


def get_asset_key_for_output(
    context: Union[OpExecutionContext, AssetExecutionContext], output_name: str
) -> Optional[AssetKey]:
    asset_info = context.job_def.asset_layer.asset_info_for_output(
        node_handle=context.op_handle, output_name=output_name
    )
    if asset_info is None:
        return None
    return asset_info.key


def extract_asset_info_from_event(context, dagster_event, record_observation_usage):
    if isinstance(dagster_event, AssetMaterialization):
        return dagster_event.asset_key, dagster_event.partition

    if isinstance(dagster_event, AssetObservation) and record_observation_usage:
        return dagster_event.asset_key, dagster_event.partition

    if isinstance(dagster_event, AssetObservation):
        return None, None

    if isinstance(dagster_event, Output):
        asset_key = get_asset_key_for_output(context, dagster_event.output_name)
        partition_key = None
        if asset_key and context._step_execution_context.has_asset_partitions_for_output(  # noqa: SLF001
            dagster_event.output_name
        ):
            # We associate cost with the first partition key in the case that an output
            # maps to multiple partitions. This is a temporary solution, but partition key
            # is not used in Insights at the moment.
            # TODO: Find a long-term solution for this
            partition_key = next(
                iter(context.asset_partition_keys_for_output(dagster_event.output_name))
            )

        return asset_key, partition_key

    return None, None


def build_bigquery_cost_metadata(
    job_ids: Optional[List[str]], bytes_billed: int, slots_ms: int
) -> Mapping[str, Any]:
    metadata: Mapping[str, Any] = {
        BIGQUERY_METADATA_BYTES_BILLED: bytes_billed,
        BIGQUERY_METADATA_SLOTS_MS: slots_ms,
    }
    if job_ids:
        metadata[BIGQUERY_METADATA_JOB_IDS] = job_ids
    return metadata


def _information_schema_cost_query(invocation_id: str, region: str) -> str:
    return rf"""
        SELECT
        job_id,
        regexp_extract(query, r"{OPAQUE_ID_SQL_SIGIL}\[\[\[(.*?):{invocation_id}\]\]\]") as unique_id,
        total_bytes_billed AS bytes_billed,
        total_slot_ms AS slots_ms
        FROM `{region}`.INFORMATION_SCHEMA.JOBS
        WHERE query like '%{invocation_id}%'
    """


def _query_from_adapter(
    adapter: BigQueryAdapter, invocation_id: str, region: str
) -> Iterator[bigquery.Row]:
    with adapter.connection_named("dagster_insights:bigquery_cost"):
        client: bigquery.Client = adapter.connections.get_thread_connection().handle
        invocation_cost_query = _information_schema_cost_query(invocation_id, region)
        query_result = client.query(invocation_cost_query)
        for row in query_result:
            yield row


def _query_from_client(
    client: bigquery.Client, invocation_id: str, region: str
) -> Iterator[bigquery.Row]:
    invocation_cost_query = _information_schema_cost_query(invocation_id, region)
    query_result = client.query(invocation_cost_query)
    for row in query_result:
        yield row


def dbt_with_bigquery_insights(
    context: Union[OpExecutionContext, AssetExecutionContext],
    dbt_cli_invocation: DbtCliInvocation,
    bigquery_region: str,
    dagster_events: Optional[
        Iterable[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]
    ] = None,
    record_observation_usage: bool = True,
    bigquery_client: Optional[bigquery.Client] = None,
) -> Iterator[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]:
    """Wraps a dagster-dbt invocation to associate each BigQuery query with the produced
    asset materializations. This allows the cost of each query to be associated with the asset
    materialization that it produced.

    If called in the context of an op (rather than an asset), filters out any Output events
    which do not correspond with any output of the op.

    Args:
        context (AssetExecutionContext): The context of the asset that is being materialized.
        dbt_cli_invocation (DbtCliInvocation): The invocation of the dbt CLI to wrap.
        dagster_events (Optional[Iterable[Union[Output, AssetObservation, AssetCheckResult]]]):
            The events that were produced by the dbt CLI invocation. If not provided, it is assumed
            that the dbt CLI invocation has not yet been run, and it will be run and the events
            will be streamed.
        record_observation_usage (bool): If True, associates the usage associated with
            asset observations with that asset. Default is True.

    **Example:**

    .. code-block:: python

        @dbt_assets(manifest=DBT_MANIFEST_PATH)
        def jaffle_shop_dbt_assets(
            context: AssetExecutionContext,
            dbt: DbtCliResource,
        ):
            dbt_cli_invocation = dbt.cli(["build"], context=context)
            yield from dbt_with_bigquery_insights(context, dbt_cli_invocation)
    """
    if dagster_events is None:
        dagster_events = dbt_cli_invocation.stream()

    asset_info_by_unique_id = {}
    for dagster_event in dagster_events:
        if isinstance(dagster_event, (AssetMaterialization, AssetObservation, Output)):
            unique_id = dagster_event.metadata["unique_id"].value
            asset_key, partition = extract_asset_info_from_event(
                context, dagster_event, record_observation_usage
            )
            if not asset_key:
                asset_key = marker_asset_key_for_job(context.job_def)
            asset_info_by_unique_id[unique_id] = (asset_key, partition)

        yield dagster_event

    marker_asset_key = marker_asset_key_for_job(context.job_def)
    run_results_json = dbt_cli_invocation.get_artifact("run_results.json")
    invocation_id = run_results_json["metadata"]["invocation_id"]

    if bigquery_client:
        query_result = _query_from_client(
            bigquery_client, invocation_id, cast(str, bigquery_region)
        )
    elif isinstance(dbt_cli_invocation.adapter, BigQueryAdapter):
        query_result = _query_from_adapter(
            dbt_cli_invocation.adapter, invocation_id, bigquery_region
        )
    else:
        context.log.exception("Could not access BigQuery client from the dbt adapter.")
        raise

    cost_by_asset = defaultdict(list)
    try:
        for row in query_result:
            if not row.unique_id:
                continue
            asset_key, partition = asset_info_by_unique_id.get(
                row.unique_id, (marker_asset_key, None)
            )
            if row.bytes_billed or row.slots_ms:
                cost_info = BigQueryCostInfo(
                    asset_key, partition, row.job_id, row.bytes_billed, row.slots_ms
                )
                cost_by_asset[cost_info.asset_partition_key].append(cost_info)
    except:
        context.log.exception("Could not query information_schema for BigQuery cost information")
        raise

    for cost_info_list in cost_by_asset.values():
        bytes_billed = sum(item.bytes_billed for item in cost_info_list)
        slots_ms = sum(item.slots_ms for item in cost_info_list)
        job_ids = [item.job_id for item in cost_info_list]
        asset_key = cost_info_list[0].asset_key
        partition = cost_info_list[0].partition
        context.log_event(
            AssetObservation(
                asset_key=asset_key,
                partition=partition,
                metadata=build_bigquery_cost_metadata(job_ids, bytes_billed, slots_ms),
            )
        )


@dbt_assets(manifest=bigquery_dbt_manifest_path, dagster_dbt_translator=BigQueryTranslator())
def jaffle_shop_bigquery_dbt_assets(
    context: AssetExecutionContext,
    bigquery_dbt: DbtCliResource,
    bigquery: BigQueryResource,
):
    dbt_cli_invocation = bigquery_dbt.cli(["build"], context=context)
    with bigquery.get_client() as bigquery_client:
        yield from dbt_with_bigquery_insights(context, dbt_cli_invocation, "region-us")


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
