from typing import Dict

import pytest
from dagster._check import CheckError
from dagster._core.storage.db_io_manager import TableSlice
from dagster_snowflake_pyspark.snowflake_pyspark_type_handler import _get_snowflake_options
from dagster_snowflake.resources import SNOWFLAKE_PARTNER_CONNECTION_IDENTIFIER


@pytest.fixture
def snowflake_config() -> Dict[str, str]:
    return {
        "account": "account",
        "user": "user",
        "password": "password",
        "database": "database",
        "warehouse": "warehouse",
    }


@pytest.fixture
def table_slice() -> TableSlice:
    return TableSlice("table_name", "schema_name", "database_name")


def test_get_snowflake_options(snowflake_config, table_slice):
    options = _get_snowflake_options(snowflake_config, table_slice)
    assert options == {
        "sfURL": "account.snowflakecomputing.com",
        "sfUser": "user",
        "sfPassword": "password",
        "sfDatabase": "database",
        "sfWarehouse": "warehouse",
        "sfSchema": "schema_name",
        "APPLICATION": SNOWFLAKE_PARTNER_CONNECTION_IDENTIFIER,
    }


def test_missing_warehouse(snowflake_config, table_slice):
    del snowflake_config["warehouse"]

    with pytest.raises(CheckError):
        _get_snowflake_options(snowflake_config, table_slice)


def test_override_snowflake_partner_connection_id(snowflake_config, table_slice):
    snowflake_config["application"] = "foobar"
    options = _get_snowflake_options(snowflake_config, table_slice)
    assert options.get("APPLICATION") == "foobar"




