import importlib.util
import os

import pytest

import dagster as dg

snippets_folder = dg.file_relative_path(__file__, "../docs_beta_snippets/")

EXCLUDED_FILES = {
    # TEMPORARY: dagster-cloud environment is weird and confusing
    f"{snippets_folder}/guides/data-assets/quality-testing/freshness-checks/anomaly-detection.py",
    f"{snippets_folder}/guides/data-modeling/metadata/plus-references.py",
    f"{snippets_folder}/dagster-plus/insights/snowflake/snowflake-dbt-asset-insights.py",
    f"{snippets_folder}/dagster-plus/insights/snowflake/snowflake-resource-insights.py",
    f"{snippets_folder}/dagster-plus/insights/google-bigquery/bigquery-resource-insights.py",
    # see DOC-375
    f"{snippets_folder}/guides/data-modeling/asset-factories/python-asset-factory.py",
    f"{snippets_folder}/guides/data-modeling/asset-factories/simple-yaml-asset-factory.py",
    f"{snippets_folder}/guides/data-modeling/asset-factories/advanced-yaml-asset-factory.py",
    # setuptools.setup() eventually parses the command line that caused setup() to be called.
    # it errors because the command line for this test is for pytest and doesn't align with the arguments
    # setup() expects. So omit files that call setup() since they cannot be loaded without errors.
    f"{snippets_folder}/dagster-plus/deployment/serverless/runtime-environment/data_files_setup.py",
    f"{snippets_folder}/dagster-plus/deployment/serverless/runtime-environment/example_setup.py",
    # these files are part of a completed project and the import references are failing the tests
    f"{snippets_folder}/guides/tutorials/etl_tutorial_completed/etl_tutorial/assets.py",
    f"{snippets_folder}/guides/tutorials/etl_tutorial_completed/etl_tutorial/definitions.py",
    # there are no components defined in the snippets and so it would fail to load
    f"{snippets_folder}/guides/components/existing-project/definitions-after.py",
    # there are no components defined in the snippets and so it would fail to load
    f"{snippets_folder}/guides/components/index/5-definitions.py",
    f"{snippets_folder}/guides/components/existing-project/6-initial-definitions.py",
    f"{snippets_folder}/guides/components/existing-project/7-updated-definitions.py",
    f"{snippets_folder}/guides/components/migrating-definitions/2-definitions-before.py",
    f"{snippets_folder}/guides/components/migrating-definitions/5-elt-nested-definitions.py",
    f"{snippets_folder}/guides/components/migrating-definitions/7-definitions-after.py",
    f"{snippets_folder}/guides/components/migrating-definitions/10-definitions-after-all.py",
    # there are no defs defined in the snippets and so it would fail to load
    f"{snippets_folder}/guides/dg/migrating-definitions/2-definitions-before.py",
    f"{snippets_folder}/guides/dg/migrating-definitions/4-definitions-after.py",
    f"{snippets_folder}/guides/dg/migrating-definitions/7-definitions-after-all.py",
    f"{snippets_folder}/guides/dg/migrating-project/6-initial-definitions.py",
    f"{snippets_folder}/guides/dg/migrating-project/7-updated-definitions.py",
}
EXCLUDED_DIRS = {
    # integrations are excluded because they have external dependencies that are easier to manage in
    # a separate tox environment
    f"{snippets_folder}/integrations",
}


def get_python_files(directory):
    for root, dirs, files in os.walk(directory):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if os.path.join(root, d) not in EXCLUDED_DIRS]

        for file in files:
            if file.endswith(".py"):
                yield os.path.join(root, file)


@pytest.mark.parametrize("file_path", get_python_files(snippets_folder))
def test_file_loads(file_path):
    if file_path in EXCLUDED_FILES:
        pytest.skip(f"Skipped {file_path}")
        return
    spec = importlib.util.spec_from_file_location("module", file_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        pytest.fail(f"Failed to load {file_path}: {e!s}")
