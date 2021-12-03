"""The main entrypoint for ops executed using the DatabricksPySparkStepLauncher.

This script is launched on Databricks using a `spark_python_task` and is passed the following
parameters:

- the DBFS path to the pickled `step_run_ref` file
- the DBFS path to the zipped dagster job
- paths to any other zipped packages which have been uploaded to DBFS.
"""

import os
import pickle
import site
import sys
import tempfile
import zipfile
import logging

from dagster.core.execution.plan.external_step import (
    PICKLED_RECORDS_FILE_NAME,
    run_step_from_ref,
    serialize_dagster_log_records,
)
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DAGSTER_META_KEY


# This won't be set in Databricks but is needed to be non-None for the
# Dagster step to run.
if "DATABRICKS_TOKEN" not in os.environ:
    os.environ["DATABRICKS_TOKEN"] = ""


class DBFSHandler(logging.Handler):
    def __init__(self, records_filepath: str):
        super().__init__()
        self._all_records = []
        self._records_filepath = records_filepath

    def emit(self, record):
        print("--------------")
        print(self._all_records)
        print(self._records_filepath)
        self._all_records.append(record)
        with open(self._records_filepath, "wb") as handle:
            handle.write(serialize_dagster_log_records(self._all_records))


def main(
    step_run_ref_filepath,
    setup_filepath,
    dagster_job_zip,
):
    # Extract any zip files to a temporary directory and add that temporary directory
    # to the site path so the contained files can be imported.
    #
    # We can't rely on pip or other packaging tools because the zipped files might not
    # even be Python packages.
    with tempfile.TemporaryDirectory() as tmp:

        with zipfile.ZipFile(dagster_job_zip) as zf:
            zf.extractall(tmp)
        site.addsitedir(tmp)

        # We can use regular local filesystem APIs to access DBFS inside the Databricks runtime.
        with open(setup_filepath, "rb") as handle:
            databricks_config = pickle.load(handle)

        # sc and dbutils are globally defined in the Databricks runtime.
        databricks_config.setup(dbutils, sc)  # noqa pylint: disable=undefined-variable

        with open(step_run_ref_filepath, "rb") as handle:
            step_run_ref = pickle.load(handle)

        records_filepath = os.path.dirname(step_run_ref_filepath) + "/" + PICKLED_RECORDS_FILE_NAME
        with DagsterInstance.ephemeral() as instance:
            # enable re-execution
            if step_run_ref.parent_run:
                instance.add_run(step_run_ref.parent_run._replace(pipeline_snapshot_id=None))
            instance.add_run(step_run_ref.pipeline_run._replace(pipeline_snapshot_id=None))
            # hack to send all events to a specific filepath
            instance.get_handlers = lambda: [DBFSHandler(records_filepath)]
            # run the step
            print("Running dagster job")  # noqa pylint: disable=print-call
            list(run_step_from_ref(step_run_ref, instance))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
