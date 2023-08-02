from dagster import (
    AssetOut,
    IOManager,
    Output,
    asset,
    build_asset_context,
    job,
    materialize,
    multi_asset,
    op,
)


class TestIOManager(IOManager):
    def handle_output(self, context, obj) -> None:
        # the I/O manager should not be invoked in these tests
        assert False

    def load_input(self, context):
        # the I/O manager should not be invoked in these tests
        assert False


def test_return_none_no_type_annotation():
    @asset
    def returns_none():
        return None

    materialize([returns_none], resources={"io_manager": TestIOManager()})


def test_return_none_with_type_annotation():
    @asset
    def returns_none() -> None:
        return None

    materialize([returns_none], resources={"io_manager": TestIOManager()})


def test_downstream_deps():
    @asset
    def returns_none():
        return None

    @asset(deps=[returns_none])
    def downstream():
        return None

    materialize([returns_none, downstream], resources={"io_manager": TestIOManager()})


def test_downstream_managed_deps():
    @asset
    def returns_none():
        return None

    @asset
    def downstream(returns_none):
        assert returns_none is None

    materialize([returns_none, downstream], resources={"io_manager": TestIOManager()})


def test_conditional_materialization():
    # not totally sure what this should test for yet...
    should_return = False

    @asset(output_required=False)
    def conditional_asset():
        if should_return:
            yield Output(1)

    @asset
    def downstream(conditional_asset):
        return conditional_asset + 1

    result = materialize([conditional_asset, downstream], resources={"io_manager": TestIOManager()})
    assert result.success

    should_return = True
    result = materialize([conditional_asset, downstream])
    assert result.success


def test_multi_asset():
    @multi_asset(outs={"out1": AssetOut(), "out2": AssetOut()})
    def returns_nones():
        return None, None

    @asset(deps=["out1", "out2"])
    def downstream():
        return None

    materialize([returns_nones, downstream], resources={"io_manager": TestIOManager()})


def test_ops():
    @op
    def returns_none():
        return None

    @op
    def asserts_none(x):
        assert x is None

    @job
    def return_none_job():
        asserts_none(returns_none())

    result = return_none_job.execute_in_process()
    assert result.success


# tests to ensure that adding instance access when loading dependencies doesn't make contexts built for
# unit tests require instances


def test_deps_unit_test_flow():
    @asset
    def returns_none():
        return None

    @asset(deps=[returns_none])
    def downstream(context):
        return 1

    asset_context = build_asset_context()

    assert downstream(asset_context) is None


def test_io_unit_test_flow():
    @asset
    def returns_one():
        return 1

    @asset
    def downstream(context, returns_one):
        return returns_one + 1

    asset_context = build_asset_context()

    assert downstream(asset_context, 1) == 2


# test partitions
# test multi assets
