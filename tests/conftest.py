"""Fixtures partagées. La session Spark locale sert uniquement aux tests :
les traitements finaux tournent sur YARN."""

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from bigdata.common.spark import build_spark

    conf = {"app_name": "tests-bigdata", "shuffle_partitions": 2}
    session = build_spark(conf, master="local[1]")
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
