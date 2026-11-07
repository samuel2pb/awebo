from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

from awebo.exceptions import WriteError
from awebo.io.ddl import PartitionTerm, column_definitions


def test_nested_schema_renders_as_duckdb_types() -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "amount", DecimalType(18, 2), required=False),
        NestedField(3, "ts", TimestamptzType(), required=False),
        NestedField(4, "tags", ListType(5, StringType()), required=False),
        NestedField(
            6,
            "attrs",
            StructType(NestedField(7, "k", StringType()), NestedField(8, "n", LongType())),
        ),
        NestedField(9, "m", MapType(10, StringType(), 11, LongType())),
    )

    assert column_definitions(schema) == [
        ("id", "BIGINT NOT NULL"),
        ("amount", "DECIMAL(18, 2)"),
        ("ts", "TIMESTAMPTZ"),
        ("tags", "VARCHAR[]"),
        ("attrs", 'STRUCT("k" VARCHAR, "n" BIGINT)'),
        ("m", "MAP(VARCHAR, BIGINT)"),
    ]


@pytest.mark.parametrize(
    ("transform", "ddl"),
    [
        ("identity", '"c"'),
        ("day", 'day("c")'),
        ("bucket[16]", 'bucket(16, "c")'),
        ("truncate[4]", 'truncate(4, "c")'),
    ],
)
def test_partition_term_renders_partitioned_by_item(transform: str, ddl: str) -> None:
    assert PartitionTerm.parse(transform, "c").ddl() == ddl


@pytest.mark.parametrize("bad", ["bucket", "day[3]", "zorder", "bucket[x]"])
def test_partition_term_rejects_malformed_transforms(bad: str) -> None:
    with pytest.raises(WriteError):
        PartitionTerm.parse(bad, "c")


@pytest.mark.property
@given(
    kind=st.sampled_from(["identity", "year", "month", "day", "hour", "void"]),
    param_kind=st.sampled_from(["bucket", "truncate"]),
    param=st.integers(min_value=1, max_value=10_000),
    column=st.text(min_size=1, max_size=20),
)
def test_partition_term_parse_is_stable_under_iceberg_spelling(
    kind: str, param_kind: str, param: int, column: str
) -> None:
    """Parsing the Iceberg spelling of a term yields the same term (round trip)."""
    for spelling, expected in (
        (kind, PartitionTerm(kind, column)),
        (f"{param_kind}[{param}]", PartitionTerm(param_kind, column, param)),
    ):
        term = PartitionTerm.parse(spelling, column)
        assert term == expected
        spelled = term.transform if term.param is None else f"{term.transform}[{term.param}]"
        assert PartitionTerm.parse(spelled, column) == term
