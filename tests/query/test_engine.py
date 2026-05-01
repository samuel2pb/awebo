from __future__ import annotations

import pyarrow as pa
import pytest

from awebo.exceptions import QueryError
from awebo.query.engine import QueryEngine
from awebo.query.result import QueryResult


@pytest.fixture
def engine() -> QueryEngine:
    return QueryEngine()


def test_engine_executes_basic_sql(engine: QueryEngine) -> None:
    relation = engine.execute("SELECT 1 AS x, 'a' AS y")
    arrow_tbl = relation.to_arrow_table()
    assert arrow_tbl.column("x").to_pylist() == [1]


def test_query_result_to_arrow_round_trip(engine: QueryEngine) -> None:
    result = QueryResult(engine.execute("SELECT 42 AS answer"))
    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column("answer").to_pylist() == [42]


def test_register_arrow_makes_table_queryable(engine: QueryEngine) -> None:
    table = pa.table({"x": [1, 2, 3]})
    engine.register_arrow("t", table)
    out = engine.execute("SELECT sum(x) AS s FROM t").to_arrow_table()
    assert out.column("s").to_pylist() == [6]


def test_invalid_sql_raises_query_error(engine: QueryEngine) -> None:
    with pytest.raises(QueryError):
        engine.execute("SELECT * FROM does_not_exist")
