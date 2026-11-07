from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from awebo.exceptions import QueryError
from awebo.io.reader import FileReader


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("foo.parquet", "read_parquet"),
        ("FOO.PARQUET", "read_parquet"),
        ("data.csv", "read_csv"),
        ("data.tsv", "read_csv"),
        ("events.json", "read_json"),
        ("events.ndjson", "read_json"),
        ("s3://bucket/path/file.parquet?versionId=abc", "read_parquet"),
    ],
)
def test_detect_format(path: str, expected: str) -> None:
    assert FileReader.detect_format(path) == expected


def test_detect_format_unknown_extension_raises() -> None:
    with pytest.raises(QueryError, match="autodetect"):
        FileReader.detect_format("file.xyz")


@pytest.mark.property
@given(
    suffix=st.sampled_from([".parquet", ".pq", ".csv", ".tsv", ".json", ".ndjson", ".jsonl"]),
    stem=st.text(
        alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=20
    ),
)
def test_detect_format_invariant_known_suffix_never_raises(suffix: str, stem: str) -> None:
    func = FileReader.detect_format(f"{stem}{suffix}")
    assert func in {"read_parquet", "read_csv", "read_json"}
