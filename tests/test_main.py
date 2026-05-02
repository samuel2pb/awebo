"""Tests for the ``python -m awebo`` entrypoint, focused on dep resolution
(``--with``, ``--with-requirements``, PEP 723) and overlay caching."""

from __future__ import annotations

from typing import TYPE_CHECKING

from awebo import __main__ as entry

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    import pytest


def _write_script(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_pep723_dependencies_parsed(tmp_path: Path) -> None:
    script = _write_script(
        tmp_path / "j.py",
        "# /// script\n"
        '# requires-python = ">=3.12"\n'
        '# dependencies = ["httpx>=0.27", "pydantic"]\n'
        "# ///\n"
        "print('hi')\n",
    )
    assert entry._parse_pep723_dependencies(script) == ["httpx>=0.27", "pydantic"]


def test_pep723_no_block_returns_empty(tmp_path: Path) -> None:
    script = _write_script(tmp_path / "j.py", "print('plain')\n")
    assert entry._parse_pep723_dependencies(script) == []


def test_pep723_skips_non_py(tmp_path: Path) -> None:
    archive = _write_script(tmp_path / "bundle.zip", "# /// script\n# ///\n")
    assert entry._parse_pep723_dependencies(archive) == []


def test_pep723_malformed_toml_swallowed(tmp_path: Path) -> None:
    script = _write_script(
        tmp_path / "j.py",
        "# /// script\n# dependencies = [unterminated\n# ///\n",
    )
    assert entry._parse_pep723_dependencies(script) == []


def test_install_extras_noop_when_empty(tmp_path: Path) -> None:
    assert entry._install_extras([], [], None) is None


def test_install_extras_invokes_uv_pip_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[list[str]] = []

    def fake_check_call(cmd: Iterable[str]) -> int:
        captured.append(list(cmd))
        return 0

    monkeypatch.setenv("AWEBO_DEPS_CACHE", str(tmp_path / "cache"))
    monkeypatch.setattr("awebo.__main__.subprocess.check_call", fake_check_call)

    overlay = entry._install_extras(["httpx>=0.27", "pydantic"], [], None)

    assert overlay is not None
    assert overlay.is_dir()
    assert (overlay / ".awebo-installed").exists()
    assert len(captured) == 1
    cmd = captured[0]
    assert cmd[:3] == ["uv", "pip", "install"]
    assert "--target" in cmd
    assert "httpx>=0.27" in cmd
    assert "pydantic" in cmd


def test_install_extras_caches_by_content_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[None] = []

    def fake_check_call(cmd: Iterable[str]) -> int:
        calls.append(None)
        return 0

    monkeypatch.setenv("AWEBO_DEPS_CACHE", str(tmp_path / "cache"))
    monkeypatch.setattr("awebo.__main__.subprocess.check_call", fake_check_call)

    first = entry._install_extras(["httpx", "pydantic"], [], None)
    second = entry._install_extras(["pydantic", "httpx"], [], None)

    assert first == second
    assert len(calls) == 1, "second call with same deps must hit the sentinel"


def test_install_extras_passes_requirements_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    req = tmp_path / "reqs.txt"
    req.write_text("httpx\npydantic\n", encoding="utf-8")
    captured: list[list[str]] = []

    def fake_check_call(cmd: Iterable[str]) -> int:
        captured.append(list(cmd))
        return 0

    monkeypatch.setenv("AWEBO_DEPS_CACHE", str(tmp_path / "cache"))
    monkeypatch.setattr("awebo.__main__.subprocess.check_call", fake_check_call)

    entry._install_extras([], [req], None)

    assert captured[0].count("-r") == 1
    assert str(req) in captured[0]


def test_main_runs_script_without_extras(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out.txt"
    script = _write_script(
        tmp_path / "j.py",
        f"from pathlib import Path; Path({str(out)!r}).write_text('ok')\n",
    )

    def fail_check_call(cmd: Iterable[str]) -> int:  # pragma: no cover - guard
        raise AssertionError(f"uv must not be invoked when no extras: {list(cmd)}")

    monkeypatch.setattr("awebo.__main__.subprocess.check_call", fail_check_call)

    rc = entry.main([str(script)])
    assert rc == 0
    assert out.read_text() == "ok"


def test_main_merges_pep723_and_with_flags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out = tmp_path / "out.txt"
    script = _write_script(
        tmp_path / "j.py",
        "# /// script\n"
        '# dependencies = ["pep-only"]\n'
        "# ///\n"
        f"from pathlib import Path; Path({str(out)!r}).write_text('done')\n",
    )
    captured: list[list[str]] = []

    def fake_check_call(cmd: Iterable[str]) -> int:
        captured.append(list(cmd))
        return 0

    monkeypatch.setenv("AWEBO_DEPS_CACHE", str(tmp_path / "cache"))
    monkeypatch.setattr("awebo.__main__.subprocess.check_call", fake_check_call)

    rc = entry.main(["--with", "cli-only", str(script)])

    assert rc == 0
    assert out.read_text() == "done"
    assert len(captured) == 1
    cmd = captured[0]
    assert "pep-only" in cmd
    assert "cli-only" in cmd
    pep_idx = cmd.index("pep-only")
    cli_idx = cmd.index("cli-only")
    assert pep_idx < cli_idx, "PEP 723 deps should precede --with deps"
