"""Container/CLI entrypoint: run a user-supplied Python job.

The job target may be:

* a single ``.py`` file (local or fsspec URI),
* a directory containing ``__main__.py`` (local only — fsspec single-shot
  ``open`` cannot stream a directory; package it as a zip first),
* a zipapp-style ``.zip`` archive whose root contains ``__main__.py``
  (local or fsspec URI).

In every case we hand the resolved local path to :func:`runpy.run_path`,
which sets ``sys.path[0]`` to the script's parent dir / the zip root /
the package dir, so sibling and intra-package imports resolve without
extra plumbing. The job runs inside *this* interpreter, so
``import awebo`` finds the installed package with no fork.

Extra runtime deps may be supplied three ways and are merged before the
script runs:

* ``--with PKG`` (repeatable) — uv-style additive package spec.
* ``--with-requirements FILE`` (repeatable) — pip-style requirements
  file (local path or fsspec URI).
* PEP 723 inline metadata — a ``# /// script ... # ///`` block at the
  top of the resolved ``.py`` file declaring ``dependencies = [...]``.

When extras are present we resolve them with ``uv pip install`` into a
content-addressed cache dir (``$AWEBO_DEPS_CACHE`` or ``/tmp/awebo-deps``)
and prepend that dir to ``sys.path`` before invoking the script. Cache
key is ``sha256(sorted(deps) + requirements bodies + python)`` so reruns
of the same job skip resolution entirely.

Usage::

    awebo my_job.py [arg ...]
    awebo --with httpx --with 'pydantic>=2.7' my_job.py [arg ...]
    awebo --with-requirements requirements.txt my_job.py
    awebo s3://bucket/path/job.py [arg ...]
    awebo s3://bucket/path/bundle.zip [arg ...]
    awebo --version

Cloud schemes require the matching fsspec backend installed (s3fs ships
with awebo by default; gcsfs is in the ``runner`` extra).
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from urllib.parse import urlparse

_LOCAL_SCHEMES: frozenset[str] = frozenset({"", "file"})

# PEP 723 inline metadata block: a contiguous run of `# ` lines wrapped
# by `# /// script` / `# ///` markers. Body is TOML once the leading
# `# ` (or `#`) is stripped.
_PEP723_RE: re.Pattern[str] = re.compile(r"(?m)^# /// script\s*$\n((?:#(?:[ \t].*)?\n)+)# ///\s*$")


def _resolve_uri(uri: str, scratch: Path, fallback_name: str = "file") -> Path:
    """Return a local filesystem path to ``uri``.

    Local paths and ``file://`` URIs are returned as-is (they may point
    at a file *or* a directory). Anything else is streamed through
    fsspec into ``scratch`` (single-shot fetch — the URI must point at
    one file blob).
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in _LOCAL_SCHEMES:
        raw = parsed.path if scheme == "file" else uri
        local = Path(raw).expanduser().resolve()
        if not local.exists():
            raise FileNotFoundError(f"target not found: {local}")
        return local

    import fsspec

    name = Path(parsed.path).name or fallback_name
    dest = scratch / name
    with fsspec.open(uri, "rb") as src, dest.open("wb") as out:
        shutil.copyfileobj(src, out)
    return dest


def _parse_pep723_dependencies(script_path: Path) -> list[str]:
    """Return PEP 723 ``dependencies`` declared in ``script_path``.

    Returns an empty list if the file is not a ``.py``, has no inline
    metadata block, or the block lacks a list-of-strings ``dependencies``
    key. Malformed TOML is silently ignored — the user can still pass
    ``--with`` flags explicitly.
    """
    if script_path.is_dir() or script_path.suffix != ".py":
        return []
    try:
        text = script_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    match = _PEP723_RE.search(text)
    if match is None:
        return []
    body_lines: list[str] = []
    for line in match.group(1).splitlines():
        if line.startswith("# "):
            body_lines.append(line[2:])
        elif line.startswith("#"):
            body_lines.append(line[1:])
        else:
            body_lines.append(line)
    try:
        meta = tomllib.loads("\n".join(body_lines))
    except tomllib.TOMLDecodeError:
        return []
    deps = meta.get("dependencies")
    if not isinstance(deps, list):
        return []
    return [str(d) for d in deps]


def _install_extras(
    packages: list[str],
    requirement_files: list[Path],
    python_version: str | None,
) -> Path | None:
    """Install ``packages`` + ``requirement_files`` into a cached overlay
    dir and return its path, or ``None`` when there is nothing to install.

    The cache key is content-addressed so concurrent / repeated runs of
    the same job hit the sentinel and skip resolution. ``uv`` is required
    on ``$PATH`` (baked into the awebo container image).
    """
    if not packages and not requirement_files:
        return None

    digest = hashlib.sha256()
    for pkg in sorted(packages):
        digest.update(pkg.encode())
        digest.update(b"\n")
    for req_file in requirement_files:
        try:
            digest.update(req_file.read_bytes())
        except OSError as err:
            raise RuntimeError(f"unreadable requirements file: {req_file}") from err
        digest.update(b"\n")
    digest.update(f"py={python_version or sys.executable}".encode())
    key = digest.hexdigest()[:16]

    cache_root = Path(os.environ.get("AWEBO_DEPS_CACHE", "/tmp/awebo-deps")).expanduser()
    target = cache_root / key
    sentinel = target / ".awebo-installed"
    if sentinel.exists():
        return target
    target.mkdir(parents=True, exist_ok=True)

    cmd: list[str] = [
        "uv",
        "pip",
        "install",
        "--target",
        str(target),
        "--python",
        python_version or sys.executable,
        *packages,
    ]
    for req_file in requirement_files:
        cmd.extend(["-r", str(req_file)])
    subprocess.check_call(cmd)
    sentinel.touch()
    return target


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="awebo",
        description=(
            "Run a Python script against the awebo runtime. Script may be a "
            "local path or any fsspec URI (s3://, gs://, https://, file://). "
            "Extra deps may be installed at runtime via --with / "
            "--with-requirements or declared inline via PEP 723."
        ),
    )
    parser.add_argument(
        "-V", "--version", action="store_true", help="Print awebo version and exit."
    )
    parser.add_argument(
        "--with",
        dest="extras",
        action="append",
        default=[],
        metavar="PKG",
        help="Extra package to install before running (uv pip install). Repeatable.",
    )
    parser.add_argument(
        "--with-requirements",
        dest="requirements",
        action="append",
        default=[],
        metavar="FILE",
        help="Requirements file to install. Local path or fsspec URI. Repeatable.",
    )
    parser.add_argument(
        "--python",
        dest="python_version",
        default=None,
        metavar="VERSION",
        help="Python interpreter spec passed to uv (default: current interpreter).",
    )
    parser.add_argument("script", nargs="?", help="Path or URI to a .py / .zip / dir.")
    parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the script as sys.argv[1:].",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    _log = logging.getLogger("awebo.runner")

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        from awebo import __version__

        print(f"awebo {__version__}")
        return 0

    if not args.script:
        parser.print_usage(sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="awebo-") as tmp:
        scratch = Path(tmp)
        script_path = _resolve_uri(args.script, scratch, fallback_name="script.py")
        _log.info("target: %s", script_path)
        requirement_files: list[Path] = [
            _resolve_uri(uri, scratch, fallback_name="requirements.txt")
            for uri in args.requirements
        ]
        pep723_deps = _parse_pep723_dependencies(script_path)
        merged_extras = [*pep723_deps, *args.extras]
        overlay = _install_extras(merged_extras, requirement_files, args.python_version)
        if overlay is not None:
            _log.info("installed %d deps to %s", len(merged_extras), overlay)
            sys.path.insert(0, str(overlay))
        sys.argv = [str(script_path), *args.script_args]
        _log.info("running %s", script_path)
        runpy.run_path(str(script_path), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
