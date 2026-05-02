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

Usage::

    awebo my_job.py [arg ...]
    awebo ./jobs/my_pkg [arg ...]            # dir with __main__.py
    awebo ./jobs/bundle.zip [arg ...]
    awebo s3://bucket/path/job.py [arg ...]
    awebo s3://bucket/path/bundle.zip [arg ...]
    awebo gs://bucket/path/job.py [arg ...]
    awebo https://example.com/job.zip [arg ...]
    awebo --version

Cloud schemes require the matching fsspec backend installed (s3fs ships
with awebo by default; gcsfs is in the ``runner`` extra).
"""

from __future__ import annotations

import argparse
import runpy
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

_LOCAL_SCHEMES: frozenset[str] = frozenset({"", "file"})


def _resolve_script(uri: str, scratch: Path) -> Path:
    """Return a local filesystem path to the job target.

    Local paths and ``file://`` URIs are returned as-is — they may point
    at a ``.py`` file, a directory containing ``__main__.py``, or a
    ``.zip`` archive. Anything else is streamed through fsspec into
    ``scratch`` (single-file fetch — the URI must point at one ``.py``
    or ``.zip`` blob).
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in _LOCAL_SCHEMES:
        raw = parsed.path if scheme == "file" else uri
        local = Path(raw).expanduser().resolve()
        if not local.exists():
            raise FileNotFoundError(f"job target not found: {local}")
        return local

    import fsspec

    name = Path(parsed.path).name or "script.py"
    dest = scratch / name
    with fsspec.open(uri, "rb") as src, dest.open("wb") as out:
        shutil.copyfileobj(src, out)
    return dest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="awebo",
        description="Run a Python script that uses awebo. "
        "Script may be a local path or any fsspec URI (s3://, gs://, https://, file://).",
    )
    parser.add_argument(
        "-V", "--version", action="store_true", help="Print awebo version and exit."
    )
    parser.add_argument("script", nargs="?", help="Path or URI to a .py file.")
    parser.add_argument(
        "script_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to the script as sys.argv[1:].",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
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
        script_path = _resolve_script(args.script, Path(tmp))
        sys.argv = [str(script_path), *args.script_args]
        runpy.run_path(str(script_path), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
