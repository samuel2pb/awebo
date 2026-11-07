"""In-process Iceberg REST catalog for hermetic tests.

DuckDB's iceberg extension writes only through an attached REST catalog.
CI has no Docker, so this module serves the subset of the Iceberg REST spec
DuckDB uses (config, namespaces, tables, single- and multi-table commits)
from a background thread, backed by PyIceberg's SQLite ``SqlCatalog`` over a
local-filesystem warehouse.

Tests attach it through the production :class:`RestCatalogBackend`, so the
ATTACH SQL awebo emits is exercised for real, and read writes back through
the same ``SqlCatalog`` (the ``_read_back`` pattern).
"""

from __future__ import annotations

import json
import logging
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, ClassVar, cast
from urllib.parse import unquote, urlsplit

from pyiceberg.catalog.rest import CreateTableRequest, TableResponse
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.table import CommitTableRequest

if TYPE_CHECKING:
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.table import Table

log = logging.getLogger(__name__)

type Json = dict[str, object]

_NS = r"/v1/namespaces/([^/]+)"
_ERRORS: dict[type[Exception], tuple[int, str]] = {
    NoSuchTableError: (404, "NoSuchTableException"),
    NoSuchNamespaceError: (404, "NoSuchNamespaceException"),
    TableAlreadyExistsError: (409, "AlreadyExistsException"),
    NamespaceAlreadyExistsError: (409, "AlreadyExistsException"),
    CommitFailedException: (409, "CommitFailedException"),
}


def _namespace(raw: str) -> tuple[str, ...]:
    return tuple(unquote(raw).split("\x1f"))


def _table_response(metadata_location: str | None, metadata: object) -> bytes:
    response = TableResponse.model_validate(
        {"metadata-location": metadata_location, "metadata": metadata, "config": {}}
    )
    return response.model_dump_json(by_alias=True, exclude_none=True).encode()


class _StagedRef:
    """Stand-in ``Table`` for commits that create a staged table."""

    def __init__(self, identifier: tuple[str, ...]) -> None:
        self._identifier = identifier

    def name(self) -> tuple[str, ...]:
        return self._identifier


class _Handler(BaseHTTPRequestHandler):
    catalog: ClassVar[SqlCatalog]

    def log_message(self, format: str, *args: object) -> None:
        log.debug("rest-catalog %s", format % args)

    # ---------------------------------------------------------------- io
    def _send(self, code: int, body: bytes | Json | None = None) -> None:
        if body is None:
            data = b""
        elif isinstance(body, bytes):
            data = body
        else:
            data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _json(self) -> Json:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        return cast("Json", json.loads(raw))

    @staticmethod
    def _table_body(table: Table) -> bytes:
        return _table_response(table.metadata_location, table.metadata)

    # ----------------------------------------------------------- routing
    def _dispatch(self) -> None:
        path = urlsplit(self.path).path
        try:
            self._route(path)
        except tuple(_ERRORS) as exc:
            code, kind = _ERRORS[type(exc)]
            self._send(code, {"error": {"message": str(exc), "type": kind, "code": code}})
        except Exception as exc:
            log.exception("rest-catalog failure on %s %s", self.command, path)
            self._send(500, {"error": {"message": str(exc), "type": "ServerError", "code": 500}})

    def _route(self, path: str) -> None:  # noqa: PLR0911
        cat = self.catalog
        method = self.command
        if path == "/v1/config":
            return self._send(200, {"defaults": {}, "overrides": {}})
        if path == "/v1/namespaces":
            if method == "GET":
                return self._send(200, {"namespaces": [list(n) for n in cat.list_namespaces()]})
            body = self._json()
            namespace = tuple(cast("list[str]", body["namespace"]))
            props = cast("dict[str, str]", body.get("properties") or {})
            cat.create_namespace(namespace, props)
            return self._send(200, {"namespace": list(namespace), "properties": props})
        if path == "/v1/transactions/commit":
            for change in cast("list[Json]", self._json()["table-changes"]):
                request = CommitTableRequest.model_validate(change)
                ident = request.identifier
                if ident is None:
                    raise CommitFailedException("multi-table commit without identifier")
                self._commit((*ident.namespace.root, ident.name), request)
            return self._send(204)
        if m := re.fullmatch(_NS, path):
            namespace = _namespace(m[1])
            if method == "DELETE":
                cat.drop_namespace(namespace)
                return self._send(204)
            props = cat.load_namespace_properties(namespace)
            if method == "HEAD":
                return self._send(204)
            return self._send(200, {"namespace": list(namespace), "properties": props})
        if m := re.fullmatch(_NS + "/tables", path):
            return self._tables(_namespace(m[1]))
        if m := re.fullmatch(_NS + r"/tables/([^/]+)", path):
            return self._table((*_namespace(m[1]), unquote(m[2])))
        return self._send(404, {"error": {"message": path, "type": "NotFound", "code": 404}})

    def _tables(self, namespace: tuple[str, ...]) -> None:
        cat = self.catalog
        if self.command == "GET":
            ids = [{"namespace": list(i[:-1]), "name": i[-1]} for i in cat.list_tables(namespace)]
            return self._send(200, {"identifiers": ids})
        body = self._json()
        for key in ("location", "partition-spec", "write-order"):
            body.setdefault(key, None)
        request = CreateTableRequest.model_validate(body)
        ident = (*namespace, request.name)
        kwargs: dict[str, object] = {"location": request.location}
        if request.partition_spec is not None:
            kwargs["partition_spec"] = request.partition_spec
        if request.write_order is not None:
            kwargs["sort_order"] = request.write_order
        create = cat._create_staged_table if request.stage_create else cat.create_table
        table = create(
            ident,
            request.table_schema,
            properties=request.properties,
            **kwargs,  # type: ignore[arg-type]
        )
        if request.stage_create:
            return self._send(200, _table_response(None, table.metadata))
        return self._send(200, self._table_body(table))

    def _table(self, ident: tuple[str, ...]) -> None:
        cat = self.catalog
        if self.command == "GET":
            return self._send(200, self._table_body(cat.load_table(ident)))
        if self.command == "HEAD":
            cat.load_table(ident)
            return self._send(204)
        if self.command == "DELETE":
            cat.drop_table(ident)
            return self._send(204)
        response = self._commit(ident, CommitTableRequest.model_validate(self._json()))
        return self._send(200, response)

    def _commit(self, ident: tuple[str, ...], request: CommitTableRequest) -> bytes:
        cat = self.catalog
        try:
            ref: object = cat.load_table(ident)
        except NoSuchTableError:
            ref = _StagedRef(ident)
        response = cat.commit_table(
            cast("Table", ref), tuple(request.requirements), tuple(request.updates)
        )
        return response.model_dump_json(by_alias=True, exclude_none=True).encode()

    do_GET = do_POST = do_HEAD = do_DELETE = _dispatch


class RestCatalogServer:
    """A running REST catalog bound to ``127.0.0.1`` on a free port."""

    def __init__(self, catalog: SqlCatalog) -> None:
        handler = type("_BoundHandler", (_Handler,), {"catalog": catalog})
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        self._thread.start()

    @property
    def uri(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
