# Security policy

## Reporting a vulnerability

Please do **not** open a public issue for security problems.

Use GitHub's private vulnerability reporting on this repository
("Security" tab → "Report a vulnerability"). You should get an
acknowledgement within a few days.

## Scope

awebo is a library and a container entrypoint. It never stores
credentials: everything is resolved at runtime from environment
variables or the cloud provider's credential chain (see `config.py`).

Things that are in scope:

- Credential leakage through logs, lineage events, or error messages.
- SQL injection through catalog / table / column names passed to DuckDB.
- Unsafe handling of remote job files fetched by `python -m awebo`.

Things that are out of scope:

- Misconfigured IAM / bucket policies in the deployer's own account
  (tundra's defaults are locked down; what you change is on you).
- Vulnerabilities in DuckDB, PyIceberg, or other upstream dependencies.
  Report those upstream; we track them via Dependabot.

## Supported versions

Only the latest release on `main` receives fixes.
