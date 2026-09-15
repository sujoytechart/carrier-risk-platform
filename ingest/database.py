"""PostgreSQL schema operations used by the immutable raw loader."""

from __future__ import annotations

import re
from importlib.resources import files

from psycopg import Connection, sql

from ingest.contracts import FeedSchema

DatabaseConnection = Connection[tuple[object, ...]]

_SOURCE_COLUMN = re.compile(r"^[A-Z][A-Z0-9_]*$")
_RAW_TABLES = {
    "crashes": "crash_rows",
    "inspections": "inspection_rows",
}
_RAW_INITIALIZATION_LOCK = "carrier-risk-raw-schema-initialization-v1"


def initialize_raw_storage(
    connection: DatabaseConnection,
    schema: FeedSchema,
) -> None:
    """Create required raw relations under a short initialization-only lock.

    Existing initialized feeds use only catalog reads. First use and new feeds
    serialize their DDL, but release the advisory lock when this short transaction
    commits, before any snapshot rows are copied.
    """
    if _raw_relations_exist(connection, schema):
        _validate_snapshot_registry(connection)
        return

    connection.execute(
        "select pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (_RAW_INITIALIZATION_LOCK,),
    )
    create_raw_schema(connection)
    ensure_feed_table(connection, schema)


def create_raw_schema(connection: DatabaseConnection) -> None:
    """Create batch and quarantine relations and validate registry compatibility."""
    statement = files("ingest").joinpath("sql", "create_raw_schema.sql").read_text()
    connection.execute(statement)
    _validate_snapshot_registry(connection)


def ensure_feed_table(
    connection: DatabaseConnection,
    schema: FeedSchema,
) -> None:
    """Create the feed's text-preserving raw table from its versioned contract."""
    table_name = _table_name(schema.feed_name)
    invalid_columns = [
        name for name in schema.columns if not _SOURCE_COLUMN.fullmatch(name)
    ]
    if invalid_columns:
        raise ValueError(
            f"Feed schema contains unsafe source columns: {', '.join(invalid_columns)}"
        )

    source_columns = sql.SQL(", ").join(
        sql.SQL("{} text").format(sql.Identifier(column.lower()))
        for column in schema.columns
    )
    statement = sql.SQL(
        """
        create table if not exists raw.{} (
            batch_id text not null references raw.snapshot_batches(batch_id),
            source_row_number bigint not null,
            {},
            primary key (batch_id, source_row_number)
        )
        """
    ).format(sql.Identifier(table_name), source_columns)
    connection.execute(statement)


def raw_copy_statement(schema: FeedSchema) -> sql.Composed:
    """Build the COPY statement for one schema-validated feed row."""
    columns = [sql.Identifier("batch_id"), sql.Identifier("source_row_number")]
    columns.extend(sql.Identifier(column.lower()) for column in schema.columns)
    return sql.SQL("copy raw.{} ({}) from stdin").format(
        sql.Identifier(_table_name(schema.feed_name)),
        sql.SQL(", ").join(columns),
    )


def _table_name(feed_name: str) -> str:
    try:
        return _RAW_TABLES[feed_name]
    except KeyError as error:
        raise ValueError(f"Unknown raw feed {feed_name!r}") from error


def _raw_relations_exist(
    connection: DatabaseConnection,
    schema: FeedSchema,
) -> bool:
    row = connection.execute(
        "select to_regclass('raw.snapshot_batches'), to_regclass(%s)",
        (f"raw.{_table_name(schema.feed_name)}",),
    ).fetchone()
    return row is not None and all(relation is not None for relation in row)


def _validate_snapshot_registry(connection: DatabaseConnection) -> None:
    source_url_column = connection.execute(
        """
        select data_type, is_nullable
          from information_schema.columns
         where table_schema = 'raw'
           and table_name = 'snapshot_batches'
           and column_name = 'source_url'
        """
    ).fetchone()
    if source_url_column != ("text", "NO"):
        raise ValueError(
            "Existing raw.snapshot_batches requires an explicit additive migration "
            "for the source_url text not null lineage column"
        )
