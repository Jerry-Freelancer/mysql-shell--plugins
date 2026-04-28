"""MySQL Shell plugin: dbsize

Provides instance-, schema-, and table-level size helpers:
  - dbsize.instance()
  - dbsize.schema('schema_name')
  - dbsize.table('schema.table')
"""


def _require_session():
    sess = shell.get_session()
    if sess is None:
        raise RuntimeError(
            "No active session. Please connect first, for example: \\connect user@host"
        )
    return sess


def _query(sql, params=None):
    sess = _require_session()
    params = params or []

    if hasattr(sess, "run_sql"):
        return sess.run_sql(sql, params)
    return sess.execute_sql(sql, params)


def _format_size(size_bytes):
    if size_bytes is None:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size_bytes)
    unit_idx = 0

    while size >= 1024 and unit_idx < len(units) - 1:
        size /= 1024
        unit_idx += 1

    return "%.2f %s" % (size, units[unit_idx])


def _split_schema_table(table_name):
    parts = table_name.split(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "Invalid table name. Use format 'schema.table', for example: dbsize.table('sakila.actor')"
        )
    return parts[0], parts[1]


def instance():
    """Return size summary for all non-system schemas and total instance size."""
    res = _query(
        """
        SELECT
            table_schema,
            COALESCE(SUM(data_length + index_length), 0) AS size_bytes
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        GROUP BY table_schema
        ORDER BY size_bytes DESC
        """
    )

    rows = res.fetch_all()
    schemas = []
    total = 0

    for row in rows:
        size_bytes = int(row[1] or 0)
        total += size_bytes
        schemas.append(
            {
                "schema": row[0],
                "size_bytes": size_bytes,
                "size_human": _format_size(size_bytes),
            }
        )

    return {
        "schema_count": len(schemas),
        "total_size_bytes": total,
        "total_size_human": _format_size(total),
        "schemas": schemas,
    }


def schema(schema_name):
    """Return size summary for one schema and its tables."""
    if not schema_name:
        raise ValueError("schema_name is required, for example: dbsize.schema('sakila')")

    schema_total_res = _query(
        """
        SELECT COALESCE(SUM(data_length + index_length), 0)
        FROM information_schema.tables
        WHERE table_schema = ?
        """,
        [schema_name],
    )
    schema_total_row = schema_total_res.fetch_one()
    schema_total = int(schema_total_row[0] or 0)

    table_res = _query(
        """
        SELECT
            table_name,
            COALESCE(data_length + index_length, 0) AS size_bytes
        FROM information_schema.tables
        WHERE table_schema = ?
        ORDER BY size_bytes DESC, table_name ASC
        """,
        [schema_name],
    )

    tables = []
    for row in table_res.fetch_all():
        size_bytes = int(row[1] or 0)
        tables.append(
            {
                "table": row[0],
                "size_bytes": size_bytes,
                "size_human": _format_size(size_bytes),
            }
        )

    return {
        "schema": schema_name,
        "table_count": len(tables),
        "total_size_bytes": schema_total,
        "total_size_human": _format_size(schema_total),
        "tables": tables,
    }


def table(table_name):
    """Return size for one table using format 'schema.table'."""
    schema_name, tbl_name = _split_schema_table(table_name)

    res = _query(
        """
        SELECT
            table_schema,
            table_name,
            COALESCE(data_length + index_length, 0) AS size_bytes,
            COALESCE(data_length, 0) AS data_bytes,
            COALESCE(index_length, 0) AS index_bytes
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema_name, tbl_name],
    )
    row = res.fetch_one()

    if row is None:
        raise ValueError("Table not found: %s" % table_name)

    size_bytes = int(row[2] or 0)
    data_bytes = int(row[3] or 0)
    index_bytes = int(row[4] or 0)

    return {
        "schema": row[0],
        "table": row[1],
        "size_bytes": size_bytes,
        "size_human": _format_size(size_bytes),
        "data_bytes": data_bytes,
        "data_human": _format_size(data_bytes),
        "index_bytes": index_bytes,
        "index_human": _format_size(index_bytes),
    }


# Register plugin object and methods in MySQL Shell.
plugin_obj = shell.create_extension_object()

shell.add_extension_object_member(
    plugin_obj,
    "instance",
    instance,
    {
        "brief": "Show size summary for all schemas in the current MySQL instance.",
        "details": [
            "Aggregates data_length + index_length from information_schema.tables.",
            "System schemas are excluded: information_schema, mysql, performance_schema, sys.",
        ],
    },
)

shell.add_extension_object_member(
    plugin_obj,
    "schema",
    schema,
    {
        "brief": "Show size summary for a schema.",
        "parameters": [{"name": "schema_name", "type": "string", "brief": "Schema name."}],
    },
)

shell.add_extension_object_member(
    plugin_obj,
    "table",
    table,
    {
        "brief": "Show size for one table.",
        "parameters": [
            {
                "name": "table_name",
                "type": "string",
                "brief": "Table name in the form schema.table.",
            }
        ],
    },
)

shell.register_global(
    "dbsize",
    plugin_obj,
    {
        "brief": "Database size helper plugin.",
        "details": [
            "Use dbsize.instance(), dbsize.schema('schema_name'), and dbsize.table('schema.table')."
        ],
    },
)
