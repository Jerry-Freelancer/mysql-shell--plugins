"""MySQL Shell plugin: dbsize"""


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


def instance():
    """Query instance size grouped by schema."""
    res = _query(
        """
        SELECT
          table_schema AS `database`,
          SUM(table_rows) AS `row_count`,
          SUM(TRUNCATE(data_length/1024/1024, 2)) AS `data_size_mb`,
          SUM(TRUNCATE(index_length/1024/1024, 2)) AS `index_size_mb`,
          SUM(TRUNCATE(data_free/1024/1024, 2)) AS `fragment_size_mb`
        FROM information_schema.tables
        GROUP BY table_schema
        ORDER BY SUM(data_length) DESC, SUM(index_length) DESC
        """
    )

    rows = []
    for row in res.fetch_all():
        rows.append(
            {
                "database": row[0],
                "row_count": int(row[1] or 0),
                "data_size_mb": float(row[2] or 0),
                "index_size_mb": float(row[3] or 0),
                "fragment_size_mb": float(row[4] or 0),
            }
        )
    return rows


def schema(schema_name):
    """Query one schema size summary."""
    if not schema_name:
        raise ValueError("schema_name is required, for example: dbsize.schema('sakila')")

    res = _query(
        """
        SELECT
          table_schema AS `database`,
          SUM(table_rows) AS `row_count`,
          SUM(TRUNCATE(data_length/1024/1024, 2)) AS `data_size_mb`,
          SUM(TRUNCATE(index_length/1024/1024, 2)) AS `index_size_mb`,
          SUM(TRUNCATE(data_free/1024/1024, 2)) AS `fragment_size_mb`
        FROM information_schema.tables
        WHERE table_schema = ?
        ORDER BY data_length DESC, index_length DESC
        """,
        [schema_name],
    )

    row = res.fetch_one()
    if row is None:
        return {
            "database": schema_name,
            "row_count": 0,
            "data_size_mb": 0.0,
            "index_size_mb": 0.0,
            "fragment_size_mb": 0.0,
        }

    return {
        "database": row[0],
        "row_count": int(row[1] or 0),
        "data_size_mb": float(row[2] or 0),
        "index_size_mb": float(row[3] or 0),
        "fragment_size_mb": float(row[4] or 0),
    }


def table(table_name):
    """Query one table size using format 'schema.table'."""
    parts = table_name.split(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            "Invalid table name. Use format 'schema.table', for example: dbsize.table('sakila.actor')"
        )

    schema_name, tbl_name = parts[0], parts[1]

    res = _query(
        """
        SELECT
          table_schema AS `database`,
          table_name AS `table_name`,
          table_rows AS `row_count`,
          TRUNCATE(data_length/1024/1024, 2) AS `data_size_mb`,
          TRUNCATE(index_length/1024/1024, 2) AS `index_size_mb`,
          TRUNCATE(data_free/1024/1024, 2) AS `fragment_size_mb`
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        ORDER BY data_length DESC, index_length DESC
        """,
        [schema_name, tbl_name],
    )

    row = res.fetch_one()
    if row is None:
        raise ValueError("Table not found: %s" % table_name)

    return {
        "database": row[0],
        "table_name": row[1],
        "row_count": int(row[2] or 0),
        "data_size_mb": float(row[3] or 0),
        "index_size_mb": float(row[4] or 0),
        "fragment_size_mb": float(row[5] or 0),
    }


plugin_obj = shell.create_extension_object()

shell.add_extension_object_member(
    plugin_obj,
    "instance",
    instance,
    {"brief": "Show instance size grouped by schema."},
)

shell.add_extension_object_member(
    plugin_obj,
    "schema",
    schema,
    {
        "brief": "Show one schema size.",
        "parameters": [{"name": "schema_name", "type": "string", "brief": "Schema name."}],
    },
)

shell.add_extension_object_member(
    plugin_obj,
    "table",
    table,
    {
        "brief": "Show one table size.",
        "parameters": [
            {
                "name": "table_name",
                "type": "string",
                "brief": "Table name in the form schema.table.",
            }
        ],
    },
)

shell.register_global("dbsize", plugin_obj, {"brief": "Database size helper plugin."})
