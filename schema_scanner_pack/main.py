"""
Schema Scanner Pack

Publishes the schema of every logical object of a source: column list, column
count, per-column type and the drift hashes computed from them.

Everything here comes from ``pack.schema()``, which reads Parquet footers only.
No data page is touched, so the cost is O(number of columns) in memory and
milliseconds in time whatever the dataset size. The previous implementation
built a full ydata-profiling ProfileReport (which needs the whole frame in RAM),
wrote it to HTML, re-read that HTML and re-parsed it with ``pd.read_html`` into
DataFrames that were never used, then re-read the JSON side of the same report
just to recover the column names.
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List

from qalita_core.pack import Pack


def type_name(dtype: Any) -> str:
    """Stable, readable name for a Polars dtype.

    ``base_type()`` drops the parameters of parametrized dtypes, so a column
    stored as ``Datetime(us)`` in one run and ``Datetime(ns)`` in the next does
    not read as a type change: the physical time unit is a storage detail of the
    Parquet writer, not a schema change the user made.
    """
    try:
        return dtype.base_type().__name__
    except AttributeError:
        return str(dtype)


def dataset_labels(pack: Pack, trigger: str = "source") -> Dict[str, str]:
    """Dataset scope name to publish for each logical object.

    A chunked single-object source stays ONE dataset. The previous code labelled
    each parquet part ``<source>_<n>`` and published one dataset per chunk, so a
    table split into three chunks produced three schemas.
    """
    tables = pack.tables(trigger)
    if len(tables) == 1:
        return {tables[0]: pack.source_config.get("name") or tables[0]}
    return {name: name for name in tables}


def md5_hex(text: str) -> str:
    return hashlib.md5(text.encode(), usedforsecurity=False).hexdigest()


def dataset_scope(dataset_name: str) -> Dict[str, Any]:
    return {"perimeter": "dataset", "value": dataset_name}


def column_scope(dataset_name: str, column: str) -> Dict[str, Any]:
    return {
        "perimeter": "column",
        "value": column,
        "parent_scope": {"perimeter": "dataset", "value": dataset_name},
    }


def schema_metrics(
    dataset_name: str, schema: Dict[str, Any], row_count: int
) -> List[Dict[str, Any]]:
    """Every metric this pack emits, derived from a schema mapping alone."""
    columns = list(schema.keys())
    types = {col: type_name(schema[col]) for col in columns}

    metrics: List[Dict[str, Any]] = [
        {
            "key": "column_count",
            "value": len(columns),
            "scope": dataset_scope(dataset_name),
        },
        {
            "key": "row_count",
            "value": row_count,
            "scope": dataset_scope(dataset_name),
        },
        {
            "key": "column_list_hash",
            "value": md5_hex(",".join(sorted(columns))),
            "scope": dataset_scope(dataset_name),
        },
        {
            "key": "column_order_hash",
            "value": md5_hex(",".join(columns)),
            "scope": dataset_scope(dataset_name),
        },
        {
            "key": "column_types_hash",
            "value": md5_hex(
                ",".join(f"{col}:{types[col]}" for col in columns)
            ),
            "scope": dataset_scope(dataset_name),
        },
    ]

    numeric = [c for c in columns if schema[c].is_numeric()]
    text = [
        c
        for c in columns
        if type_name(schema[c]) in ("String", "Categorical", "Enum")
    ]
    temporal = [c for c in columns if schema[c].is_temporal()]
    metrics.extend(
        [
            {
                "key": "types_numeric",
                "value": len(numeric),
                "scope": dataset_scope(dataset_name),
            },
            {
                "key": "types_text",
                "value": len(text),
                "scope": dataset_scope(dataset_name),
            },
            {
                "key": "types_temporal",
                "value": len(temporal),
                "scope": dataset_scope(dataset_name),
            },
        ]
    )

    for col in columns:
        scope = column_scope(dataset_name, col)
        metrics.append(
            {
                "key": "column_type",
                "value": types[col],
                "scope": scope,
            }
        )
        # `type` is what pack_conf.json charts as a badge; it carried no value
        # before because nothing emitted it.
        metrics.append(
            {
                "key": "type",
                "value": types[col],
                "scope": dict(scope),
            }
        )

    return metrics


def schema_entries(
    dataset_name: str, schema: Dict[str, Any]
) -> List[Dict[str, Any]]:
    return [
        {
            "key": "column",
            "value": column,
            "scope": column_scope(dataset_name, column),
        }
        for column in schema
    ]


def main() -> None:
    with Pack() as pack:
        if pack.source_config.get("type") == "database":
            table_or_query = pack.source_config.get("config", {}).get(
                "table_or_query"
            )
            if not table_or_query:
                raise ValueError(
                    "For a 'database' type source, you must specify "
                    "'table_or_query' in the config."
                )
            pack.load_data("source", table_or_query=table_or_query)
        else:
            pack.load_data("source")

        is_database = pack.source_config.get("type") == "database"
        labels = dataset_labels(pack, "source")

        for table, dataset_name in labels.items():
            schema = pack.schema("source", table)
            row_count = pack.get_row_count("source", table)
            print(
                f"Reading schema of {dataset_name} "
                f"({len(schema)} columns, {row_count} rows)"
            )

            pack.schemas.data.extend(schema_entries(dataset_name, schema))
            pack.metrics.data.extend(
                schema_metrics(dataset_name, schema, row_count)
            )

            if is_database:
                pack.schemas.data.append(
                    {
                        "key": "dataset",
                        "value": dataset_name,
                        "scope": {
                            "perimeter": "dataset",
                            "value": dataset_name,
                            "parent_scope": {
                                "perimeter": "database",
                                "value": pack.source_config["name"],
                            },
                        },
                    }
                )
            else:
                pack.schemas.data.append(
                    {
                        "key": "dataset",
                        "value": dataset_name,
                        "scope": dataset_scope(dataset_name),
                    }
                )

        if is_database:
            pack.schemas.data.append(
                {
                    "key": "database",
                    "value": pack.source_config["name"],
                    "scope": {
                        "perimeter": "database",
                        "value": pack.source_config["name"],
                    },
                }
            )

        pack.schemas.save()
        pack.metrics.save()


if __name__ == "__main__":
    main()
