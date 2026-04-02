from pytest_substrate.server import SubstrateServer


def redshift_rows(
    columns: list[str],
    rows: list[list],
    col_type: str = "varchar",
) -> dict:
    """Build a Redshift Data result dict for use with SubstrateServer.seed_result().

    Args:
        columns: Column name strings.
        rows: List of rows; each row is a list of scalar values (str, int,
            float, bool, or None).
        col_type: Default column typeName string applied to all columns
            (e.g. ``"varchar"``, ``"int8"``).

    Returns:
        A dict matching the RedshiftDataResult shape expected by the
        control plane (``{"ColumnMetadata": [...], "Records": [[...]]}``) .
    """
    col_metadata = [
        {
            "name": c,
            "typeName": col_type,
            "nullable": 1,
            "precision": 0,
            "scale": 0,
            "length": 0,
        }
        for c in columns
    ]
    records = []
    for row in rows:
        record = []
        for val in row:
            if val is None:
                record.append({"isNull": True})
            elif isinstance(val, bool):
                record.append({"booleanValue": val})
            elif isinstance(val, int):
                record.append({"longValue": val})
            elif isinstance(val, float):
                record.append({"doubleValue": val})
            else:
                record.append({"stringValue": str(val)})
        records.append(record)
    return {"ColumnMetadata": col_metadata, "Records": records}


__all__ = ["SubstrateServer", "redshift_rows"]
