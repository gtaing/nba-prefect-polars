from prefect import task
from loguru import logger

from config.motherduck import nba_db


@task(log_prints=True)
def export_to_duckdb(filepath: str, table_name: str) -> str:
    """Export a parquet file to DuckDB table.

    Args:
        filepath: Path to the parquet file (local or S3)
        table_name: Name of the table to create in DuckDB

    Returns:
        The table name that was created
    """
    logger.info(f"Exporting {filepath} to DuckDB table {table_name}")
    nba_db.create_table_from_file(filepath, table_name)
    return table_name
