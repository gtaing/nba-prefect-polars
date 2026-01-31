import os
from enum import Enum
from pathlib import Path

import duckdb
from loguru import logger


class DBMode(Enum):
    LOCAL = "local"
    PRODUCTION = "production"


class DuckDB:
    """DuckDB connection wrapper supporting local and MotherDuck (production) modes.

    Mode is determined by the DUCKDB_MODE environment variable:
    - "local": Uses a local DuckDB file at target/local.duckdb
    - "production" (default): Uses MotherDuck with motherduck_token env var
    """

    LOCAL_DB_PATH = Path(__file__).parent.parent.parent / "target" / "local.duckdb"

    def __init__(self, database: str = "my_db"):
        self.database = database
        self._mode = self._resolve_mode()
        self._conn = None

    @staticmethod
    def _resolve_mode() -> DBMode:
        mode_str = os.getenv("DUCKDB_MODE", "production").lower()
        if mode_str == "local":
            return DBMode.LOCAL
        return DBMode.PRODUCTION

    @property
    def mode(self) -> DBMode:
        return self._mode

    def _get_motherduck_token(self) -> str:
        """Get MotherDuck token from environment or Prefect Secret block."""
        # First try environment variable (for local dev and Evidence)
        token = os.getenv("MOTHERDUCK_TOKEN")
        if token:
            logger.info("Using MOTHERDUCK_TOKEN from environment variable")
            return token

        # Fall back to Prefect Secret block (for Prefect Cloud execution)
        try:
            from prefect.blocks.system import Secret

            secret_block = Secret.load("motherduck-token")
            token = secret_block.get()
            logger.info("Using MOTHERDUCK_TOKEN from Prefect Secret block")
            return token
        except Exception as e:
            logger.warning(f"Could not load Prefect Secret block: {e}")

        raise ValueError(
            "MOTHERDUCK_TOKEN not found. Set it as an environment variable "
            "or create a Prefect Secret block named 'motherduck-token'"
        )

    @property
    def conn_str(self) -> str:
        if self._mode == DBMode.LOCAL:
            self.LOCAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            return str(self.LOCAL_DB_PATH)

        token = self._get_motherduck_token()
        return f"md:{self.database}?motherduck_token={token}"

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            logger.info(f"Connecting to DuckDB in {self._mode.value} mode")
            self._conn = duckdb.connect(self.conn_str)
            self._configure_s3_access()
        return self._conn

    def _configure_s3_access(self) -> None:
        """Configure S3 access using the standard AWS credential chain.

        Uses DuckDB's load_aws_credentials() which follows the AWS SDK credential
        provider chain: environment variables → ~/.aws/credentials → IAM role.
        """
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")
        self._conn.execute("CALL load_aws_credentials();")
        logger.info("AWS credentials loaded for S3 access")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def create_table_from_file(self, filepath: str, table_name: str) -> None:
        logger.info(f'Creating DuckDB table "{table_name}" from "{filepath}" ({self._mode.value} mode)')

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name}
            AS SELECT * FROM '{filepath}';
        """)

        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f'Table "{table_name}" created with {row_count} rows')

    def table_exists(self, table_name: str) -> bool:
        result = self.conn.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = '{table_name}'
        """).fetchone()
        return result[0] > 0

    def get_table_row_count(self, table_name: str) -> int:
        if not self.table_exists(table_name):
            return 0
        return self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


nba_db = DuckDB()
