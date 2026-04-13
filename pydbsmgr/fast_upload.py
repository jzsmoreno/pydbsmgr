import logging
import os
import warnings
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyodbc
from pandas.core.frame import DataFrame

from pydbsmgr.utils.tools import ColumnsCheck

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFrameToSQL(ColumnsCheck):
    """Optimized class for creating tables from DataFrames and uploading data to SQL databases.

    This class provides efficient methods for importing DataFrames into SQL tables with
    automatic schema inference, connection pooling, and robust error handling.

    Parameters
    ----------
    connection_string : `str`
        Database connection string
    max_pool_size : `int`, `optional`
        Maximum number of connections in the pool. Defaults to `5`.
    """

    def __init__(self, connection_string: str, max_pool_size: int = 5) -> None:
        """Initialize the DataFrameToSQL instance.

        Parameters
        ----------
        connection_string : `str`
            Database connection string
        max_pool_size : `int`, `optional`
            Maximum number of connections in the pool. Defaults to `5`.
        """
        self._connection_string = connection_string
        self._max_pool_size = max_pool_size
        self._connection_pool = []
        self._transaction_batch_size = 1000  # Optimal batch size for transactions

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections with pooling.

        Yields
        ------
        `pyodbc.Connection`
            Database connection object for use within context
        """
        conn = None
        try:
            # Try to get connection from pool
            if self._connection_pool:
                conn = self._connection_pool.pop()
                if conn.closed:
                    conn = pyodbc.connect(self._connection_string, autocommit=False)
            else:
                conn = pyodbc.connect(self._connection_string, autocommit=False)

            yield conn

            # Return connection to pool if not at capacity
            if not conn.closed and len(self._connection_pool) < self._max_pool_size:
                self._connection_pool.append(conn)
            elif conn and not conn.closed:
                conn.close()

        except Exception as e:
            if conn and not conn.closed:
                conn.rollback()
                conn.close()
            logger.error(f"Connection error: {e}")
            raise
        finally:
            # Ensure connection is closed if not returned to pool
            if conn and not conn.closed and conn not in self._connection_pool:
                conn.close()

    def import_table(
        self,
        df: DataFrame,
        table_name: str,
        overwrite: bool = True,
        char_length: int = 512,
        override_length: bool = True,
        batch_size: int = 1000,
        use_transactions: bool = True,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Import a DataFrame into the database as a new table with optimized performance.

        Parameters
        ----------
        df : `DataFrame`
            DataFrame to import
        table_name : `str`
            Name of the target table
        overwrite : `bool`, `optional`
            Whether to overwrite existing table. Defaults to `True`.
        char_length : `int`, `optional`
            Default length for VARCHAR columns. Defaults to `512`.
        override_length : `bool`, `optional`
            Whether to override column lengths. Defaults to `True`.
        batch_size : `int`, `optional`
            Number of rows to insert per batch. Defaults to `1000`.
        use_transactions : `bool`, `optional`
            Whether to use transactions for data integrity. Defaults to `True`.
        verbose : `bool`, `optional`
            Whether to print progress information. Defaults to `False`.

        Returns
        -------
        `Dict[str, Any]`
            Dictionary with operation statistics
        """
        start_time = datetime.now()
        stats = {
            "table_name": table_name,
            "rows_processed": 0,
            "rows_inserted": 0,
            "execution_time": 0,
            "errors": [],
            "warnings": [],
        }

        try:
            df = self._preprocess_dataframe(df)
            stats["rows_processed"] = len(df)

            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Create table with transaction
                try:
                    if use_transactions:
                        conn.autocommit = False

                    # Check if table exists and handle overwrite
                    if self._table_exists(cursor, table_name):
                        if overwrite:
                            if verbose:
                                logger.info(f"Dropping existing table {table_name}")
                            cursor.execute(f"DROP TABLE {self._sanitize_identifier(table_name)}")
                        else:
                            raise ValueError(
                                f"Table {table_name} already exists and overwrite=False"
                            )

                    # Create table
                    create_query = self._create_table_query(
                        table_name, df, char_length, override_length
                    )
                    if verbose:
                        logger.info(f"Creating table {table_name}")
                    cursor.execute(create_query)

                    if use_transactions:
                        conn.commit()

                except Exception as e:
                    if use_transactions:
                        conn.rollback()
                    stats["errors"].append(str(e))
                    raise

                # Insert data in batches
                if len(df) > 0:
                    insert_stats = self._batch_insert_data(
                        conn, cursor, table_name, df, batch_size, use_transactions, verbose
                    )
                    stats.update(insert_stats)

                if verbose:
                    execution_time = (datetime.now() - start_time).total_seconds()
                    logger.info(
                        f"Table {table_name} successfully imported in {execution_time:.2f} seconds"
                    )

        except Exception as e:
            logger.error(f"Failed to import table {table_name}: {e}")
            stats["errors"].append(str(e))
            raise
        finally:
            stats["execution_time"] = (datetime.now() - start_time).total_seconds()

        return stats

    def upload_table(
        self,
        df: DataFrame,
        table_name: str,
        batch_size: int = 1000,
        use_transactions: bool = True,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Update data in an existing table from a DataFrame with optimized performance.

        Parameters
        ----------
        df : `DataFrame`
            DataFrame to upload
        table_name : `str`
            Name of the target table
        batch_size : `int`, `optional`
            Number of rows to insert per batch. Defaults to `1000`.
        use_transactions : `bool`, `optional`
            Whether to use transactions for data integrity. Defaults to `True`.
        verbose : `bool`, `optional`
            Whether to print progress information. Defaults to `False`.

        Returns
        -------
        `Dict[str, Any]`
            Dictionary with operation statistics
        """
        start_time = datetime.now()
        stats = {
            "table_name": table_name,
            "rows_processed": 0,
            "rows_inserted": 0,
            "execution_time": 0,
            "errors": [],
        }

        try:
            df = self._preprocess_dataframe(df)
            stats["rows_processed"] = len(df)

            # Validate table exists
            with self._get_connection() as conn:
                cursor = conn.cursor()
                if not self._table_exists(cursor, table_name):
                    raise ValueError(f"Table {table_name} does not exist")

                if len(df) > 0:
                    insert_stats = self._batch_insert_data(
                        conn, cursor, table_name, df, batch_size, use_transactions, verbose
                    )
                    stats.update(insert_stats)

                if verbose:
                    execution_time = (datetime.now() - start_time).total_seconds()
                    logger.info(
                        f"Data successfully uploaded to table {table_name} in {execution_time:.2f} seconds"
                    )

        except Exception as e:
            logger.error(f"Failed to upload data to table {table_name}: {e}")
            stats["errors"].append(str(e))
            raise
        finally:
            stats["execution_time"] = (datetime.now() - start_time).total_seconds()

        return stats

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        """Clean and preprocess a DataFrame for database upload.

        Parameters
        ----------
        df : `DataFrame`
            Input DataFrame to clean

        Returns
        -------
        `DataFrame`
            Cleaned DataFrame with null values handled
        """
        df_cleaned = df.copy()

        df_cleaned.replace(
            ["", " ", "<NA>", "NULL", "nan"],
            np.nan,
            inplace=True,
        )

        df_cleaned.replace([np.inf, -np.inf], np.nan, inplace=True)

        df_cleaned = df_cleaned.where(pd.notna(df_cleaned), None)

        return df_cleaned

    def _batch_insert_data(
        self,
        conn: pyodbc.Connection,
        cursor: pyodbc.Cursor,
        table_name: str,
        df: DataFrame,
        batch_size: int,
        use_transactions: bool,
        verbose: bool,
    ) -> Dict[str, Any]:
        """Insert DataFrame data in optimized batches.

        Parameters
        ----------
        conn : `pyodbc.Connection`
            Database connection object
        cursor : `pyodbc.Cursor`
            Database cursor for executing queries
        table_name : `str`
            Name of the target table
        df : `DataFrame`
            DataFrame containing data to insert
        batch_size : `int`
            Number of rows to insert per batch
        use_transactions : `bool`
            Whether to use transactions for data integrity
        verbose : `bool`
            Whether to print progress information

        Returns
        -------
        `Dict[str, Any]`
            Dictionary with insertion statistics (rows_inserted, batches_processed)
        """
        stats = {"rows_inserted": 0, "batches_processed": 0}

        try:
            # Prepare insert query
            insert_query = self._insert_table_query(table_name, df)

            # Enable fast execution
            cursor.fast_executemany = True

            # Process in batches
            total_rows = len(df)
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]

                if use_transactions and start_idx % self._transaction_batch_size == 0:
                    conn.autocommit = False

                try:
                    # Prepare batch data
                    batch_data = self._prepare_data_for_insertion(batch_df)

                    # Execute batch insert
                    cursor.executemany(insert_query, batch_data)
                    stats["rows_inserted"] += len(batch_data)
                    stats["batches_processed"] += 1

                    # Commit transaction periodically
                    if (
                        use_transactions
                        and (start_idx + batch_size) % self._transaction_batch_size == 0
                    ):
                        conn.commit()

                    if verbose:
                        progress = (end_idx / total_rows) * 100
                        logger.info(f"Progress: {progress:.1f}% ({end_idx}/{total_rows} rows)")

                except Exception as e:
                    if use_transactions:
                        conn.rollback()
                    logger.error(f"Batch insert failed at row {start_idx}: {e}")
                    raise

            # Final commit
            if use_transactions:
                conn.commit()

        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise

        return stats

    def _table_exists(self, cursor: pyodbc.Cursor, table_name: str) -> bool:
        """Check if a table exists in the database.

        Parameters
        ----------
        cursor : `pyodbc.Cursor`
            Database cursor for executing queries
        table_name : `str`
            Name of the table to check

        Returns
        -------
        `bool`
            True if table exists, False otherwise
        """
        try:
            # Use parameterized query to prevent SQL injection
            query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """
            cursor.execute(query, (table_name,))
            result = cursor.fetchone()
            return bool(result[0])
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False

    def _sanitize_identifier(self, identifier: str) -> str:
        """Sanitize SQL identifiers to prevent injection attacks.

        Parameters
        ----------
        identifier : `str`
            SQL identifier (table name, column name) to sanitize

        Returns
        -------
        `str`
            Sanitized identifier safe for SQL queries
        """
        # Remove any characters that could be used for SQL injection
        sanitized = identifier.replace("'", "").replace('"', "").replace(";", "").replace("--", "")
        # Ensure it starts with a letter or underscore
        if not sanitized[0].isalpha() and sanitized[0] != "_":
            sanitized = "_" + sanitized
        return sanitized.replace(" ", "")

    def _create_table_query(
        self, table_name: str, df: DataFrame, char_length: int, override_length: bool
    ) -> str:
        """Generate CREATE TABLE query with optimized data type inference.

        Parameters
        ----------
        table_name : `str`
            Name of the table to create
        df : `DataFrame`
            DataFrame containing column definitions
        char_length : `int`
            Default length for VARCHAR columns
        override_length : `bool`
            Whether to override column lengths

        Returns
        -------
        `str`
            CREATE TABLE SQL query string
        """
        columns = []

        for col in df.columns:
            col_name = self._sanitize_identifier(col)
            data_type = self._infer_schema(col, df, char_length, override_length)
            columns.append(f"{col_name} {data_type}")

        return f"CREATE TABLE {self._sanitize_identifier(table_name)} ({', '.join(columns)})"

    def _insert_table_query(self, table_name: str, df: DataFrame) -> str:
        """Generate INSERT INTO query with proper parameterization.

        Parameters
        ----------
        table_name : `str`
            Name of the target table
        df : `DataFrame`
            DataFrame containing column definitions

        Returns
        -------
        `str`
            INSERT INTO SQL query string with parameterized values
        """
        columns = [self._sanitize_identifier(col) for col in df.columns]
        placeholders = ", ".join(["?" for _ in columns])
        return f"INSERT INTO {self._sanitize_identifier(table_name)} ({', '.join(columns)}) VALUES ({placeholders})"

    def _infer_schema(
        self, column: str, df: DataFrame, char_length: int, override_length: bool
    ) -> str:
        """Enhanced data type inference with better SQL type mapping.

        Parameters
        ----------
        column : `str`
            Name of the column to infer schema for
        df : `DataFrame`
            DataFrame containing the column data
        char_length : `int`
            Default length for VARCHAR columns
        override_length : `bool`
            Whether to override column lengths

        Returns
        -------
        `str`
            SQL data type string (e.g., VARCHAR, INT, FLOAT, DATETIME2)
        """
        dtype = str(df[column].dtype).lower()

        try:
            # Handle null columns
            if df[column].isnull().all():
                return f"VARCHAR({char_length})"

            # Remove null values for type analysis
            non_null_values = df[column].dropna()
            if len(non_null_values) == 0:
                return f"VARCHAR({char_length})"

            if "float" in dtype:
                # Check if all values are integers
                if non_null_values.apply(lambda x: float(x).is_integer()).all():
                    return "BIGINT"
                return "FLOAT"
            elif "int" in dtype:
                if (
                    "64" in dtype
                    or non_null_values.max() > 2147483647
                    or non_null_values.min() < -2147483648
                ):
                    return "BIGINT"
                return "INT"
            elif "datetime" in dtype:
                return "DATETIME2"
            elif "object" in dtype or "category" in dtype:
                # Calculate actual max length
                max_length = non_null_values.astype(str).str.len().max()
                length = (
                    char_length if override_length or max_length == 0 else int(max_length * 1.2)
                )  # Add 20% buffer
                # Cap at reasonable maximum
                length = min(length, 4000)
                return f"VARCHAR({length})"
            elif "bool" in dtype:
                return "BIT"
            else:
                # Default to VARCHAR for unknown types
                return f"VARCHAR({char_length})"

        except Exception as e:
            logger.warning(f"Could not infer schema for column {column}: {e}")
            return f"VARCHAR({char_length})"

    def _prepare_data_for_insertion(self, df: DataFrame) -> List[List[Any]]:
        """Prepare DataFrame data for SQL insertion with enhanced type handling.

        Parameters
        ----------
        df : `DataFrame`
            DataFrame containing data to prepare for insertion

        Returns
        -------
        `List[List[Any]]`
            List of lists with processed values ready for SQL insertion
        """
        prepared_data = []

        for _, row in df.iterrows():
            processed_row = []
            for value in row:
                # Handle NaN and infinite values
                if isinstance(value, float):
                    if np.isnan(value) or np.isinf(value):
                        processed_row.append(None)
                    else:
                        processed_row.append(value)
                # Handle pandas timestamps
                elif isinstance(value, pd.Timestamp):
                    processed_row.append(value.to_pydatetime() if not pd.isna(value) else None)
                # Handle numpy datetime64
                elif isinstance(value, np.datetime64):
                    processed_row.append(
                        pd.Timestamp(value).to_pydatetime() if not pd.isna(value) else None
                    )
                else:
                    processed_row.append(value)

            prepared_data.append(processed_row)

        return prepared_data


class UploadToSQL(DataFrameToSQL):
    """Enhanced class for efficiently importing/updating tables from DataFrames.

    This class extends DataFrameToSQL with chunking, automatic batch size optimization,
    and comprehensive monitoring capabilities.

    Parameters
    ----------
    connection_string : `str`
        Database connection string
    max_pool_size : `int`, `optional`
        Maximum number of connections in the pool. Defaults to `5`.
    """

    def __init__(self, connection_string: str, max_pool_size: int = 5) -> None:
        """Initialize the UploadToSQL instance.

        Parameters
        ----------
        connection_string : `str`
            Database connection string
        max_pool_size : `int`, `optional`
            Maximum number of connections in the pool. Defaults to `5`.
        """
        super().__init__(connection_string, max_pool_size)
        self._verbose = True
        self._auto_batch_size = True

    def execute(
        self,
        df: DataFrame,
        table_name: str,
        chunk_size: Optional[int] = None,
        method: str = "override",
        char_length: int = 512,
        override_length: bool = True,
        use_transactions: bool = True,
        auto_resolve: bool = True,
        frac: float = 0.01,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """Execute the import/update operation with intelligent chunking and optimization.

        Parameters
        ----------
        df : `DataFrame`
            DataFrame to process
        table_name : `str`
            Name of the target table
        chunk_size : `int`, `optional`
            Number of chunks to split DataFrame into (auto-calculated if None)
        method : `str`, `optional`
            Operation method ("override" or "append"). Defaults to "override".
        char_length : `int`, `optional`
            Default length for VARCHAR columns. Defaults to 512.
        override_length : `bool`, `optional`
            Whether to override column lengths. Defaults to True.
        use_transactions : `bool`, `optional`
            Whether to use transactions. Defaults to True.
        auto_resolve : `bool`, `optional`
            Whether to auto-resolve large DataFrames. Defaults to True.
        frac : `float`, `optional`
            Fraction for auto-resolution. Defaults to 0.01.
        verbose : `bool`, `optional`
            Whether to print progress information. Defaults to False.

        Returns
        -------
        `Dict[str, Any]`
            Dictionary with comprehensive operation statistics
        """
        start_time = datetime.now()
        stats = {
            "operation": method,
            "table_name": table_name,
            "total_rows": len(df),
            "chunks_processed": 0,
            "execution_time": 0,
            "errors": [],
            "warnings": [],
            "chunk_stats": [],
        }

        try:
            # Validate inputs
            if df.empty:
                raise ValueError("DataFrame is empty")

            if method not in ["override", "append"]:
                raise ValueError('Invalid method. Choose from ["override", "append"]')

            # Auto-calculate optimal chunk size
            if chunk_size is None:
                chunk_size = self._calculate_optimal_chunk_size(len(df))

            if chunk_size <= 0:
                raise ValueError("chunk_size must be positive")

            # Determine chunking strategy
            if auto_resolve and len(df) >= 500000:  # 0.5M rows
                n = max(int(len(df) * frac), 1000)  # Minimum 1000 rows per chunk
                df_chunks = [df[i : i + n] for i in range(0, len(df), n)]
                if verbose:
                    logger.info(f"Auto-resolved to {len(df_chunks)} chunks of ~{n} rows each")
            else:
                df_chunks = np.array_split(df, chunk_size)

            stats["chunks_processed"] = len(df_chunks)

            # Execute based on method
            if method == "override":
                stats.update(
                    self._execute_override(
                        df_chunks,
                        table_name,
                        char_length,
                        override_length,
                        use_transactions,
                        verbose,
                    )
                )
            elif method == "append":
                stats.update(self._execute_append(df_chunks, table_name, use_transactions, verbose))

            if verbose:
                execution_time = (datetime.now() - start_time).total_seconds()
                logger.info(f"Operation completed in {execution_time:.2f} seconds")

        except Exception as e:
            logger.error(f"Operation failed: {e}")
            stats["errors"].append(str(e))
            raise
        finally:
            stats["execution_time"] = (datetime.now() - start_time).total_seconds()

        return stats

    def _calculate_optimal_chunk_size(self, total_rows: int) -> int:
        """Calculate optimal chunk size based on DataFrame size and system resources.

        Parameters
        ----------
        total_rows : `int`
            Total number of rows in the DataFrame

        Returns
        -------
        `int`
            Optimal number of chunks for processing
        """
        # Base chunk sizes for different data volumes
        if total_rows < 10000:
            return 1  # Single chunk for small datasets
        elif total_rows < 100000:
            return 4  # 4 chunks for medium datasets
        elif total_rows < 1000000:
            return 10  # 10 chunks for large datasets
        else:
            return 20  # 20 chunks for very large datasets

    def _execute_override(
        self,
        df_chunks: List[DataFrame],
        table_name: str,
        char_length: int,
        override_length: bool,
        use_transactions: bool,
        verbose: bool,
    ) -> Dict[str, Any]:
        """Execute override method with first chunk creating the table.

        Parameters
        ----------
        df_chunks : `List[DataFrame]`
            List of DataFrame chunks to process
        table_name : `str`
            Name of the target table
        char_length : `int`
            Default length for VARCHAR columns
        override_length : `bool`
            Whether to override column lengths
        use_transactions : `bool`
            Whether to use transactions for data integrity
        verbose : `bool`
            Whether to print progress information

        Returns
        -------
        `Dict[str, Any]`
            Dictionary with chunk statistics and operation results
        """
        stats = {"chunk_stats": []}

        # Check if table exists and drop if necessary
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if self._table_exists(cursor, table_name):
                if verbose:
                    logger.info(f"Table {table_name} exists, dropping...")
                cursor.execute(f"DROP TABLE {self._sanitize_identifier(table_name)}")
                conn.commit()

        # Process chunks
        for i, chunk in enumerate(df_chunks):
            chunk_stats = {"chunk_index": i, "rows": len(chunk)}

            try:
                if i == 0:
                    # First chunk creates the table
                    result = self.import_table(
                        df=chunk,
                        table_name=table_name,
                        overwrite=True,
                        char_length=char_length,
                        override_length=override_length,
                        batch_size=1000,
                        use_transactions=use_transactions,
                        verbose=verbose and len(df_chunks) == 1,
                    )
                else:
                    # Subsequent chunks append to existing table
                    result = self.upload_table(
                        df=chunk,
                        table_name=table_name,
                        batch_size=1000,
                        use_transactions=use_transactions,
                        verbose=False,
                    )

                chunk_stats.update(result)
                stats["chunk_stats"].append(chunk_stats)

                if verbose and len(df_chunks) > 1:
                    progress = ((i + 1) / len(df_chunks)) * 100
                    logger.info(f"Progress: {progress:.1f}% ({i + 1}/{len(df_chunks)} chunks)")

            except Exception as e:
                chunk_stats["error"] = str(e)
                stats["chunk_stats"].append(chunk_stats)
                logger.error(f"Chunk {i} failed: {e}")
                raise

        return stats

    def _execute_append(
        self, df_chunks: List[DataFrame], table_name: str, use_transactions: bool, verbose: bool
    ) -> Dict[str, Any]:
        """Execute append method for existing table.

        Parameters
        ----------
        df_chunks : `List[DataFrame]`
            List of DataFrame chunks to process
        table_name : `str`
            Name of the target table
        use_transactions : `bool`
            Whether to use transactions for data integrity
        verbose : `bool`
            Whether to print progress information

        Returns
        -------
        `Dict[str, Any]`
            Dictionary with chunk statistics and operation results
        """
        stats = {"chunk_stats": []}

        # Validate table exists
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if not self._table_exists(cursor, table_name):
                raise ValueError(f"Table {table_name} does not exist for append operation")

        # Process chunks
        for i, chunk in enumerate(df_chunks):
            chunk_stats = {"chunk_index": i, "rows": len(chunk)}

            try:
                result = self.upload_table(
                    df=chunk,
                    table_name=table_name,
                    batch_size=1000,
                    use_transactions=use_transactions,
                    verbose=False,
                )

                chunk_stats.update(result)
                stats["chunk_stats"].append(chunk_stats)

                if verbose and len(df_chunks) > 1:
                    progress = ((i + 1) / len(df_chunks)) * 100
                    logger.info(f"Progress: {progress:.1f}% ({i + 1}/{len(df_chunks)} chunks)")

            except Exception as e:
                chunk_stats["error"] = str(e)
                stats["chunk_stats"].append(chunk_stats)
                logger.error(f"Chunk {i} failed: {e}")
                raise

        return stats

    @property
    def verbose(self) -> bool:
        return self._verbose

    @verbose.setter
    def verbose(self, value: bool):
        self._verbose = value


########################################################################################

if __name__ == "__main__":
    # Example usage with connection string from environment
    connection_string = os.getenv("conn_string")

    # Fallback connection string for testing
    if connection_string is None:
        connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=localhost,1433;"
            "Database=master;"
            "UID=sa;"
            "PWD=vSk60DcYRU;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )

    # Create test DataFrame
    data = {"Name": ["John", "Alice", "Bob"], "Age": [25, 30, 35]}
    df = pd.DataFrame(data)
    table_name = "test_table"

    try:
        # Initialize uploader
        upload_from_df = UploadToSQL(connection_string)

        # Override example
        logger.info("Executing override operation...")
        result = upload_from_df.execute(
            df=df, table_name=table_name, method="override", verbose=True
        )
        logger.info(f"Override result: {result}")

        # Append example
        logger.info("Executing append operation...")
        data = {"Name": ["Alexis", "Ivan", "Cordero"], "Age": [27, 27, 28]}
        df = pd.DataFrame(data)

        result = upload_from_df.execute(df=df, table_name=table_name, method="append", verbose=True)
        logger.info(f"Append result: {result}")

    except Exception as e:
        logger.error(f"Example execution failed: {e}")
