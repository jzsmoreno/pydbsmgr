import os

import numpy as np
import pandas as pd
import pyodbc
from pandas.core.frame import DataFrame

from pydbsmgr.utils.tools import ColumnsCheck


class DataFrameToSQL(ColumnsCheck):
    """Allows creation of a table from a DataFrame and uploading data to the database"""

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        self._con = pyodbc.connect(self._connection_string, autocommit=True)
        self._cur = self._con.cursor()

    def import_table(
        self,
        df: DataFrame,
        table_name: str,
        overwrite: bool = True,
        char_length: int = 512,
        override_length: bool = True,
        close_connection: bool = True,
        verbose: bool = False,
    ) -> None:
        """Imports a DataFrame into the database as a new table"""

        df = self._preprocess_dataframe(df)

        if self._con.closed:
            self._reconnect()

        try:
            query = self._create_table_query(table_name, df, char_length, override_length)
            self._cur.execute(query)
        except pyodbc.Error as e:
            if overwrite:
                self._drop_and_recreate_table(table_name, query)
            else:
                print(f"UserWarning: Could not create table {table_name}. Error: {e}")

        query = self._insert_table_query(table_name, df)
        self._cur.fast_executemany = True
        self._cur.executemany(query, self._prepare_data_for_insertion(df))

        if close_connection:
            self._con.close()

        if verbose:
            print(f"Table {table_name} successfully imported!")

    def upload_table(
        self, df: DataFrame, table_name: str, close_connection: bool = True, verbose: bool = False
    ) -> None:
        """Updates data in an existing table from a DataFrame"""

        df = self._preprocess_dataframe(df)

        if self._con.closed:
            self._reconnect()

        try:
            query = self._insert_table_query(table_name, df)
            self._cur.fast_executemany = True
            self._cur.executemany(query, self._prepare_data_for_insertion(df))
        except pyodbc.Error as e:
            print(f"UserWarning: Could not upload data to table {table_name}. Error: {e}")

        if close_connection:
            self._con.close()

        if verbose:
            print(f"Data successfully uploaded to table {table_name}!")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        super().__init__(df)
        return self.get_frame().replace([" ", "<NA>", np.datetime64("NaT")], None)

    def _reconnect(self):
        self._con = pyodbc.connect(self._connection_string, autocommit=True)
        self._cur = self._con.cursor()

    def _drop_and_recreate_table(self, table_name: str, query: str) -> None:
        try:
            self._cur.execute(f"DROP TABLE {table_name}")
            self._cur.execute(query)
        except pyodbc.Error as e:
            print(f"UserWarning: Could not recreate table {table_name}. Error: {e}")

    def _create_table_query(
        self, table_name: str, df: DataFrame, char_length: int, override_length: bool
    ) -> str:
        columns = ", ".join(
            f"{col} {self._infer_schema(col, df, char_length, override_length)}"
            for col in df.columns
        )
        return f"CREATE TABLE {table_name} ({columns})"

    def _insert_table_query(self, table_name: str, df: DataFrame) -> str:
        columns = ", ".join(df.columns)
        placeholders = ", ".join("?" * len(df.columns))
        return f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    def _infer_schema(
        self, column: str, df: DataFrame, char_length: int, override_length: bool
    ) -> str:
        dtype = str(df[column].dtype).lower()
        if "float" in dtype:
            return "FLOAT"
        elif "int" in dtype:
            return "BIGINT" if "64" in dtype else "INT"
        elif "datetime" in dtype:
            return "DATE"
        elif "object" in dtype or "category" in dtype:
            max_length = df[column].astype(str).str.len().max()
            length = char_length if override_length or max_length == 0 else max_length
            return f"VARCHAR({length})"
        elif "bool" in dtype:
            return "BIT"
        raise ValueError(f"Data type of column {column} could not be inferred: {dtype}")

    def _prepare_data_for_insertion(self, df: DataFrame) -> list:
        return [
            [None if (isinstance(value, float) and np.isnan(value)) else value for value in row]
            for row in df.values.tolist()
        ]


class UploadToSQL(DataFrameToSQL):
    """Efficiently imports/updates a table from a `DataFrame` using the `DataFrameToSQL` class."""

    def __init__(self, connection_string: str) -> None:
        """Establishes the connection to the database."""
        super().__init__(connection_string)
        self._verbose = True

    def execute(
        self,
        df: DataFrame,
        table_name: str,
        chunk_size: int,
        method: str = "override",  # or append
        char_length: int = 512,
        override_length: bool = True,
        close_connection: bool = True,
        auto_resolve: bool = True,
        frac: float = 0.01,
        verbose: bool = False,
    ) -> None:
        """Executes the import/update operation based on the specified method."""
        if len(df) <= chunk_size:
            raise ValueError(
                "'chunk_size' cannot be greater than or equal to the length of the 'DataFrame'. Change the 'chunk_size'."
            )

        # Get chunks of DataFrame
        if auto_resolve and len(df) >= 0.5e6:
            n = int(len(df) * frac)
            df_chunks = [df[i : i + n] for i in range(0, len(df), n)]
        else:
            df_chunks = np.array_split(df, chunk_size)

        if method == "override":
            if self._check_table_exists(table_name):
                print("Table exists, executing OVERRIDE...")
                self._drop_table(table_name)
                # Create table with the first chunk
                self.import_table(
                    df=df_chunks[0],
                    table_name=table_name,
                    overwrite=True,
                    char_length=char_length,
                    override_length=override_length,
                    close_connection=False,
                    verbose=verbose,
                )
            else:
                print("Table does not exist, proceeding with CREATE TABLE.")
                # Create table with the first chunk
                self.import_table(
                    df=df_chunks[0],
                    table_name=table_name,
                    overwrite=True,
                    char_length=char_length,
                    override_length=override_length,
                    close_connection=False,
                    verbose=verbose,
                )

            # Insert the rest of the chunks
            for i in range(1, len(df_chunks)):
                self.upload_table(df_chunks[i], table_name, close_connection=False)

        elif method == "append":
            if self._check_table_exists(table_name):
                for data in df_chunks:
                    self.upload_table(data, table_name, close_connection=False)
            else:
                raise ValueError("Method 'append' requires an existing table.")
        else:
            raise ValueError(
                'Invalid value for argument "method". Choose from ["override", "append"].'
            )

        if close_connection:
            self._con.close()

    def _drop_table(self, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {table_name}"
        if self._con.closed:
            self._reconnect()
        try:
            self._cur.execute(query)
            if self.verbose:
                print(f"Table '{table_name}' dropped successfully.")
        except Exception as e:
            print(f"Failed to drop table '{table_name}'. Error message:\n{str(e)}")

    def _check_table_exists(self, table_name: str) -> bool:
        if self._con.closed:
            self._reconnect()
        query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{table_name}'"
        try:
            self._cur.execute(query)
            result = self._cur.fetchone()
            self._con.close()
            return bool(result[0])
        except Exception as e:
            print("Error checking if table exists.")
            raise ValueError(f"Query Error: {str(e)}")

    def _execute_query(self, query: str):
        try:
            self._cur.execute(query)
            results = {"columns": [desc[0] for desc in self._cur.description]}
            results["data"] = self._cur.fetchall()
            results["count"] = len(results["data"])
            return results
        except Exception as e:
            print("Error executing SQL Query")
            raise ValueError(f"Query Error: {str(e)}")

    @property
    def verbose(self) -> bool:
        return self._verbose

    @verbose.setter
    def verbose(self, value: bool):
        self._verbose = value


########################################################################################

if __name__ == "__main__":
    connection_string = os.getenv("conn_string")
    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;"
        "Database=master;"
        "UID=sa;"
        "PWD=vSk60DcYRU;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    # Create a DataFrame
    data = {"Name": ["John", "Alice", "Bob"], "Age": [25, 30, 35]}
    df = pd.DataFrame(data)
    table_name = "test_table"

    if connection_string is None:
        raise ValueError("Connection string not found.")

    upload_from_df = UploadToSQL(connection_string)
    upload_from_df.execute(
        df=df,
        table_name=table_name,
        chunk_size=2,
        method="override",
    )

    # Update the table
    data = {"Name": ["Alexis", "Ivan", "Cordero"], "Age": [27, 27, 28]}
    df = pd.DataFrame(data)

    upload_from_df.execute(
        df=df,
        table_name=table_name,
        chunk_size=2,
        method="append",
    )
