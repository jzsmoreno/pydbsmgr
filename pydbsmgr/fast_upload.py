import os
import pickle
import re

import numpy as np
import pandas as pd
import pyodbc
from pandas.core.frame import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from pydbsmgr.utils.tools import ColumnsCheck


class DataFrameToSQL(ColumnsCheck):
    """Allows you to create a table from a dataframe"""

    def __init__(self, connection_string: str) -> None:
        """Set the connection with the database"""
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
        close_cursor: bool = True,
        verbose: bool = False,
    ) -> None:
        """Process for importing the dataframe into the database"""

        """Check if the current connection is active. If it is not, create a new connection"""

        super().__init__(df)
        df = self.get_frame()
        df = df.replace(" ", None)
        df = df.replace("<NA>", None)
        df = df.replace(np.datetime64("NaT"), None)

        if self._con.closed:
            self._con = pyodbc.connect(self._connection_string, autocommit=True)
            self._cur = self._con.cursor()

        try:
            """Create table"""
            self._cur.execute(
                self._create_table_query(table_name, df, char_length, override_length)
            )
        except pyodbc.Error as e:
            if overwrite:
                """If the table exists, it will be deleted and recreated"""
                self._cur.execute("DROP TABLE %s" % (table_name))
                self._cur.execute(
                    self._create_table_query(table_name, df, char_length, override_length)
                )
            else:
                warning_type = "UserWarning"
                msg = "It was not possible to create the table {%s}" % table_name
                msg += "Error: {%s}" % e
                print(f"{warning_type}: {msg}")

        """Insert data"""
        self._cur.fast_executemany = True
        self._cur.executemany(
            self._insert_table_query(table_name, df),
            [
                [None if (isinstance(value, float) and np.isnan(value)) else value for value in row]
                for row in df.values.tolist()
            ],
        )
        if close_cursor:
            self._con.close()

        if verbose:
            msg = "Table {%s}, successfully imported!" % table_name
            print(f"{msg}")

    def upload_table(self, df: DataFrame, table_name: str, verbose: bool = False) -> None:
        """Access to update data from a dataframe to a database"""

        """Check if the current connection is active. If it is not, create a new connection"""

        super().__init__(df)
        df = self.get_frame()
        df = df.replace(" ", None)
        df = df.replace("<NA>", None)
        df = df.replace(np.datetime64("NaT"), None)

        if self._con.closed:
            self._con = pyodbc.connect(self._connection_string, autocommit=True)
            self._cur = self._con.cursor()

        try:
            """Insert data"""
            self._cur.fast_executemany = True
            self._cur.executemany(self._insert_table_query(table_name, df), df.values.tolist())
            self._con.close()
        except pyodbc.Error as e:
            print(e)
            warning_type = "UserWarning"
            msg = "It was not possible to create the table {%s}" % table_name
            msg += "Error: {%s}" % e
            print(f"{warning_type}: {msg}")

        if verbose:
            msg = "Table {%s}, successfully uploaded!" % table_name
            print(f"{msg}")

    def _create_table_query(
        self, table_name: str, df: DataFrame, char_length: int, override_length: bool
    ) -> str:
        """Build the query that will be used to create the table"""
        query = "CREATE TABLE " + table_name + "("
        for j, column in enumerate(df.columns):
            matches = re.findall(r"([^']*)", str(df.iloc[:, j].dtype))
            dtype = self._infer_schema_query(matches[0])
            if dtype == "VARCHAR(MAX)":
                element = max(list(df[column].astype(str)), key=len)
                max_string_length = len(element)
                if max_string_length == 0 or override_length:
                    max_string_length = char_length
                dtype = dtype.replace("MAX", str(max_string_length))
            query += column + " " + dtype + ", "

        query = query[:-2]
        query += ")"
        return query

    def _insert_table_query(self, table_name: str, df: DataFrame) -> str:
        """Build the query to insert all rows found in the dataframe"""
        query = "INSERT INTO %s({0}) values ({1})" % (table_name)
        query = query.format(",".join(df.columns), ",".join("?" * len(df.columns)))
        return query

    def _infer_schema_query(self, datatype: str) -> str:
        """Infer schema from a given datatype string"""
        datatype = datatype.lower()
        if datatype.find("float") != -1:
            return "FLOAT"
        elif datatype.find("int") != -1:
            if datatype.find("64") != -1:
                return "BIGINT"
            else:
                return "INT"
        elif datatype.find("datetime") != -1:
            return "DATE"
        elif datatype.find("object") != -1:
            return "VARCHAR(MAX)"
        elif datatype.find("category") != -1:
            return "VARCHAR(MAX)"
        elif datatype.find("bool") != -1:
            return "BIT"
        else:
            raise ValueError("Data type could not be inferred!")


class UploadToSQL(DataFrameToSQL):
    """It allows you to import/update a table from a `DataFrame` in an efficient way using the `DataFrameToSQL` class."""

    def __init__(self, connection_string: str) -> None:
        """Establish the connection to the database using the `DataFrameToSQL` class."""
        super().__init__(connection_string)

    def execute(
        self,
        df: DataFrame,
        table_name: str,
        chunk_size: int,
        method: str = "override",  # or append
        char_length: int = 512,
        override_length: bool = True,
        close_cursor: bool = True,
        auto_resolve: bool = True,
        frac: float = 0.01,
        verbose: bool = False,
    ) -> None:
        """Checks if the number of chunks corresponds to the `DataFrame`."""
        assert (
            len(df) > chunk_size
        ), "'chunk_size' cannot be greater than the length of the 'DataFrame', change the 'chunk_size'"

        """Get chunks of `DataFrame`"""
        if auto_resolve:
            if len(df) >= 0.5e6:
                n = int((df).shape[0] * frac)
                df_chunks = [(df)[i : i + n] for i in range(0, (df).shape[0], n)]
            else:
                df_chunks = np.array_split(df, chunk_size)
        else:
            df_chunks = np.array_split(df, chunk_size)

        if method == "override":
            if self._check_table_exists(table_name):
                print("Table exists, executing OVERRIDE...")
                self._drop_table(table_name)

                self.import_table(
                    df=df_chunks[0],
                    table_name=table_name,
                    overwrite=True,
                    char_length=char_length,
                    override_length=override_length,
                    close_cursor=close_cursor,
                    verbose=verbose,
                )

            else:
                print("Table does not exist, proceeding with CREATE TABLE.")

                """Inserting the first chunk"""
                self.import_table(
                    df_chunks[0],
                    table_name,
                    True,
                    char_length,
                    override_length,
                    close_cursor,
                    verbose,
                )

            """Inserting the rest of the chunks"""
            for i in range(1, len(df_chunks)):
                self.upload_table(df_chunks[i], table_name, False)
        elif method == "append":
            if self._check_table_exists(table_name):
                for data in df_chunks:
                    self.upload_table(data, table_name, False)
            else:
                raise ValueError("Method 'append' requires an existing table.")
        else:
            raise ValueError(
                'Invalid value for argument "method". Choose from ["override","append"].'
            )

    def _drop_table(self, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {table_name}"
        _con = pyodbc.connect(self._connection_string, autocommit=True)
        cursor = _con.cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            print(f"Failed to drop table '{table_name}'. Error message:\n{str(e)}")
        cursor.close()
        _con.close()

    def _check_table_exists(self, table_name: str) -> bool:
        query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{table_name}'"
        result = self._execute_query(query)
        return bool(result["data"][0][0])

    def _execute_query(self, query: str):
        _con = pyodbc.connect(self._connection_string, autocommit=True)
        cursor = _con.cursor()
        try:
            cursor.execute(query)
            results = {"columns": [desc[0] for desc in cursor.description]}
            results["data"] = cursor.fetchall()
            results["count"] = [len(results["data"])]
        except Exception as e:
            print("Error executing SQL Query")
            raise ValueError(f"Query Error: {str(e)}")
        cursor.close()
        _con.close()
        return results


########################################################################################

if __name__ == "__main__":
    connection_string = os.getenv("conn_string")
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
