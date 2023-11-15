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

    sql_types = [
        "FLOAT",
        "INT",
        "BIGINT",
        "DATE",
        "VARCHAR(MAX)",
        "BIT",
        "VARCHAR(MAX)",
        "INT",
        "BIGINT",
    ]
    pandas_types = [
        "float64",
        "int32",
        "int64",
        "datetime64[ns]",
        "object",
        "bool",
        "category",
        "Int32",
        "Int64",
    ]
    datatype_dict = dict(zip(pandas_types, sql_types))

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
        self._con.close()

        msg = "Table {%s}, successfully imported!" % table_name
        print(f"{msg}")

    def upload_table(self, df: DataFrame, table_name: str, overwrite: bool = True) -> None:
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

        msg = "Table {%s}, successfully uploaded!" % table_name
        print(f"{msg}")

    def _create_table_query(
        self, table_name: str, df: DataFrame, char_length: int, override_length: bool
    ) -> str:
        """Build the query that will be used to create the table"""
        query = "CREATE TABLE " + table_name + "("
        for j, column in enumerate(df.columns):
            matches = re.findall(r"([^']*)", str(df.iloc[:, j].dtype))
            dtype = self.datatype_dict[matches[0]]
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


########################################################################################

if __name__ == "__main__":
    with open("conn.pkl", "rb") as file:
        connection_string = pickle.load(file)

    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    engine = create_engine(connection_url)

    # Create a DataFrame
    data = {"Name": ["John", "Alice", "Bob"], "Age": [25, 30, 35]}
    df = pd.DataFrame(data)
    table_name = "test_table"

    upload_from_df = DataFrameToSQL(connection_string)
    upload_from_df.import_table(df, table_name)

    # Update the table
    data = {"Name": ["Alexis", "Ivan", "Cordero"], "Age": [27, 27, 28]}
    df = pd.DataFrame(data)

    upload_from_df.upload_table(df, table_name)
