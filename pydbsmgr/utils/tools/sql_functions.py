import os

import pandas as pd
import pyodbc

from pydbsmgr.fast_upload import DataFrame, DataFrameToSQL


class FileToSQL(DataFrameToSQL):
    """Allows you to create a table from a file or dataframe"""

    def __init__(self, connection_string: str) -> None:
        """Set the connection with the database"""
        self._connection_string = connection_string
        self._con = pyodbc.connect(self._connection_string, autocommit=True)
        self._cur = self._con.cursor()
        super().__init__(connection_string)

    def insert_data(
        self,
        df: DataFrame | str,
        table_name: str,
        overwrite: bool = True,
        char_length: int = 512,
        override_length: bool = True,
    ) -> None:
        """Insert data into SQL Server.

        Parameters:
        ----------
            df (`Dataframe` or `str`): The pandas dataframe that will be inserted into sql server
            table_name (`str`): Name of the table in which the data is being inserted
            overwrite (`bool`): If `True` it will delete and recreate the table before inserting new data
            if `False` it will append the new data onto the end of the existing table
            char_length (`int`): Length of varchar fields for text columns
            override_length (`bool`): Override length of varchar fields for text columns.

        Returns:
        ----------
            `None`
        """

        self.file_type = None
        if type(df) == str:
            self.path = df
            # read csv file
            if df.find("csv") != -1:
                try:
                    df = pd.read_csv(df)
                except:
                    raise ValueError("Unable to parse CSV")
            # read xlsx file
            else:
                try:
                    df = pd.read_excel(df)
                    self.file_type = "xlsx"
                except:
                    raise ValueError("Unable to parse Excel")
        txt = "{:,}".format(len(df))
        print(f"Will be loaded {txt} rows.")
        if overwrite:
            self._create(df, table_name, overwrite, char_length, override_length)
            self._append_to_table(df.iloc[1:, :], table_name)
        else:
            self._append_to_table(df.iloc[1:, :], table_name)

    def _create(
        self,
        df: DataFrame,
        table_name: str,
        overwrite: bool,
        char_length: str,
        override_length: str,
    ):
        self.import_table((df.iloc[0:1, :]), table_name, overwrite, char_length, override_length)

    def _append_to_table(self, df: DataFrame, db_table_nm: str) -> None:
        # csv_buffer = StringIO()
        # df.to_csv(csv_buffer, index=False)

        # Get the CSV data as a string
        # csv_data = csv_buffer.getvalue()
        # get path
        if self.file_type == "xlsx":
            csv_file_nm = os.path.join(os.getcwd(), "csv_file_nm.csv")
            df.to_csv(csv_file_nm, index=False)
        else:
            csv_file_nm = self.path
        self.query = (
            "BULK INSERT "
            + db_table_nm
            + " FROM '"
            + csv_file_nm
            + "' WITH (FORMAT = 'CSV', FIRSTROW = 2)"
        )
        self._con = pyodbc.connect(self._connection_string, autocommit=True)
        self._cur = self._con.cursor()
        self._cur.execute(self.query)
        self._con.close()
        print("Successfully loaded")

        if self.file_type == "xlsx":
            # Delete csv_file_nm.csv file
            os.remove(csv_file_nm)


########################################################################################
