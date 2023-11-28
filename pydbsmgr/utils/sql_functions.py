import os

import pandas as pd
import pyodbc

from pydbsmgr.fast_upload import DataFrame, DataFrameToSQL
from pydbsmgr.utils.azure_sdk import StorageController

from pydbsmgr.utils.tools import generate_secure_password


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
        df: DataFrame,
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
        df_to_load: DataFrame = pd.DataFrame()
        if type(df) == str:
            self.path = df
            # read csv file
            if df.find("csv") != -1:
                try:
                    df_to_load = pd.read_csv(df)
                except:
                    raise ValueError("Unable to parse CSV")
            # read xlsx file
            else:
                try:
                    df_to_load = pd.read_excel(df)
                    self.file_type = "xlsx"
                except:
                    raise ValueError("Unable to parse Excel")
        if type(df) == DataFrame:
            df_to_load = df
        # txt = "{:,}".format(len(df))
        print(f"Will be loaded {len(df_to_load)} rows.")
        if overwrite:
            self._create(
                df_to_load, table_name, overwrite, char_length, override_length
            )
            self._append_to_table(df_to_load.iloc[2:, :], table_name)
        else:
            self._append_to_table(df_to_load.iloc[2:, :], table_name)

    def _create(
        self,
        df: DataFrame,
        table_name: str,
        overwrite: bool,
        char_length: int,
        override_length: bool,
        close_cursor: bool = True,
    ):
        self.import_table(
            (df.iloc[0:1, :]),
            table_name,
            overwrite,
            char_length,
            override_length,
            close_cursor,
        )

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

    def bulk_insert_from_csv(
        self,
        file_path: str,
        db_table_name: str,
        sas_str: str,
        storage_connection_string: str,
        storage_account: str,
        container_name: str,
        credential_name: str = "UploadCSV_Credential",
        data_source_name: str = "UploadCSV_DS",
        char_length: int = 512,
        overwrite: bool = True,
    ) -> bool:
        """Insert data from csv files in Azure Blob Storage into SQL Server with Bulk command

        Parameters:
        ----------
            file_path (`str`): Path to the file in Azure Blob Storage
            db_table_name (`str`): Name of the table in which the data is being inserted
            sas_str (`str`): SAS string to the storage account
            storage_connection_string (`str`): Connection string to the storage account
            storage_account (`str`): Name of the storage account
            container_name (`str`): Name of the container in which the data is being inserted
            credential_name (`str`): Name of the credentials
            data_source_name (`str`): Name of the data source
            char_length (`int`): Length of varchar fields for text columns
            overwrite (`bool`): If `True` it will delete and recreate the table before inserting new data
            if `False` it will append the new data onto the end of the existing table
        Returns:
        ----------
            `bool`: True if the data was inserted successfully
        """
        # Get all the files in the container or file individually
        filter_condition = ""
        if not file_path.endswith("/"):
            filter_condition = file_path.split("/")[-1]

        print("GETTING FILES FROM THE CONTAINER")
        df_list, name_list = self.get_df(
            file_path=file_path,
            storage_connection_string=storage_connection_string,
            container_name=container_name,
            filter_condition=filter_condition,
        )
        print("FINISHED GETTING FILES FROM THE CONTAINER")
        if not df_list:
            print("No files found")
            return False

        df = df_list[0]
        print("=============================================")
        if file_path.endswith(".parquet"):
            # Write the csv files from df_list and name_list
            print("WRITING CSV FILES")
            self.write_csv_from_parquet(
                df_list=df_list,
                name_list=name_list,
                directory="/".join(file_path.split("/")[:-1]),
            )
            print("FINISHED WRITING CSV FILES")

        # Create master key if not created already
        print("CREATING MASTER KEY")
        try:
            sspassword = generate_secure_password()
            self._cur.execute(
                f"CREATE MASTER KEY ENCRYPTION BY PASSWORD = '{sspassword}'"
            )
            print(f"Master key created")
        except pyodbc.ProgrammingError as e:
            print("==============================================")
            warning_type = "UserWarning"
            msg = f"Master key already exists. If you want to create a new one, please drop the existing one first."
            msg += f"Error: {e}"
            print(f"{warning_type}: {msg}")
        print("=============================================")

        # Create credentials query
        print("CREATING CREDENTIALS")
        try:
            credentials_query = f"""CREATE DATABASE SCOPED CREDENTIAL {credential_name}
            WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
            SECRET = '{sas_str}';"""
            self._cur.execute(credentials_query)
            print(f"Credentials with name {credential_name} created")
        except pyodbc.ProgrammingError as e:
            print("==============================================")
            warning_type = "UserWarning"
            msg = f"Credentials already exists. If you want to create a new one, please drop the existing one first."
            msg += f"Error: {e}"
            print(f"{warning_type}: {msg}")
        print("=============================================")

        # Create external data source query
        print("CREATING EXTERNAL DATA SOURCE")
        try:
            external_data_source_query = f"""CREATE EXTERNAL DATA SOURCE {data_source_name}
            WITH (
                TYPE = BLOB_STORAGE,
                LOCATION = 'https://{storage_account}.blob.core.windows.net',
                CREDENTIAL = {credential_name}
            );"""
            self._cur.execute(external_data_source_query)
            print(f"External data source with name {data_source_name} created")
        except pyodbc.ProgrammingError as e:
            print("==============================================")
            warning_type = "UserWarning"
            msg = f"External data source already exists. If you want to create a new one, please drop the existing one first."
            msg += f"Error: {e}"
            print(f"{warning_type}: {msg}")

        print("=============================================")

        # Create bulk insert query
        bulk_insert_query = f"""BULK INSERT {db_table_name}
        FROM '{file_path}'
        WITH (
            DATA_SOURCE = '{data_source_name}',
            FIELDTERMINATOR = ',',
            DATAFILETYPE = 'char',
            ROWTERMINATOR = '\n',
            FIRSTROW = 3
        );"""

        # Create table
        if overwrite:
            print("CREATING TABLE FOR BULK INSERT")
            self._create(
                df=df,
                table_name=db_table_name,
                overwrite=overwrite,
                char_length=char_length,
                override_length=True,
                close_cursor=False,
            )
            print("TABLE CREATED")
            print("=============================================")
        print("INSERTING DATA, PLEASE WAIT")
        self._cur.execute(bulk_insert_query)
        print("SUCCESSFULLY 'BULK INSERTED'")
        print("=============================================")

        # Drop dropable objects
        self.drop_dropables(
            data_source_name, credential_name=credential_name, masterkey=True
        )
        self._con.close()

        print("PROCESS FINISHED")
        return True

    def drop_dropables(
        self, data_source_name: str, credential_name: str, masterkey: bool = False
    ) -> bool:
        """Drop dropable objects

        Parameters:
        ----------
            data_source_name (`str`): Name of the data source
            masterkey (`bool`): If `True` it will drop the master key
        Returns:
        ----------
            `Bool`: True if the data was inserted successfully
        """
        print("DROPPING EXTERNAL DATA SOURCE")
        self._cur.execute(f"DROP EXTERNAL DATA SOURCE {data_source_name}")
        print("DROPPING CREDENTIALS")
        self._cur.execute(f"DROP DATABASE SCOPED CREDENTIAL {credential_name}")
        if masterkey:
            print("DROPPING MASTER KEY")
            try:
                self._cur.execute(f"DROP MASTER KEY")
            except pyodbc.ProgrammingError as e:
                print("==============================================")
                warning_type = "UserWarning"
                msg = f"Master key does not exist."
                msg += f"Error: {e}"
                print(f"{warning_type}: {msg}")
                print("==============================================")

        return True

    def get_df(
        self,
        file_path: str,
        storage_connection_string: str,
        container_name: str,
        filter_condition: str,
    ) -> tuple[list[DataFrame], list[str]]:
        self.storage_controller = StorageController(
            storage_connection_string, container_name
        )
        file_names = self.storage_controller.get_all_blob(filter_criteria=file_path)
        filter_names = file_names
        if filter_condition != "":
            filter_names = self.storage_controller._list_filter(
                file_names, filter_condition
            )

        self.storage_controller.set_BlobPrefix(filter_names)
        if file_path.endswith(".parquet"):
            # Get all the files in the container
            print("GETTING PARQUET FILES FROM THE CONTAINER")
            df_list, name_list = self.storage_controller.get_parquet(
                directory_name="/".join(file_path.split("/")[:-1]),
                regex="\w+.parquet",
                manual_mode=True,
            )
            return df_list, name_list
        elif file_path.endswith(".csv"):
            # Get all the files in the container
            print("GETTING CSV FILES FROM THE CONTAINER")
            df_list, name_list = self.storage_controller.get_excel_csv(
                directory_name="/".join(file_path.split("/")[:-1]),
                regex="\w+.csv",
                manual_mode=True,
            )
            return df_list, name_list
        else:
            print("Not supported yet")

        return [], []

    def write_csv_from_parquet(
        self,
        df_list: list[DataFrame],
        name_list: list[str],
        directory: str = "/",
        write_to_csv: bool = True,
    ) -> None:
        """Write a csv file from parquet files in a container
        Parameters:
        ----------
            connection_string (`str`): Connection string to the storage account
            container_name (`str`): Name of the container in which the data is being inserted
            directory (`str`): Directory in which the parquet files are located
        Returns:
        ----------
            `bool`: True if the file was created successfully
        """
        # Write the csv files
        if write_to_csv:
            print("WRITING CSV FILES")
            self.storage_controller.upload_excel_csv(
                directory_name=directory,
                dfs=df_list,
                blob_names=name_list,
            )

            print("FINISHED WRITING CSV FILES")


########################################################################################
