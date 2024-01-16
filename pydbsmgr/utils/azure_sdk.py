"""Define azure storage utilities"""
import gzip
import os
import re
from io import BytesIO, StringIO
from typing import List, Tuple

import pandas as pd
import pyarrow.parquet as pq
from azure.storage.blob import BlobPrefix, BlobServiceClient
from dotenv import load_dotenv
from pandas import ExcelFile, read_csv, read_excel, read_parquet, read_table
from pandas.core.frame import DataFrame

from pydbsmgr.utils.tools import ControllerFeatures


def get_connection_string() -> str:
    """Get connection string. Load env variables from .env"""
    load_dotenv()
    return os.getenv("CONNECTION_STRING")


class StorageController(ControllerFeatures):
    """Retrive blobs from a container/directory"""

    def __init__(self, connection_string: str, container_name: str):
        """Create blob storage client and container client"""
        self.__connection_string = connection_string
        self.container_name = container_name

        self._blob_service_client = BlobServiceClient.from_connection_string(
            self.__connection_string
        )
        self._container_client = self._blob_service_client.get_container_client(self.container_name)
        super().__init__(self._container_client)

    def get_BlobList(self, directory_name: str) -> List[str]:
        _BlobPrefix = self._get_BlobPrefix(directory_name)
        _BlobList = []
        for _blob in _BlobPrefix:
            _BlobList.append(_blob["name"])
        _BlobList.sort()
        return _BlobList

    def get_parquet(
        self, directory_name: str, regex: str, manual_mode: bool = False, engine: str = "pyarrow"
    ) -> Tuple[List[DataFrame], List[str]]:
        """Perform reading of `.parquet` and `.parquet.gzip` files in container-directory"""
        dataframes = list()
        dataframe_names = list()
        if manual_mode:
            file_list = self.file_list
        else:
            file_list = self._container_client.walk_blobs(directory_name + "/", delimiter="/")

        for file in file_list:
            if not re.search(regex, file["name"], re.IGNORECASE):
                print(f"Ignoring {file.name}, does not match {regex}")
                continue
            blob_client = self._blob_service_client.get_blob_client(
                container=self.container_name, blob=file["name"]
            )
            blob_data = blob_client.download_blob().readall()
            print("File name : ", file["name"].split("/")[-1])

            if file["name"].endswith(".parquet"):
                df_name = str(file["name"]).replace(".parquet", "").split("/")[-1]
                dataframe_names.append(df_name)
                bytes_io = BytesIO(blob_data)
                parquet_file = pq.ParquetFile(bytes_io)
                df = parquet_file.read().to_pandas()
                dataframes.append(df)
            elif file["name"].endswith(".gzip"):
                gzip_file = BytesIO(blob_data)
                df_name = str(file["name"]).replace(".parquet.gzip", ".gzip").split("/")[-1]
                dataframe_names.append(df_name)
                parquet_file = pq.ParquetFile(gzip_file)
                df = parquet_file.read().to_pandas()
                dataframes.append(df)

        return dataframes, dataframe_names

    def upload_parquet(
        self,
        directory_name: str,
        dfs: List[DataFrame],
        blob_names: List[str],
        format_type: str = "parquet",
        engine: str = "auto",
        compression: bool = True,
        overwrite: bool = True,
    ) -> None:
        """Perform upload of `.parquet` and `.parquet.gzip` files in container-directory"""
        for df, blob_name in zip(dfs, blob_names):
            blob_path_name = directory_name + "/" + blob_name
            if not compression:
                parquet_data = df.to_parquet(index=False, engine=engine)
                self._container_client.upload_blob(
                    name=blob_path_name + "." + format_type, data=parquet_data, overwrite=overwrite
                )
            elif compression:
                parquet_data = df.to_parquet(index=False, engine=engine, compression="gzip")
                self._container_client.upload_blob(
                    name=blob_path_name + "." + format_type + ".gzip",
                    data=parquet_data,
                    overwrite=overwrite,
                )
            else:
                raise ValueError(f"{format_type} not supported")

    def get_excel_csv(
        self, directory_name: str, regex: str, manual_mode: bool = False, encoding: str = "utf-8"
    ) -> Tuple[List[DataFrame], List[str]]:
        """Perform reading of `.xlsx` and `.csv` files in container-directory"""
        dataframes = list()
        dataframe_names = list()
        if manual_mode:
            file_list = self.file_list
        else:
            file_list = self._container_client.walk_blobs(directory_name + "/", delimiter="/")

        for file in file_list:
            if not re.search(regex, file["name"], re.IGNORECASE):
                print(f"Ignoring {file.name}, does not match {regex}")
                continue
            blob_client = self._blob_service_client.get_blob_client(
                container=self.container_name, blob=file["name"]
            )
            blob_data = blob_client.download_blob().readall()
            print("File name : ", file["name"].split("/")[-1])

            blob_data_str = StringIO(str(blob_data, encoding))

            if file["name"].endswith(".csv"):
                df_name = str(file["name"]).replace(".csv", "").split("/")[-1]
                dataframe_names.append(df_name)
                df = read_csv(blob_data_str, index_col=None, low_memory=False)
                dataframes.append(df)
            elif file["name"].endswith(".xlsx"):
                xls_buffer = ExcelFile(blob_data)
                for sheet_name in xls_buffer.sheet_names:
                    df_name = (
                        str(file["name"]).replace(".xlsx", "").split("/")[-1] + "-" + sheet_name
                    )
                    dataframe_names.append(df_name)
                    df = read_excel(xls_buffer, sheet_name=sheet_name, index_col=None)
                    dataframes.append(df)

        return dataframes, dataframe_names

    def upload_excel_csv(
        self,
        directory_name: str,
        dfs: List[DataFrame],
        blob_names: List[str],
        format_type: str = "csv",
        encoding: str = "utf-8",
        overwrite: bool = True,
    ) -> None:
        """Perform upload of `.xlsx` and `.csv` files in container-directory"""
        for df, blob_name in zip(dfs, blob_names):
            blob_path_name = directory_name + "/" + blob_name
            if format_type == "csv":
                csv_data = df.to_csv(index=False, encoding=encoding)
                self._container_client.upload_blob(
                    name=blob_path_name + "." + format_type, data=csv_data, overwrite=overwrite
                )
            elif format_type == "xlsx":
                xlsx_data = BytesIO()
                df.to_excel(xlsx_data, index=False)
                self._container_client.upload_blob(
                    name=blob_path_name + "." + format_type,
                    data=xlsx_data.getvalue(),
                    overwrite=overwrite,
                )
            else:
                raise ValueError(f"{format_type} not supported")

    def show_all_blobs(self) -> None:
        """Show directories from a container"""
        print(f"Container Name: {self.container_name}")
        for blob in self._container_client.list_blobs():
            if len(blob["name"].split("/")) > 1:
                print("\tBlob name : {}".format(blob["name"]))

    def get_all_blob(self, filter_criteria: str = None) -> List[str]:
        """Get all blob names from a container"""
        blob_names = []
        for blob in self._container_client.list_blobs():
            if len(blob["name"].split("/")) > 1:
                blob_names.append(blob["name"])
        if filter_criteria != None:
            blob_names = self._list_filter(blob_names, filter_criteria)
        return blob_names

    def show_blobs(self, directory_name) -> None:
        """Show blobs from a directory"""
        print(f"Container Name: {self.container_name}")
        print(f"\tDirectory Name: {directory_name}")
        file_list = self._container_client.walk_blobs(directory_name + "/", delimiter="/")
        for file in file_list:
            print("\t\tBlob name: {}".format(file["name"].split("/")[1]))

    def _get_BlobPrefix(self, directory_name: str) -> BlobPrefix:
        self.file_list = self._container_client.walk_blobs(directory_name + "/", delimiter="/")
        return self.file_list

    def set_BlobPrefix(self, file_list: list) -> None:
        self.file_list = self._list_to_BlobPrefix(file_list)

    def _list_filter(self, elements: list, character: str) -> List[str]:
        """Function that filters a list from a criteria

        Args:
        ----------
            elements (`list`): list of values to be filtered
            character (`str`): filter criteria

        Returns:
        ----------
            List[`str`]: list of filtered elements
        """
        filter_elements = []
        for element in elements:
            if element.find(character) != -1:
                filter_elements.append(element)
        return filter_elements

    def _print_BlobItem(self) -> None:
        for file in self.file_list:
            print("File name : ", file["name"].split("/")[-1])

    def _print_BlobPrefix(self) -> None:
        for file in self.file_list:
            print("File name : ", file["name"])

    def _list_to_BlobPrefix(self, my_list: list) -> BlobPrefix:
        """Converts a list of iterable directory items into a BlobPrefix class"""
        blob_prefixes = []

        for item in my_list:
            prefix = "/".join(item.split("/")[:-1])
            blob_prefix = BlobPrefix(prefix)
            blob_prefix.name = item
            blob_prefixes.append(blob_prefix)
        return blob_prefixes
