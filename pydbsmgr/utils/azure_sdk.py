import os
import re
from io import BytesIO, StringIO
from typing import List, Tuple

import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pandas import read_csv, read_excel
from pandas.core.frame import DataFrame

from pydbsmgr.utils.tools import ControllerFeatures


def get_connection_string() -> str:
    """Get connection string. Load env variables from `.env`"""
    load_dotenv()
    return os.getenv("CONNECTION_STRING")


class StorageController(ControllerFeatures):
    """Retrieve blobs from a container/directory"""

    def __init__(self, connection_string: str, container_name: str):
        """Create blob storage client and container client"""
        self.__connection_string = connection_string
        self.container_name = container_name

        self._blob_service_client = BlobServiceClient.from_connection_string(
            self.__connection_string
        )
        self._container_client = self._blob_service_client.get_container_client(self.container_name)
        super().__init__(self._container_client)

    def get_blob_list(self, directory_name: str) -> List[str]:
        blob_prefixes = self._get_blob_prefix(directory_name)
        return sorted(blob["name"] for blob in blob_prefixes)

    def _get_blob_prefix(self, directory_name: str):
        return list(
            self._container_client.walk_blobs(name_starts_with=directory_name + "/", delimiter="/")
        )

    def get_parquet(
        self, directory_name: str, regex: str, manual_mode: bool = False
    ) -> Tuple[List[DataFrame], List[str]]:
        """Perform reading of `.parquet` and `.parquet.gzip` files in container-directory"""
        file_list = (
            self.file_list
            if manual_mode
            else self._container_client.walk_blobs(
                name_starts_with=directory_name + "/", delimiter="/"
            )
        )
        return self._read_files(file_list, regex, "parquet")

    def upload_parquet(
        self,
        directory_name: str,
        dfs: List[DataFrame],
        blob_names: List[str],
        format_type: str = "parquet",
        compression: bool = True,
        overwrite: bool = True,
    ) -> None:
        """Perform upload of `.parquet` and `.parquet.gzip` files in container-directory"""
        for df, blob_name in zip(dfs, blob_names):
            blob_path_name = f"{directory_name}/{blob_name}"
            parquet_data = df.to_parquet(
                index=False, engine="pyarrow", compression="gzip" if compression else None
            )
            self._container_client.upload_blob(
                name=(
                    f"{blob_path_name}.{format_type}.gz"
                    if compression
                    else f"{blob_path_name}.{format_type}"
                ),
                data=parquet_data,
                overwrite=overwrite,
            )

    def get_excel_csv(
        self, directory_name: str, regex: str, manual_mode: bool = False
    ) -> Tuple[List[DataFrame], List[str]]:
        """Perform reading of `.xlsx` and `.csv` files in container-directory"""
        file_list = (
            self.file_list
            if manual_mode
            else self._container_client.walk_blobs(
                name_starts_with=directory_name + "/", delimiter="/"
            )
        )
        return self._read_files(file_list, regex, "excel_csv")

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
            blob_path_name = f"{directory_name}/{blob_name}"
            if format_type == "csv":
                csv_data = df.to_csv(index=False, encoding=encoding)
                self._container_client.upload_blob(
                    name=f"{blob_path_name}.csv", data=csv_data, overwrite=overwrite
                )
            elif format_type == "xlsx":
                xlsx_data = BytesIO()
                with xlsx_data:
                    df.to_excel(xlsx_data, index=False)
                    self._container_client.upload_blob(
                        name=f"{blob_path_name}.xlsx",
                        data=xlsx_data.getvalue(),
                        overwrite=overwrite,
                    )
            else:
                raise ValueError(f"Unsupported format: {format_type}")

    def _read_files(self, file_list, regex, file_type):
        """Read files based on the given type and regex filter."""
        dataframes = []
        dataframe_names = []

        for file in file_list:
            if not re.search(regex, file.name, re.IGNORECASE):
                print(f"Ignoring {file.name}, does not match {regex}")
                continue

            blob_data = self._download_blob(file.name)

            if file_type == "parquet":
                df_name = file.name.rsplit(".", 2)[0].rsplit("/", 1)[-1]
                dataframe_names.append(df_name)
                with BytesIO(blob_data) as bytes_io:
                    df = pq.read_table(bytes_io).to_pandas()
                    dataframes.append(df)

            elif file_type == "excel_csv":
                filename, extension = os.path.splitext(file.name.split("/")[-1])
                if extension == ".csv":
                    try:
                        blob_str = blob_data.decode("utf-8")
                    except UnicodeDecodeError:
                        blob_str = blob_data.decode("latin-1")
                    dataframe_names.append(filename)
                    with StringIO(blob_str) as csv_file:
                        df = read_csv(csv_file, index_col=None, low_memory=False)
                        dataframes.append(df)
                elif extension == ".xlsx":
                    with BytesIO(blob_data) as xlsx_buffer:
                        all_sheets = read_excel(xlsx_buffer, sheet_name=None, index_col=None)
                        for sheet_name, df in all_sheets.items():
                            dataframe_names.append(f"{filename}-{sheet_name}")
                            dataframes.append(df.reset_index(drop=True))

        return dataframes, dataframe_names

    def _download_blob(self, blob_name):
        """Download a blob from Azure Storage."""
        blob_client = self._blob_service_client.get_blob_client(
            container=self.container_name, blob=blob_name
        )
        return blob_client.download_blob().readall()

    def show_all_blobs(self) -> None:
        """Show directories from a container"""
        print(f"Container Name: {self.container_name}")
        for blob in self._container_client.list_blobs():
            if len(blob.name.split("/")) > 1:
                print(f"\tBlob name : {blob.name}")

    def get_all_blob(self, filter_criteria: str = None) -> List[str]:
        """Get all blob names from a container"""
        blob_names = [
            blob.name
            for blob in self._container_client.list_blobs()
            if len(blob.name.split("/")) > 1
        ]
        return self._list_filter(blob_names, filter_criteria) if filter_criteria else blob_names

    def show_blobs(self, directory_name: str) -> None:
        """Show blobs from a directory"""
        print(f"Container Name: {self.container_name}")
        print(f"\tDirectory Name: {directory_name}")
        for file in self._container_client.walk_blobs(
            name_starts_with=directory_name + "/", delimiter="/"
        ):
            print(f"\t\tBlob name: {file.name.rsplit('/', 1)[-1]}")

    def _list_filter(self, elements: list, character: str) -> List[str]:
        """Filter a list based on a criteria."""
        return [element for element in elements if character in element]
