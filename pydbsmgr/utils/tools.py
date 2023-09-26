import glob
import os
import re
from typing import List

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from numpy import datetime64
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pyarrow import Table


class ColumnsCheck:
    """Performs the relevant checks on the columns of the `DataFrame`"""

    def __init__(self, df: DataFrame):
        self.df = df

    def get_frame(self) -> DataFrame:
        self.df = self._process_columns()
        self.df = self._check_reserved_words()
        return self.df

    def _process_columns(self) -> DataFrame:
        df = (self.df).copy()
        df.columns = df.columns.str.replace(".", "")
        df.columns = df.columns.str.replace(",", "")
        df.columns = df.columns.str.replace("__", "_")
        new_cols = []
        for col in df.columns:
            res = any(chr.isdigit() for chr in col)
            if res:
                col = "[" + col + "]"
            else:
                col = re.sub("[^a-zA-Z0-9ñáéíóú_]", "_", col)
            new_cols.append(col)

        df.columns = new_cols
        return df

    def _check_reserved_words(self) -> DataFrame:
        df = (self.df).copy()
        new_cols = []
        for col in df.columns:
            # SQL reserved words
            reserved_words = [
                "update",
                "insert",
                "delete",
                "create",
                "drop",
                "truncate",
                "into",
                "from",
                "where",
                "group",
                "view",
            ]
            if col in reserved_words:
                col = "[" + col + "]"
            new_cols.append(col)
        df.columns = new_cols
        return df


def coerce_datetime(x: str) -> datetime64:
    try:
        x = x.replace("-", "")
        return pd.to_datetime(x, format="%Y%m%d")
    except:
        return np.datetime64("NaT")


class ControllerFeatures:
    def __init__(self, _container_client):
        self._container_client = _container_client

    def write_pyarrow(
        self,
        directory_name: str,
        pytables: List[Table],
        names: List[str],
        overwrite: bool = True,
    ) -> None:
        """Write pyarrow table as `parquet` format"""
        format_type = "parquet"
        for table, blob_name in zip(pytables, names):
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf)
            parquet_data = buf.getvalue().to_pybytes()
            blob_path_name = directory_name + "/" + blob_name
            self._container_client.upload_blob(
                name=blob_path_name + "." + format_type, data=parquet_data, overwrite=overwrite
            )

    def write_parquet(
        self,
        directory_name: str,
        dfs: List[DataFrame],
        names: List[str],
        overwrite: bool = True,
        upload: bool = True,
    ) -> None:
        """Write dataframes as `parquet` format by converting them first into `bytes`"""
        files = []
        format_type = "parquet"
        for data, blob_name in zip(dfs, names):
            table = pa.Table.from_pandas(data)
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf)
            parquet_data = buf.getvalue().to_pybytes()
            blob_path_name = directory_name + "/" + blob_name
            if upload:
                self._container_client.upload_blob(
                    name=blob_path_name + "." + format_type, data=parquet_data, overwrite=overwrite
                )
            else:
                files.append(parquet_data)

        if not upload:
            return files


def column_coincidence(df1: DataFrame, df2: DataFrame) -> float:
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise ValueError("Both inputs should be pandas DataFrames")

    column_names1 = set(df1.columns)
    column_names2 = set(df2.columns)

    common_columns = column_names1.intersection(column_names2)
    total_columns = column_names1.union(column_names2)

    coincidence_percentage = len(common_columns) / len(total_columns)
    return coincidence_percentage


def merge_by_coincidence(df1: DataFrame, df2: DataFrame, tol: float = 0.9) -> DataFrame:
    percentage = column_coincidence(df1, df2)
    total_columns = set(df1.columns).union(set(df2.columns))
    num_col1 = len(df1.columns)
    num_col2 = len(df2.columns)
    if num_col1 < num_col2:
        min_cols = set(df1.columns)
        min_cols = list(min_cols.intersection(set(df2.columns)))
    else:
        min_cols = set(df2.columns)
        min_cols = list(min_cols.intersection(set(df1.columns)))

    df2 = (df2[min_cols]).copy()
    df1 = (df1[min_cols]).copy()
    diff = total_columns.difference(set(min_cols))

    df = pd.concat([df1, df2], ignore_index=True)
    if percentage > tol:
        print("The following columns were lost : ", diff)
    else:
        print(
            f"The following columns were missed with a match percentage of {percentage*100:.2f}% : ",
            diff,
        )
    return df


def terminate_process_holding_file(file_path):
    for proc in psutil.process_iter(["pid", "open_files"]):
        try:
            if any(file_path in file_info.path for file_info in proc.open_files()):
                print(f"Terminating process {proc.pid} holding the file.")
                proc.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


def erase_files(format: str = "log") -> None:
    for filename in glob.glob("*." + format):
        try:
            os.remove(filename)
        except:
            terminate_process_holding_file(filename)
            os.remove(filename)


class ColumnsDtypes:
    """Convert all columns to specified dtype."""

    def __init__(self, df_: DataFrame):
        self.df = df_.copy()

    def correct(self) -> DataFrame:
        self._check_int_float()
        self._check_datetime()
        return self.df

    def get_frame(self) -> DataFrame:
        return self.df

    def _check_int_float(self, drop_rows: bool = False) -> None:
        """
        Check and correct the data types of columns in a `DataFrame`.
        """

        def check_float(x):
            if isinstance(x, str):
                try:
                    return float(x)
                except:
                    return np.nan
            else:
                return x

        def check_int(x):
            if isinstance(x, str):
                try:
                    return int(x)
                except:
                    return np.nan
            else:
                return x

        df_ = (self.df).copy()
        dict_dtypes = dict(zip(["float", "int", "str"], ["float64", "Int64", "object"]))
        for col in df_.columns:
            col_dtype = df_[col].dtype
            col_samples = df_[col].sample(n=round(len(df_[col]) * 0.01))
            for sample in col_samples:
                val_dtype = type(sample).__name__
                if not dict_dtypes[val_dtype] == col_dtype:
                    df_[col] = df_[col].replace("nan", np.nan)

                    if col_dtype == "float":
                        df_[col] = df_[col].apply(check_float)
                        df_[col] = df_[col].astype(dict_dtypes[val_dtype])

                    if col_dtype == "int":
                        df_[col] = df_[col].apply(check_int)
                        if drop_rows:
                            df_ = df_.loc[df_[col].notnull()]
                        else:
                            df_[col] = df_[col].astype(dict_dtypes[val_dtype])
            print(f"Successfully transformed the '{col}' column into {col_dtype}.")
        self.df = df_

    def _check_datetime(self) -> None:
        """
        Check and convert date-time string columns to datetime objects.
        """
        df_ = self.df

        for col in df_.columns:
            if (col.lower()).find("fecha") != -1:
                try:
                    df_[col] = df_[col].apply(lambda date: date.replace("-", ""))
                    df_[col] = pd.to_datetime(df_[col], format="%Y%m%d")
                    print(f"Successfully transformed the '{col}' column into datetime64[ns].")
                except:
                    None
            elif (col.lower()).find("date") != -1:
                try:
                    df_[col] = df_[col].apply(coerce_datetime)
                    df_[col] = pd.to_datetime(df_[col], format="%Y%m%d", errors="coerce")
                    print(f"Successfully transformed the '{col}' column into datetime64[ns].")
                except:
                    print(f"The column '{col}' could not be converted")
        self.df = df_


def create_directory(data, parent_path=""):
    """
    Creates the directory tree from a yaml file
    """
    for key, value in data.items():
        path = os.path.join(parent_path, key)
        if isinstance(value, dict):
            os.makedirs(path, exist_ok=True)
            create_directory(value, path)
        else:
            os.makedirs(path, exist_ok=True)


def create_directories_from_yaml(yaml_file):
    with open(yaml_file, "r") as file:
        data = yaml.safe_load(file)
        create_directory(data)


if __name__ == "__main__":
    yaml_file = "directories.yaml"
    create_directories_from_yaml(yaml_file)
