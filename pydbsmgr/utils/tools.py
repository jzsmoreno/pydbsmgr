import concurrent.futures
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
from pandas.errors import IntCastingNaNError
from pyarrow import Table

from pydbsmgr.main import is_number_regex, check_if_contains_dates


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
        df.columns = df.columns.str.lower()
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
        files_not_loaded = []
        for table, blob_name in zip(pytables, names):
            if table != None:
                buf = pa.BufferOutputStream()
                pq.write_table(table, buf)
                parquet_data = buf.getvalue().to_pybytes()
                blob_path_name = directory_name + "/" + blob_name
                self._container_client.upload_blob(
                    name=blob_path_name + "." + format_type, data=parquet_data, overwrite=overwrite
                )
            else:
                files_not_loaded.append(blob_name)
        if len(files_not_loaded) > 0:
            return files_not_loaded

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
        files_not_loaded = []
        for data, blob_name in zip(dfs, names):
            if data != None:
                table = pa.Table.from_pandas(data)
                buf = pa.BufferOutputStream()
                pq.write_table(table, buf)
                parquet_data = buf.getvalue().to_pybytes()
                blob_path_name = directory_name + "/" + blob_name
                if upload:
                    self._container_client.upload_blob(
                        name=blob_path_name + "." + format_type,
                        data=parquet_data,
                        overwrite=overwrite,
                    )
                else:
                    files.append(parquet_data)
            else:
                files_not_loaded.append(blob_name)
        if len(files_not_loaded) > 0:
            return files_not_loaded

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

    def correct(
        self, drop_values: bool = False, drop_rows: bool = False, sample_frac: float = 0.1
    ) -> DataFrame:
        self._check_int_float(drop_values, drop_rows)
        self._check_datetime(sample_frac)
        return self.df

    def get_frame(self) -> DataFrame:
        return self.df

    def _check_int_float(self, drop_values: bool = False, drop_rows: bool = False) -> None:
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
                    return ""
            else:
                return x

        df_ = (self.df).copy()
        if drop_values:
            if len(df_) < 1e5:
                for col in df_.columns:
                    value = str(df_[col].iloc[0])
                    val_dtype = None
                    if is_number_regex(value):
                        if type(check_float(value)).__name__ == "float":
                            df_[col] = df_[col].apply(check_float)
                            df_[col] = df_[col].astype("float64")
                            val_dtype = "float64"
                            print("Checking {%s} for column {%s}" % (val_dtype, col))

                        if type(check_int(value)).__name__ == "int" and val_dtype == None:
                            df_[col] = df_[col].apply(check_int)
                            if drop_rows:
                                df_ = df_.loc[df_[col].notnull()]
                            try:
                                df_[col] = df_[col].astype("int64")
                                val_dtype = "int64"
                                print("Checking {%s} for column {%s}" % (val_dtype, col))
                            except IntCastingNaNError as e:
                                df_[col] = df_[col].astype("object")
                                val_dtype = "object"
                                print("Checking {%s} for column {%s}" % (val_dtype, col))

                        print(f"Successfully transformed the '{col}' column into {val_dtype}.")
        self.df = df_

    def _check_datetime(self, sample_frac: float) -> None:
        """
        Check and convert date-time string columns to datetime objects.
        """
        df_ = self.df
        cols = df_.columns
        df_sample = df_.sample(frac=sample_frac)
        for column_index, datatype in enumerate(df_.dtypes):
            col = cols[column_index]
            if datatype == "object":
                x = (df_sample[col].values)[0]
                datetype_column = True
                if isinstance(x, str):
                    if (
                        x == ""
                        or x.find("/") != -1
                        or x.find("-") != -1
                        or x == np.datetime64("NaT")
                    ):
                        datetype_column = (
                            (df_sample[col].apply(check_if_contains_dates)).isin([True]).any()
                        )
                    if not (x.find("//") or x.find("\\")) != -1 and datetype_column:
                        try:
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                df_[col] = list(
                                    executor.map(lambda date: date.replace("-", ""), df_[col])
                                )
                            df_[col] = pd.to_datetime(df_[col], format="%Y%m%d")
                            print(
                                f"Successfully transformed the '{col}' column into datetime64[ns]."
                            )
                        except:
                            with concurrent.futures.ThreadPoolExecutor() as executor:
                                df_[col] = list(executor.map(coerce_datetime, df_[col]))
                            df_[col] = pd.to_datetime(df_[col], format="%Y%m%d", errors="coerce")
                            print(
                                f"Successfully transformed the '{col}' column into datetime64[ns]."
                            )
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
