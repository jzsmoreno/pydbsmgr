import concurrent.futures
import glob
import os
import random
import re
import sys
from collections import Counter
from typing import Callable, List, Tuple

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pandas.core.frame import DataFrame
from pandas.errors import IntCastingNaNError
from pyarrow import Table

from pydbsmgr.main import check_if_contains_dates, is_number_regex
from pydbsmgr.utils.config import load_config, parse_config


def disableprints(func: Callable) -> Callable:
    """Decorator to temporarily suppress print statements in a function"""

    def wrapper(*args, **kwargs):
        with open(os.devnull, "w") as devnull:
            old_stdout = sys.stdout
            sys.stdout = devnull
            result = func(*args, **kwargs)
            sys.stdout = old_stdout
        return result

    if func.__doc__ is not None:
        wrapper.__doc__ = func.__doc__
    return wrapper


def most_repeated_item(items: List[str], two_most_common: bool = False) -> Tuple[str, str | None]:
    """Returns a `Tuple` with the most common elements of a `list`.

    Parameters
    ----------
    items : `List[str]`
        The list containing the items to be evaluated.
    two_most_common : `bool`, optional
        If `False`, returns only one element. Defaults to `False`.

    Returns
    -------
    Tuple[`str`, `str` | `None`]
        The two most common elements.
    """
    counter = Counter(items)
    most_common = counter.most_common(2)

    if two_most_common:
        return tuple(item for item, _ in most_common) + (None,) * (2 - len(most_common))
    else:
        return most_common[0], None


def generate_secure_password(pass_len: int = 24) -> str:
    """
    Generate a secure password with the length specified
    """
    config = parse_config(load_config("./pydbsmgr/utils/config.ini"))
    char_matrix = config["security"]["char_matrix"]
    return "".join(random.choice(char_matrix) for _ in range(pass_len))


def coerce_datetime(x: str) -> np.datetime64:
    try:
        x = x.replace("-", "")
        return pd.to_datetime(x, format="%Y%m%d")
    except ValueError:
        return np.datetime64("NaT")


class ColumnsCheck:
    """Performs checks on the columns of a DataFrame"""

    def __init__(self, df: DataFrame):
        self.df = df

    def get_frame(self, surrounding: bool = True) -> DataFrame:
        return self._process_columns(surrounding)

    def _process_columns(self, surrounding: bool) -> DataFrame:
        df = self.df.copy()
        df.columns = (
            df.columns.str.lower()
            .str.replace("[.,]", "", regex=True)
            .str.replace(r"[^a-zA-Z0-9ñáéíóú_]", "_", regex=True)
            .str.replace("_+", "_", regex=True)
            .str.strip()
            .str.rstrip("_")
        )
        if surrounding:
            df.columns = [f"[{col}]" for col in df.columns]
        return df


class ControllerFeatures:
    def __init__(self, container_client):
        self.container_client = container_client

    def write_pyarrow(
        self,
        directory_name: str,
        pytables: List[Table],
        names: List[str],
        overwrite: bool = True,
    ) -> List[str] | None:
        """Write PyArrow tables as Parquet format."""
        return self._write_tables(directory_name, pytables, names, "parquet", overwrite)

    def write_parquet(
        self,
        directory_name: str,
        dfs: List[DataFrame],
        names: List[str],
        overwrite: bool = True,
        upload: bool = True,
    ) -> List[str] | List[bytes] | None:
        """Write DataFrames as Parquet format by converting them to bytes first."""
        pytables = [pa.Table.from_pandas(df) for df in dfs]
        return self._write_tables(directory_name, pytables, names, "parquet", overwrite, upload)

    def _write_tables(
        self,
        directory_name: str,
        tables: List[Table],
        names: List[str],
        format_type: str,
        overwrite: bool = True,
        upload: bool = True,
    ) -> List[str] | List[bytes] | None:
        files_not_loaded, collected_files = [], []
        for table, blob_name in zip(tables, names):
            if table is not None:
                buf = pa.BufferOutputStream()
                pq.write_table(table, buf)
                parquet_data = buf.getvalue().to_pybytes()
                blob_path_name = os.path.join(directory_name, f"{blob_name}.{format_type}")
                if upload:
                    self.container_client.upload_blob(
                        name=blob_path_name,
                        data=parquet_data,
                        overwrite=overwrite,
                    )
                else:
                    collected_files.append(parquet_data)
            else:
                files_not_loaded.append(blob_name)

        return files_not_loaded or (collected_files if not upload else None)


def column_coincidence(df1: DataFrame, df2: DataFrame) -> float:
    """Return the percentage of coincident columns between two pandas DataFrames."""
    set_columns1 = set(df1.columns)
    set_columns2 = set(df2.columns)

    common_columns = set_columns1.intersection(set_columns2)
    total_columns = set_columns1.union(set_columns2)

    return len(common_columns) / len(total_columns)


def merge_by_coincidence(df1: DataFrame, df2: DataFrame, tol: float = 0.9) -> DataFrame:
    """Merge two pandas DataFrames by finding the most similar columns based on their names."""
    percentage = column_coincidence(df1, df2)
    if percentage < tol:
        print(
            f"The following columns were missed with a match percentage of {percentage*100:.2f}%: "
            f"{set(df1.columns).union(set(df2.columns)) - set(df1.columns).intersection(set(df2.columns))}"
        )

    common_cols = set(df1.columns).intersection(set(df2.columns))
    df_combined = pd.concat([df1[common_cols], df2[common_cols]], ignore_index=True)

    return df_combined


def terminate_process(file_path: str) -> None:
    """Terminate the process holding the file."""
    for proc in psutil.process_iter(["pid", "open_files"]):
        try:
            if any(file_info.path == file_path for file_info in proc.info["open_files"] or []):
                print(f"Terminating process {proc.pid} holding the file.")
                proc.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass


def erase_files(format: str = "log") -> None:
    """Erase all files with the given format."""
    for filename in glob.glob(f"*.{format}"):
        try:
            os.remove(filename)
        except OSError:
            terminate_process(filename)
            os.remove(filename)


def get_extraction_date(
    filename: str | List[str], REGEX_PATTERN: str = r"\d{4}-\d{2}-\d{2}"
) -> str | List[str]:
    """Allows to extract the date of extraction according to the directory within the storage account.

    Parameters
    ----------
    filename : Union[str, List[str]]
        file path inside the storage account
    REGEX_PATTERN : `str`, `optional`
        regular expression pattern to extract the date.

    Returns
    -------
    `Union[str, List[str]]`
        the date that was extracted if found in the file path.
    """

    def sub_extraction_date(filename: str) -> str:
        extraction_date = re.findall(REGEX_PATTERN, filename)
        return extraction_date[0] if extraction_date else ""

    if isinstance(filename, list):
        return [sub_extraction_date(name) for name in filename]
    return sub_extraction_date(filename)


class ColumnsDtypes:
    """Convert all columns to specified dtype."""

    def __init__(self, df_: DataFrame):
        self.df = df_.copy()

    def correct(
        self,
        drop_values: bool = False,
        drop_rows: bool = False,
        sample_frac: float = 0.1,
    ) -> DataFrame:
        self._check_int_float(drop_values, drop_rows)
        self._check_datetime(sample_frac)
        return self.df

    def get_frame(self) -> DataFrame:
        return self.df

    def _check_int_float(self, drop_values: bool, drop_rows: bool) -> None:
        """Check and correct the data types of columns in a `DataFrame`."""

        def check_value(value):
            try:
                if float(value).is_integer():
                    return int(value)
                return float(value)
            except ValueError:
                return np.nan

        if len(self.df) < 1e5 or not drop_values:
            for col in self.df.columns:
                value = str(self.df[col].iloc[0])
                if is_number_regex(value):
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        self.df[col] = list(executor.map(check_value, self.df[col]))
                    try:
                        self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
                        print(f"Successfully transformed the '{col}' column into numeric.")
                    except ValueError:
                        self.df[col] = self.df[col].astype("object")
                        print(f"Failed to transform the '{col}' column, setting to object.")

        if drop_rows:
            self.df.dropna(inplace=True)

    def _check_datetime(self, sample_frac: float) -> None:
        """Check and convert date-time string columns to `datetime` objects."""
        df_sample = self.df.sample(frac=sample_frac)
        for col in self.df.columns:
            if pd.api.types.is_string_dtype(df_sample[col]):
                if (df_sample[col].apply(check_if_contains_dates)).any():
                    try:
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            self.df[col] = list(executor.map(coerce_datetime, self.df[col]))
                        print(f"Successfully transformed the '{col}' column into datetime64[ns].")
                    except ValueError:
                        print(f"Failed to transform the '{col}' column into datetime.")


def create_directory(data, parent_path=""):
    """Creates the directory tree from a `yaml` file."""
    for key, value in data.items():
        path = os.path.join(parent_path, key)
        if isinstance(value, dict):
            os.makedirs(path, exist_ok=True)
            create_directory(value, path)
        else:
            os.makedirs(path, exist_ok=True)


def create_directories_from_yaml(yaml_file):
    """Reads a `yaml` file and creates directories based on its content."""
    with open(yaml_file, "r") as file:
        data = yaml.safe_load(file)
        create_directory(data)
