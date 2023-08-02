import os

import numpy as np
import yaml
from loguru import logger
from pandas.core.frame import DataFrame


def check_column_types(df: DataFrame, drop_rows: bool = True, int_replace: int = -1) -> DataFrame:
    """
    Check and correct the data types of columns in a `DataFrame`.

    Parameters
    ----------
    df : `DataFrame`
        The input `DataFrame` to check and correct data types.
    drop_rows : `bool`, optional
        If `True`, rows with incorrect data types will be dropped, by default `True`.
    int_replace : `int`, optional
        The value used to replace incorrect integer values if `drop_rows` is `False`, by default -1.

    Returns
    -------
    DataFrame
        A new `DataFrame` with corrected data types or dropped rows based on the options chosen.
    """
    logger.remove(0)
    logger.add("check_col_types_{time}.log")


    def check_float(x):
        if isinstance(x, str):
            try:
                return float(x)
            except:
                return np.nan

    def check_int(x):
        if isinstance(x, str):
            try:
                return int(x)
            except:
                return np.nan

    df_ = df.copy()
    logger.info("`DataFrame` has been copied.")
    dict_dtypes = dict(zip(["float", "int", "str"], ["float64", "int64", "object"]))
    logger.info("Dictionary with created dtypes")
    for col in df_.columns:
        col_dtype = df_[col].dtype
        col_samples = df_[col].sample(n=round(len(df_[col]) * 0.01))
        for sample in col_samples:
            val_dtype = type(sample).__name__
            if not dict_dtypes[val_dtype] == col_dtype:
                # print(f'The dtype of the sample {sample} is {val_dtype} and does not match the dtype of the column {col} which is {col_dtype}')
                df_[col] = df_[col].replace("nan", np.nan)

                if val_dtype == "float":
                    df_[col] = df_[col].apply(check_float)
                    df_[col] = df_[col].astype(dict_dtypes[val_dtype])

                if val_dtype == "int":
                    df_[col] = df_[col].apply(check_int)
                    if drop_rows:
                        df_ = df_.loc[df_[col].notnull()]
                    else:
                        df_[col] = df_[col].fillna(int_replace)
                    df_[col] = df_[col].astype(dict_dtypes[val_dtype])
        logger.success(f"Successfully transformed the '{col}' column into {col_dtype}.")
    return df_


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
