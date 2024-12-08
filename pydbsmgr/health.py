import os
from abc import abstractmethod
from typing import List, Union

import missingno as msno
import numpy as np
import pandas as pd
import yaml
from loguru import logger
from pandas.core.frame import DataFrame

from pydbsmgr.main import check_dtypes, clean_transform, drop_empty_columns, intersection_cols

# Configure logging globally
LOG_FILE = "report_{time}.log"
logger.add(LOG_FILE, rotation="100 KB")


class FrameCheck:
    """Class for checking and transforming a `DataFrame`/dataframes"""

    def __init__(
        self, _df: Union[DataFrame, List[DataFrame]], df_names: Union[str, List[str]] = None
    ) -> None:
        self.df_names = df_names

        if isinstance(self.df_names, str):
            logger.add(f"{self.df_names}_{{time}}.log", rotation="100 KB")

        if isinstance(_df, list):
            self._dfs = _df
            assert len(self._dfs) > 0, "At least one dataframe must be provided"
        elif isinstance(_df, DataFrame):
            self._dfs = [_df]
        else:
            raise TypeError("Input should be either a single dataframe or a list of dataframes")

    def fix(self, cols_upper_case: bool = False, drop_empty_cols: bool = True) -> None:
        """Performs the clean of the data and validation

        Parameters
        ----------
        cols_upper_case : `bool`, `optional`
            Indicates whether to convert column names to uppercase. Defaults to `False`.
        drop_empty_cols : `bool`, `optional`
            Variable indicating whether columns with all their values empty should be removed. Defaults to `True`.
        """
        if drop_empty_cols:
            for count, df in enumerate(self._dfs):
                self._dfs[count] = drop_empty_columns(df)
                logger.info(f"{count+1}) Empty columns have been removed.")
                self._dfs[count].columns = clean_transform(df.columns, cols_upper_case)
                logger.info(f"{count+1}) Columns have been cleaned and transformed.")
                self._dfs[count] = self._ops_dtypes(df, count)

    def get_frames(self) -> List[DataFrame]:
        return self._dfs

    def generate_report(
        self,
        report_name: str = "./report.html",
        yaml_name: str = "./output.yaml",
        database_name: str = "database",
        directory_name: str = "summary",
        concat_vertically: bool = False,
        encoding: str = "utf-8",
    ) -> None:
        """Generate a `.html` health check report.

        Parameters
        ----------
        report_name : `str`, `optional`
            Name of the quality assessment report. Defaults to `./report.html`.
        yaml_name : `str`, `optional`
            Indicates the name of the `.yaml` file that will serve as a template for the creation of the SQL table. Defaults to `./output.yaml`.
        database_name : `str`, `optional`
            The header of the `.yaml` file. Default value is `database`
        directory_name : `str`, `optional`
            Folder in which the reports will be saved. Defaults to `summary`.
        concat_vertically : `bool`, `optional`
            Variable indicating whether the list of dataframes should be vertically concatenated into a single one. Default value is `False`.
        encoding : `str`, `optional`
            The encoding of dataframes. Defaults to `utf-8`.
        """
        self.df_files_info = pd.DataFrame()
        self.yaml_name = yaml_name
        self.database_name = database_name

        if not os.path.exists(directory_name):
            os.mkdir(directory_name)
            logger.warning(f"The {directory_name} directory has been created.")

        if concat_vertically:
            self._dfs = intersection_cols(self._dfs)
            self._dfs = [pd.concat(self._dfs, axis=0)]

        for j, df in enumerate(self._dfs):
            ax = msno.matrix(df)
            ax.get_figure().savefig(f"./{directory_name}/{j}_msno_report.png", dpi=300)

            info = [
                [
                    col,
                    str(df.dtypes[col]),
                    self.df_names[j],
                    len(df),
                    df.isnull().sum()[col],
                    df.isnull().sum()[col] / len(df),
                    len(df[col].unique()),
                ]
                for col in df.columns
                if "unnamed" not in col.lower()
            ]

            info_df = pd.DataFrame(
                info,
                columns=[
                    "column name",
                    "data type",
                    "database name",
                    "# rows",
                    "# missing rows",
                    "# missing rows (percentage)",
                    "unique values",
                ],
            )

            self._format_info_df(info_df)
            logger.info(f"DataFrame '{self.df_names[j]}' has been processed")
            self.df_files_info = pd.concat([self.df_files_info, info_df])

        self.df_files_info.to_html(report_name, index=False, encoding=encoding)
        logger.info(f"A report has been created under the name '{report_name}'")

        self._create_yaml_tree()

    def _format_info_df(self, df: DataFrame) -> None:
        df["# missing rows (percentage)"] = df["# missing rows (percentage)"].apply(
            lambda x: f"{x:.2%}"
        )
        df["# rows"] = df["# rows"].apply(lambda x: f"{int(x):,}")
        df["# missing rows"] = df["# missing rows"].apply(lambda x: f"{int(x):,}")
        df["unique values"] = df["unique values"].apply(lambda x: f"{int(x):,}")

    @abstractmethod
    def _ops_dtypes(self, df: DataFrame, count: int) -> DataFrame:
        """Processing of data types"""
        df = check_dtypes(df, df.dtypes)
        logger.info(f"{count+1}) The data type has been verified.")
        df = df.replace(r"(nan|Nan)", np.nan, regex=True)
        logger.info(f"{count+1}) The `nan` strings have been replaced by `np.nan`.")
        df = df.loc[:, ~df.columns.str.contains("^unnamed", case=False)]
        logger.info(f"{count+1}) Only the named columns have been retained.")
        return df

    def _create_yaml_tree(self) -> None:
        """Function that creates a `yaml` configuration file for database data type validation."""
        data = {
            col_name: {"type": [data_type], "sql_name": [col_name.replace(" ", "_")]}
            for col_name, data_type in zip(
                self.df_files_info["column name"].to_list(), self.df_files_info["data type"]
            )
        }

        with open(self.yaml_name, "w") as file:
            yaml.dump({self.database_name: data}, file)
