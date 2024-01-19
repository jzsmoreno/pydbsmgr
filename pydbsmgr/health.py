from typing import List

import pandas as pd
from loguru import logger
from pandas.core.frame import DataFrame
import numpy as np
from abc import abstractmethod
import os
import missingno as msno
import yaml
from pydbsmgr.main import (
    check_dtypes,
    clean_transform,
    drop_empty_columns,
    intersection_cols,
)


class FrameCheck:
    """Class for checking and transforming a `DataFrame`/dataframes"""

    def __init__(self, _df: DataFrame | List[DataFrame], df_names: str | List[str] = None) -> None:
        self.df_names = df_names
        if isinstance(self.df_names, str):
            logger.add(self.df_names + "_{time}.log", rotation="100 KB")
        else:
            logger.add("report_{time}.log", rotation="100 KB")

        if isinstance(_df, list):
            self._dfs = _df
            assert len(self._dfs) > 0, "At least one dataframe must be provided"
        elif isinstance(_df, DataFrame):
            self._dfs = [_df]
        else:
            raise TypeError("Input should be either a single dataframe or a list of dataframes")

    def fix(self, cols_upper_case: bool = False, drop_empty_cols: bool = True) -> None:
        """Performs the clean of the data and validation

        Args:
        -----
            cols_upper_case (`bool`, optional): Indicates whether to convert column names to uppercase. Defaults to `False`.
            drop_empty_cols (`bool`, optional): Variable indicating whether columns with all their values empty should be removed. Defaults to `True`.
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

        Args:
        -----
            report_name (`str`, optional): Name of the quality assessment report. Defaults to `./report.html`.
            yaml_name (`str`, optional): Indicates the name of the `.yaml` file that will serve as a template for the creation of the SQL table. Defaults to `./output.yaml`.
            database_name (`str`, optional): The header of the `.yaml` file. Default value is `database`
            directory_name (`str`, optional): Folder in which the reports will be saved. Defaults to `summary`.
            concat_vertically: (`bool`, optional), Variable indicating whether the list of dataframes should be vertically concatenated into a single one. Default value is `False`.
            encoding (`str`, optional): The encoding of dataframes. Defaults to `utf-8`.
        """
        self.df_files_info = pd.DataFrame()
        self.yaml_name = yaml_name
        self.database_name = database_name
        if not os.path.exists(directory_name):
            os.mkdir(directory_name)
            warning_type = "UserWarning"
            msg = "The directory {%s} was created" % directory_name
            print(f"{warning_type}: {msg}")
            logger.info(f"The {directory_name} directory has been created.")

        if concat_vertically:
            self._dfs = intersection_cols(self._dfs)
            self._dfs = [pd.concat(self._dfs, axis=0)]

        for j, df in enumerate(self._dfs):
            ax = msno.matrix(self._dfs[j])
            ax.get_figure().savefig("./" + directory_name + f"/{j}_msno_report.png", dpi=300)
            info = []
            for col in df.columns:
                try:
                    if col.find("unnamed") == -1:
                        nrows = df.isnull().sum()[col] + df[col].count()
                        nrows_missing = df.isnull().sum()[col]
                        percentage = nrows_missing / nrows
                        datatype = df.dtypes[col]
                        unique_vals = len(df[col].unique())
                        info.append(
                            [
                                col,
                                datatype,
                                self.df_names[j],
                                nrows,
                                nrows_missing,
                                percentage,
                                unique_vals,
                            ]
                        )

                except:
                    pass  # column doesn't exist in this dataframe
            info = np.array(info).reshape((-1, 7))
            info = pd.DataFrame(
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
            info["# missing rows (percentage)"] = info["# missing rows (percentage)"].apply(
                lambda x: "{:.2%}".format(float(x))
            )
            info["# rows"] = info["# rows"].apply(lambda x: "{:,}".format(int(x)))
            info["# missing rows"] = info["# missing rows"].apply(lambda x: "{:,}".format(int(x)))
            info["unique values"] = info["unique values"].apply(lambda x: "{:,}".format(int(x)))

            logger.info(f"DataFrame '{self.df_names[j]}' has been processed")
            self.df_files_info = pd.concat([self.df_files_info, info])

        self.df_files_info.to_html(report_name, index=False, encoding=encoding)
        logger.info(f"A report has been created under the name '{report_name}'")

        self.df_files_info["data type"] = [
            str(_type) for _type in self.df_files_info["data type"].to_list()
        ]
        self.df_files_info["sql name"] = [
            col_name.replace(" ", "_") for col_name in self.df_files_info["column name"]
        ]

        self._create_yaml_tree()

    @abstractmethod
    def _ops_dtypes(self, df, count) -> DataFrame:
        """Processing of data types"""
        df = check_dtypes(df, df.dtypes)
        logger.info(f"{count+1}) The data type has been verified.")
        df = df.replace(r"(nan|Nan)", np.nan, regex=True)
        logger.info(f"{count+1}) The `nan` strings have been replaced by `np.nan`.")
        df = df.loc[:, ~df.columns.str.contains("^unnamed")]
        logger.info(f"{count+1}) Only the named columns have been retained.")
        return df

    def _create_yaml_tree(self) -> None:
        """Function that creates a `yaml` configuration file for database data type validation."""
        data = {}
        for col_name, data_type, sql_name in zip(
            self.df_files_info["column name"].to_list(),
            self.df_files_info["data type"],
            self.df_files_info["sql name"].to_list(),
        ):
            data[col_name] = {"type": [data_type], "sql_name": [sql_name]}

        yaml_data = yaml.dump({self.database_name: data})

        with open(self.yaml_name, "w") as file:
            file.write(yaml_data)
