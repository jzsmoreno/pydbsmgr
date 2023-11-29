import datetime
import glob
import os
import re
import sys
import warnings
from typing import List, Tuple

import missingno as msno
import numpy as np
import pandas as pd
import yaml
from cleantext import clean
from IPython.display import clear_output
from loguru import logger
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pandas.core.series import Series

warnings.filterwarnings("ignore")

########################################################################################


def get_date_format(input_string: str) -> str:
    """Infer the date format from a given string."""
    regex_formats = [
        r"\d{4}(-|/)[0-1]+[0-9](-|/)[0-3]+[0-9]",
        r"\d{4}(-|/)[0-3]+[0-9](-|/)[0-1]+[0-9]",
        r"[0-3]+[0-9](-|/)[0-1]+[0-9](-|/)\d{4}",
        r"[0-1]+[0-9](-|/)[0-3]+[0-9](-|/)\d{4}",
    ]
    formats = ["%Y%m%d", "%Y%d%m", "%d%m%Y", "%m%d%Y"]
    for format, regex in enumerate(regex_formats):
        if re.search(regex, str(input_string)):
            return formats[format]

    return ""


def check_if_contains_dates(input_string: str) -> bool:
    """Check if a string contains date."""
    if input_string == "":
        return False
    else:
        if re.search(r"\d{4}(-|/)\d{1,2}(-|/)\d{1,2}", str(input_string)):
            return True
        else:
            if re.search(r"\d{1,2}(-|/)\d{1,2}(-|/)\d{4}", str(input_string)):
                return True
            else:
                return False


def remove_numeric_char(input_string: str) -> str:
    """Remove all numeric characters from a string.

    Args:
        input_string (`str`): character string to be cleaned of numeric characters

    Returns:
        `str`: clean character string
    """
    return re.sub(r"\d", "", input_string)


def clean_names(dirty_string: str, pattern: str = r"[a-zA-Zñáéíóú_]+\b") -> str:
    """
    Receive a string and clean it of special characters

    Parameters
    ----------
    dirty_string : str
        string of characters
    pattern : str
        regular expression string

    Returns
    -------
    result : str
        clean character string
    """
    result = re.findall(pattern, str(dirty_string).replace("_", ""))
    if len(result) > 0:
        result = "_".join(result)
    else:
        pattern = r"[a-zA-Z]+"
        result = re.findall(pattern, str(dirty_string).replace("_", ""))
        result = "_".join(result)
    return result


def clean_transform_helper(
    col: str, mode: bool = True, remove_numeric: bool = True, remove_spaces: bool = True
) -> str:
    """
    Transforms a column name by cleaning the column name and if needed makes it capital.

    Parameters
    ----------
    col : str
        The column name to be transformed.
    mode : bool
        Indicates if names will be capitalized. By default it is set to `True`.
    remove_numeric : bool
        Indicates if numeric characters will be removed. By default it is set to `True`.
    remove_spaces : bool
        Indicates if spaces will be removed. By default it is set to `True`.
    Returns
    ----------
    col_name : str
        The transformed column name.
    """
    col_name = remove_char(str(clean(col)))
    if mode:
        col_name = col_name.title()
    if remove_numeric:
        col_name = remove_numeric_char(col_name).strip()
    if remove_spaces:
        col_name = col_name.replace(" ", "_").replace("-", "_").replace("/", "_")

    return col_name


def clean_transform(
    col_index: Index,
    mode: bool = True,
    remove_spaces: bool = True,
    remove_numeric: bool = True,
) -> List[str]:
    """
    Transforms a column index by cleaning the column names and if needed makes them capital.

    Parameters
    ----------
    col_index : Index
        The column index to be transformed.
    mode : bool
        Indicates if names will be capitalized. By default it is set to `True`.

    Returns
    ----------
    col_name_list : str
        The transformed column names as a `list` of strings.
    """
    return [
        clean_transform_helper(
            col, mode=mode, remove_spaces=remove_spaces, remove_numeric=remove_numeric
        )
        for col in col_index
    ]


def remove_char(input_string: str) -> str:
    """
    Removes special characters from a string.

    Parameters
    ----------
    input_string : str
        The input string from which characters will be removed.

    Returns
    ----------
    input_string : str
        The string with specified characters removed.
    """
    list_of_char = ["#", "$", "*", "?", "!", "(", ")", "&", "%"]
    for char in list_of_char:
        try:
            input_string = input_string.replace(char, "")
        except:
            pass
    input_string = correct_nan(input_string)
    return input_string


def check_if_isemail(check_email: str) -> Tuple[str, bool]:
    """
    Checks if a string is an email address and returns the cleaned string and a flag indicating if the string is an email.

    Parameters
    ----------
    check_email : str
        The input string to be checked for an email address.

    Returns
    ----------
    check_email, found_email : str, bool
        A tuple containing the cleaned string and a boolean flag indicating if an email address was found.
    """
    found_email = False
    if str(check_email).find("@") != -1:
        check_email = str(clean(check_email))
        found_email = True
        print(f"An e-mail has been detected.")

    return check_email, found_email


def convert_date(date_string: str) -> str:
    """
    Converts a `str` of a date to a proper `datetime64[ns]` format.

    Parameters
    ----------
    date_string : str
        The input string representing a date.

    Returns
    -------
    proper_date : str
        The date string in the proper format `YYYY-MM-DD`.
    """
    try:
        proper_date = str(pd.to_datetime(date_string, format="%Y%m%d", errors="raise"))[:10]
    except:
        try:
            proper_date = str(pd.to_datetime(date_string, format="%d%m%Y", errors="raise"))[:10]
        except:
            proper_date = str(pd.to_datetime(date_string, format="%Y%m%d", errors="ignore"))[:10]
    return proper_date


def is_number_regex(s):
    """Returns True if string is a number."""
    if re.match("^\d+?\.\d+?$", s) is None:
        return s.isdigit()
    return True


def clean_and_convert_to(x: str) -> str:
    """
    Performs cleaning and some conversions on a `str`.

    Parameters
    ----------
    x : `str`
        The input string to be cleaned and converted.

    Returns
    -------
    x : `str`
        The cleaned and converted string.
    """

    # Consider cases where a number is passed as a `str`
    if is_number_regex(str(x)):
        if str(x).find(".") != -1:
            try:
                return float(x)
            except:
                # Could not convert to float, converted to `np.nan`.
                return np.nan
        else:
            try:
                return int(x)
            except:
                # Could not convert to `int`, converted to `np.nan`.
                return np.nan
    else:
        # Consider cases in which a `float` number is passed as a `str` and is erroneous
        if str(x).find(".") != -1:
            try:
                return float(x)
            except:
                # Could not convert {x} to float, converting to `str`...
                x = str(x)
                # Successfully converted {x} to `str`.
        # Cases in which we have an identifier with numbers and letters
        else:
            result = re.findall(r"^[A-Za-z0-9]+$", str(x))
            try:
                return result[0]
            # Case in which none of the above applies
            except:
                x = str(x)

    x = remove_char(x)
    try:
        x, find_ = check_if_isemail(x)
        if (x.find("/") != -1 or x.find("-")) != -1 and not (x.find("//") or x.find("\\")) != -1:
            x_ = x.replace("/", "")
            x_ = x_.replace("-", "")

            if len(x_) == 8:
                x = convert_date(x_)
            else:
                if str(x_).find(":") != -1:
                    x = convert_date(x_[:8])
                else:
                    # No date found.
                    x = clean(x)
                    x = x.title()
        else:
            if not find_:
                if str(x).find(".") != -1:
                    x_ = x.replace(".", "")
                    if len(x) == 8:
                        x = convert_date(x_)
                    else:
                        if x.find("//") == -1:
                            x_ = x.replace(".", " ")
                            x_ = " ".join(x_.split())
                            x_ = clean(x_)
                            x = x_.title()
                else:
                    x = clean(x)
                    x = " ".join(x.split())
                    x = x.title()
    except:
        print(f"No transformation has been performed, the character will be returned as it came.")
        None
    return x


def correct_nan(check_missing: str) -> str:
    """
    Corrects the format of missing values in a `str` to the correct `empty str`.

    Parameters
    ----------
    check_missing : `str`
        The string to be checked for incorrect missing value format.

    Returns
    -------
    check_missing : `str`
        The corrected string format or `empty str`.
    """
    if str(check_missing).lower() == "nan":
        return ""
    return check_missing


def check_dtypes(dataframe: DataFrame, datatypes: Series) -> DataFrame:
    """
    Checks and updates the data types of columns in a `DataFrame`.

    Parameters
    ----------
    dataframe : `DataFrame`
        The `DataFrame` to check and update the data types.
    datatypes : `Series`
        The `Series` containing the desired data types for each column in the `DataFrame`.

    Returns
    -------
    dataframe : `DataFrame`
        The `DataFrame` with updated data types.
    """
    cols = dataframe.columns

    for column_index, datatype in enumerate(datatypes):
        if datatype == "object" or datatype == "datetime64[ns]":
            dataframe[cols[column_index]] = dataframe[cols[column_index]].apply(
                clean_and_convert_to
            )
            dataframe[cols[column_index]] = dataframe[cols[column_index]].apply(correct_nan)
            try:
                dataframe[cols[column_index]] = dataframe[cols[column_index]].map(str.strip)
            except:
                try:
                    dataframe[cols[column_index]] = dataframe[cols[column_index]].astype(
                        "datetime64[ns]"
                    )
                except:
                    warning_type = "UserWarning"
                    msg = (
                        "It was not possible to convert the column {%s} to datetime64[ns] type"
                        % cols[column_index]
                    )
                    print(f"{warning_type}: {msg}")
    return dataframe


def create_yaml_from_df(
    df_: DataFrame, yaml_name: str = "./output.yaml", dabase_name: str = "database"
) -> None:
    """
    Function that creates a `yaml` configuration file from a `DataFrame` for data type validation.

    Parameters
    ----------
    df_ : `DataFrame`
        The DataFrame.
    yaml_name : `str`
        The name of the `yaml` configuration file to be created. By default it is set to `./output.yaml`
    database_name : `str`
        The header of the `.yaml` file. By default it is set to `database`

    Returns
    -------
        `None`.
    """
    df_info, df = check_values(df_, df_name="df_name", sheet_name="sheet_name")

    df_info["data type"] = [str(_type) for _type in df_info["data type"].to_list()]
    df_info["sql name"] = [col_name.replace(" ", "_") for col_name in df_info["column name"]]

    data = {}
    for col_name, data_type, sql_name in zip(
        df_info["column name"].to_list(),
        df_info["data type"],
        df_info["sql name"].to_list(),
    ):
        data[col_name] = {"type": [data_type], "sql_name": [sql_name]}

    yaml_data = yaml.dump({dabase_name: data})

    with open(yaml_name, "w") as file:
        file.write(yaml_data)


def create_yaml_tree(yaml_name: str, df_info: DataFrame, dabase_name: str = "database") -> None:
    """
    Function that creates a `yaml` configuration file for database data type validation.

    Parameters
    ----------
    yaml_name : `str`
        The name of the `yaml` configuration file to be created.
    df_info : `DataFrame`
        The DataFrame with the column information for data type validation.
    database_name : `str`
        The header of the `.yaml` file. By default it is set to `database`

    Returns
    -------
        `None`.
    """
    data = {}
    for col_name, data_type, sql_name in zip(
        df_info["column name"].to_list(),
        df_info["data type"],
        df_info["sql name"].to_list(),
    ):
        data[col_name] = {"type": [data_type], "sql_name": [sql_name]}

    yaml_data = yaml.dump({dabase_name: data})

    with open(yaml_name, "w") as file:
        file.write(yaml_data)


def drop_empty_columns(df_: DataFrame) -> DataFrame:
    """
    Function that removes empty columns
    """
    cols_to_keep = []
    for col in df_.columns:
        if not (pd.isnull(df_[col]).sum() == len(df_[col])):
            cols_to_keep.append(col)
    return df_[cols_to_keep].copy()


def intersection_cols(dfs_: List[DataFrame]) -> DataFrame:
    """
    Function that resolves columns issues of a `list` of dataframes

    Parameters
    ----------
    dfs_ : List[`DataFrame`]
        The `list` of dataframes with columns to be resolves.

    Returns
    -------
    dfs_ : List[`DataFrame`]
        The `list` of dataframes with the corrections in their columns (intersection).
    """
    min_cols = []
    index_dfs = []
    for i, df in enumerate(dfs_):
        min_cols.append(len(df.columns))
        index_dfs.append(i)
    df_dict = dict(zip(min_cols, index_dfs))

    min_col = min(min_cols)
    logger.info(f"The minimum number of columns is {min_col}.")
    index_min = df_dict[min_col]
    cols_ = set(dfs_[index_min].columns)
    for i, df in enumerate(dfs_):
        dfs_[i] = (dfs_[i][list(cols_)]).copy()
    logger.success(f"Successfully maintains only intersecting columns.")

    return dfs_


def check_values(
    df_: DataFrame,
    df_name: str,
    mode: bool = False,
    cols_upper_case: bool = False,
    drop_empty_cols: bool = True,
    directory_name: str = "report",
) -> Tuple[DataFrame, DataFrame]:
    """
    Performs the clean of the data and validation on a `DataFrame`, also creates a quality assesment report.

    Parameters
    ----------
    df_ : `DataFrame`
        The `DataFrame` to be validated.
    df_name : `str`
        The name of the `DataFrame`.
    mode : `bool`
        Indicates whether to generate a visualization and report in `html`. By default it is set to `False`.
    cols_upper_case : `bool`
        Indicates whether to convert column names to uppercase. By default it is set to `False`.

    Returns
    -------
    info, df : `DataFrame`, `DataFrame`
        A tuple containing the information `DataFrame` and the validated `DataFrame`.
    """
    df = df_.copy()
    logger.add(df_name + "_{time}.log", rotation="100 KB")
    logger.info(f"`DataFrame` has been copied.")

    if drop_empty_cols:
        df = drop_empty_columns(df)
        logger.info(f"Empty columns have been removed.")

    df.columns = clean_transform(df.columns, cols_upper_case)
    logger.info("Columns have been cleaned and transformed.")
    df = check_dtypes(df, df.dtypes)
    logger.info("The data type has been verified.")
    df = df.replace("Nan", np.nan)
    logger.info("The `nan` strings have been replaced by `np.nan`.")
    df = df.loc[:, ~df.columns.str.contains("^unnamed")]
    logger.info("Only the named columns have been retained.")

    info = []
    title = "Report " + df_name
    if mode:
        # profile = ProfileReport(df, title=title)
        # profile.to_file("./" + directory_name + "/" + title + ".html")

        if not os.path.exists(directory_name):
            os.mkdir(directory_name)
            warning_type = "UserWarning"
            msg = "The directory {%s} was created" % directory_name
            print(f"{warning_type}: {msg}")
            logger.info(f"The {directory_name} directory has been created.")

        ax = msno.matrix(df)
        ax.get_figure().savefig("./" + directory_name + "/" + title + ".png", dpi=300)
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
                        df_name,
                        nrows,
                        nrows_missing,
                        percentage,
                        unique_vals,
                    ]
                )

        except:
            None
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

    return info, df


def check_for_list(
    dfs_: List[DataFrame],
    dfs_names: List[str],
    mode: bool = False,
    yaml_name: str = "./output.yaml",
    report_name: str = "./report-health-checker.html",
    encoding: str = "latin1",
    concat_vertically: bool = False,
    drop_empty_cols: bool = True,
) -> Tuple[DataFrame, DataFrame]:
    """Function that performs the implementation of the check_values function on lists of dataframes.

    Parameters
    ----------
    dfs_ : List[`DataFrame`]
        The `list` of dataframes to be validated.
    dfs_names : List[str]
        The `list` containing the dataframe names.
    mode : `bool`
        Indicates whether to generate a visualization and report in `html`. By default it is set to `False`.
    yaml_name : `str`
        Indicates the name of the `.yaml` file that will serve as a template for the creation of the SQL table. By default it is set to `./output.yaml`
    report_name : `str`
        Name of the quality assessment report. By default it is set to `./report-health-checker.html`
    encoding : `str`
        The encoding of dataframes. By default it is set to `latin1`
    concat_vertically : `bool`
        Variable indicating whether the list of dataframes should be vertically concatenated into a single one. By default it is set to `False`
    drop_empty_cols : `bool`
        Variable indicating whether columns with all their values empty should be removed. By default it is set to `True`

    Returns
    -------
    df_concatenated, df_sheet_files_info : `DataFrame`, `DataFrame`
        A tuple containing the validated `DataFrames` concatenated or not, depending on the `concat_vertically` variable and the `DataFrame` information.
    """
    dataframes = []
    df_sheet_files_info = pd.DataFrame()
    for j, df in enumerate(dfs_):
        info, df = check_values(
            df,
            df_name=dfs_names[j],
            mode=mode,
            drop_empty_cols=drop_empty_cols,
        )
        dataframes.append(df)
        logger.info(f"DataFrame '{dfs_names[j]}' has been added to the list of dataframes")
        df_sheet_files_info = pd.concat([df_sheet_files_info, info])
    df_sheet_files_info.to_html(report_name, index=False, encoding=encoding)
    logger.info(f"A report has been created under the name '{report_name}'")

    if concat_vertically:
        dataframes = intersection_cols(dataframes)
        df_concatenated = pd.concat(dataframes, axis=0)
    else:
        df_concatenated = dataframes

    df_sheet_files_info["data type"] = [
        str(_type) for _type in df_sheet_files_info["data type"].to_list()
    ]
    df_sheet_files_info["sql name"] = [
        col_name.replace(" ", "_") for col_name in df_sheet_files_info["column name"]
    ]

    create_yaml_tree(yaml_name, df_sheet_files_info)

    return df_concatenated, df_sheet_files_info


def clearConsole():
    command = "clear"
    if os.name in ("nt", "dos"):
        command = "cls"
    os.system(command)


########################################################################################

if __name__ == "__main__":
    today = datetime.date.today()
    date = str(today)
    print("Today date is: ", today)

    # directory_path = os.getcwd()
    # directory_path = input("Enter directory path of the database: ")

    directory_path = sys.argv[1]
    if len(sys.argv) > 2:
        for i in range(2, len(sys.argv)):
            directory_path += " " + sys.argv[i]
    directory_path.replace("\\", "/")
    print("You have selected the path :", directory_path)

    extension = ".xlsx"

    print("Searching files...")
    dirs = glob.glob(directory_path + "/**/*" + extension, recursive=True)
    print("Found files :", len(dirs))
    df_sheet_files_info = pd.DataFrame()

    if not os.path.exists("Detail of the report"):
        print("Directory created : ", "Detail of the report")
        os.mkdir("Detail of the report")

    j = 0
    docs = []
    docs.append(["rpt_name", "name_xls", "sheet_name"])
    with pd.ExcelWriter("./Detail of the report/mult_sheets_database.xlsx") as writer:
        for i in dirs:
            xls = pd.ExcelFile(i)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, index_col=None)
                name_xls = i
                name_xls = name_xls.replace(directory_path, "")
                name_xls = name_xls.replace("\\", "")
                name_xls = name_xls.replace(extension, "")
                print("Reading file : ", name_xls, "sheet :", sheet_name)
                df = df.T.drop_duplicates().T
                info, df = check_values(df, name_xls)
                df.to_excel(writer, sheet_name="rpt_" + str(j), index=False)
                docs.append(["rpt_" + str(j), name_xls, sheet_name])
                clearConsole()
                clear_output(wait=True)
                df_sheet_files_info = pd.concat([df_sheet_files_info, info])
                j += 1

    print("Results on : ", "Detail of the report")

    with open("./Detail of the report/docs.txt", "w") as f:
        for line in docs:
            f.write(line[0] + "\t" + line[1] + "\t" + line[2] + "\n")
    df_sheet_files_info.to_html("report-health-checker.html", index=False, encoding="latin1")
    print("***process completed***")
