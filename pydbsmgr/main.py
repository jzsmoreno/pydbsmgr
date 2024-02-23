import datetime
import glob
import os
import re
import sys
import warnings
from typing import List, Tuple

import numpy as np
import pandas as pd
from cleantext import clean
from IPython.display import clear_output
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
        r"([1-9]|[12][0-9]|3[01])(-|/)([1-9]|1[0-2])(-|/)\d{4}",
        r"([1-9]|1[0-2])(-|/)([1-9]|[12][0-9]|3[01])(-|/)\d{4}",
    ]
    formats = ["%Y%m%d", "%Y%d%m", "%d%m%Y", "%m%d%Y", "dayfirst", "monthfirst"]
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
    dirty_string : `str`
        string of characters
    pattern : `str`
        regular expression string

    Returns
    -------
    result : `str`
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
    col : `str`
        The column name to be transformed.
    mode : `bool`
        Indicates if names will be capitalized. By default it is set to `True`.
    remove_numeric : `bool`
        Indicates if numeric characters will be removed. By default it is set to `True`.
    remove_spaces : `bool`
        Indicates if spaces will be removed. By default it is set to `True`.
    Returns
    -------
    col_name : `str`
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
    col_index : `Index`
        The column index to be transformed.
    mode : bool
        Indicates if names will be capitalized. By default it is set to `True`.

    Returns
    -------
    col_name_list : `str`
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
    input_string : `str`
        The input string from which characters will be removed.

    Returns
    -------
    input_string : `str`
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
    check_email : `str`
        The input string to be checked for an email address.

    Returns
    -------
    check_email, found_email : `str`, `bool`
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
    date_string : `str`
        The input string representing a date.

    Returns
    -------
    proper_date : `str`
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
    index_min = df_dict[min_col]
    cols_ = set(dfs_[index_min].columns)
    for i, df in enumerate(dfs_):
        dfs_[i] = (dfs_[i][list(cols_)]).copy()

    return dfs_


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
                df.to_excel(writer, sheet_name="rpt_" + str(j), index=False)
                docs.append(["rpt_" + str(j), name_xls, sheet_name])
                clearConsole()
                clear_output(wait=True)
