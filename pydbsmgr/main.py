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
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pandas.core.series import Series

warnings.filterwarnings("ignore")

########################################################################################


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


def clean_transform(col_index: Index, mode: bool = True) -> List[str]:
    """
    Transforms a column index by cleaning the column names and if needed makes them capital.

    Parameters
    ----------
        col_index : Index
            The column index to be transformed.
        mode : bool = True
            Indicates if names will be capitalized (True as default).

    Returns
    ----------
        List : str
            The transformed column names as a list of strings.
    """
    col_name_list = []
    for col in col_index:
        if mode:
            col_name_list.append(str(clean(col)).title())
        else:
            col_name_list.append(clean(col))
    return col_name_list


def remove_char(input_string: str) -> str:
    """
    Removes special characters from a string.

    Parameters
    ----------
        input_string : str
            The input string from which characters will be removed.

    Returns
    ----------
        str : The string with specified characters removed.
    """
    list_of_char = ["#", "$", "*", "?", "!"]
    for char in list_of_char:
        try:
            input_string = input_string.replace(char, "")
        except:
            return input_string
    return input_string


def check_if_isemail(check_email: str) -> str:
    """
    Checks if a string is an email address and returns the cleaned string and a flag indicating if the string is an email.

    Parameters
    ----------
        check_email : str
            The input string to be checked for an email address.

    Returns
    ----------
        Tuple : str, bool
            A tuple containing the cleaned string and a boolean flag indicating if an email address was found.
    """
    found_email = False
    if str(check_email).find("@") != -1:
        check_email = str(clean(check_email))
        found_email = True

    return check_email, found_email


def convert_date(date_string: str) -> str:
    """
    Converts a string of a date to a proper date format.

    Parameters
    ----------
    date_string : str
        The input string representing a date.

    Returns
    -------
    proper_date : str
        The date string in the proper format 'YYYY-MM-DD'.
    """
    try:
        proper_date = str(pd.to_datetime(date_string, format="%Y%m%d", errors="raise"))[:10]
    except:
        try:
            proper_date = str(pd.to_datetime(date_string, format="%d%m%Y", errors="raise"))[:10]
        except:
            proper_date = str(pd.to_datetime(date_string, format="%Y%m%d", errors="ignore"))[:10]
    return proper_date


def clean_and_convert_to(x: str) -> str:
    """
    Performs cleaning and some conversions on a string.

    Parameters
    ----------
    x : str
        The input string to be cleaned and converted.

    Returns
    -------
    str
        The cleaned and converted string.
    """
    # pattern_to_year = r"\d{4}"
    x = remove_char(x)
    try:
        x, find_ = check_if_isemail(x)
        if (x.find("/") != -1 or x.find("-")) != -1 and not (x.find("//") or x.find("\\")) != -1:
            x = x.replace("/", "")
            x = x.replace("-", "")
            # result = re.findall(pattern_to_year, x)
            # year = result[0]
            if len(x) == 8:
                x = convert_date(x)
            elif str(x).find(":") != -1:
                x = convert_date(x[:8])
        else:
            if not find_:
                if str(x).find(".") != -1:
                    x_ = x.replace(".", "")
                    if len(x) == 8:
                        x = convert_date(x_)
                    else:
                        if x.find("//") == -1:
                            x_ = x.replace(".", " ")
                            x = clean(x_)
                else:
                    x = clean(x)
    except:
        None
    return x


def correct_nan(check_missing: str) -> str:
    """
    Corrects the format of missing values in a string to the correct numpy.nan.

    Parameters
    ----------
    check_missing : str
        The string to be checked for incorrect missing value format.

    Returns
    -------
    str
        The corrected string format.
    """
    if str(check_missing).find("nan") != -1:
        return np.nan
    else:
        return check_missing


def check_dtypes(dataframe: DataFrame, datatypes: Series) -> DataFrame:
    """
    Checks and updates the data types of columns in a DataFrame.

    Parameters
    ----------
    dataframe : DataFrame
        The DataFrame to check and update the data types.
    datatypes : Series
        The Series containing the desired data types for each column in the DataFrame.

    Returns
    -------
    DataFrame
        The DataFrame with updated data types.
    """
    cols = dataframe.columns
    column_index = 0
    for datatype in datatypes:
        if datatype == "object":
            dataframe[cols[column_index]] = dataframe[cols[column_index]].apply(clean_and_convert_to)
            dataframe[cols[column_index]] = dataframe[cols[column_index]].apply(correct_nan)
            try:
                dataframe[cols[column_index]] = dataframe[cols[column_index]].map(str.strip)
            except:
                try:
                    dataframe[cols[column_index]] = dataframe[cols[column_index]].astype("datetime64[ns]")
                except:
                    None
        column_index += 1
    return dataframe


def create_yaml_tree(yaml_name: str, df_info: DataFrame) -> None:
    """
    Function that creates a yaml configuration file for database data type validation.

    Parameters
    ----------
    yaml_name : str
        The name of the yaml configuration file to be created.
    df_info : DataFrame
        The DataFrame with the column information for data type validation.

    Returns
    -------
    Yaml file with the name given.
    """
    data = {}
    for col_name, data_type, sql_name in zip(
        df_info["column name"].to_list(),
        df_info["data type"],
        df_info["sql name"].to_list(),
    ):
        data[col_name] = {"type": [data_type], "sql_name": [sql_name]}

    yaml_data = yaml.dump({"database": data})

    with open(yaml_name, "w") as file:
        file.write(yaml_data)


def check_values(
    df_: DataFrame, df_name: str, sheet_name: str, mode: bool = True, cols_upper_case: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Perform data validation on a DataFrame and create a Data Quality Assesment Report on the DataFrame.

    Parameters
    ----------
    df_ : DataFrame
        The DataFrame to be validated.
    df_name : str
        The name of the DataFrame.
    sheet_name : str
        The name of the excel sheet within the DataFrame.
    mode : bool, optional
        Indicates whether to generate a visualization and report in html, True by default.
    cols_upper_case : bool, optional
        Indicates whether to convert column names to uppercase, False by default.

    Returns
    -------
    info, df : DataFrame, DataFrame
        A tuple containing the information DataFrame and the validated DataFrame.
    """
    df = df_.copy()
    df.columns = clean_transform(df.columns, cols_upper_case)
    df = check_dtypes(df, df.dtypes)
    df = df.replace("Nan", np.nan)
    df = df.loc[:, ~df.columns.str.contains("^unnamed")]
    # print(f"Total size of {df_name}: ", "{:,}".format(len(df)))
    info = []
    title = "Report " + df_name + "_" + sheet_name
    if mode:
        # profile = ProfileReport(df, title=title)
        # profile.to_file("./Detail of the report/" + title + ".html")
        ax = msno.matrix(df)
        ax.get_figure().savefig("./Detail of the report/" + title + ".png", dpi=300)
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
                        sheet_name,
                        nrows,
                        nrows_missing,
                        percentage,
                        unique_vals,
                    ]
                )
                # print(str(col)+' : 'f"{(percentage)*100:.2f}%")
        except:
            None
    info = np.array(info).reshape((-1, 8))
    info = pd.DataFrame(
        info,
        columns=[
            "column name",
            "data type",
            "database name",
            "sheet name",
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
    sheet_names: List[str],
    mode: bool = True,
    yaml_name: str = "./output.yaml",
    report_name: str = "./report-health-checker.html",
    encoding: str = "latin1",
    concat_vertically: bool = False,
) -> Tuple[DataFrame, DataFrame]:
    """Function that performs the implementation of the check_values function on lists of dataframes."""
    dataframes = []
    df_sheet_files_info = pd.DataFrame()
    for j, df in enumerate(dfs_):
        info, df = check_values(df, df_name=dfs_names[j], sheet_name=sheet_names[j], mode=mode)
        dataframes.append(df)
        df_sheet_files_info = pd.concat([df_sheet_files_info, info])
    df_sheet_files_info.to_html(report_name, index=False, encoding=encoding)

    if concat_vertically:
        df_concatenated = pd.concat(dataframes, axis=0)
    else:
        df_concatenated = dfs_

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
                info, df = check_values(df, name_xls, sheet_name)
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
