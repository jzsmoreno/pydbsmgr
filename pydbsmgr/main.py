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


def clean_names(x: str, pattern: str = r"[a-zA-Zñáéíóú_]+\b") -> str:
    """
    Receive a string and clean it of special characters

    Parameters
    ----------
    x : str
        string of characters
    pattern : str
        regular expression string

    Returns
    -------
    result : str
        clean character string
    """
    result = re.findall(pattern, str(x).replace("_", ""))
    if len(result) > 0:
        result = "_".join(result)
    else:
        pattern = r"[a-zA-Z]+"
        result = re.findall(pattern, str(x).replace("_", ""))
        result = "_".join(result)
    return result


def clean_transform(x: Index, mode: bool = True) -> List[str]:
    y = []
    for i in x:
        if mode:
            y.append(str(clean(i)).title())
        else:
            y.append(clean(i))
    return y


def remove_char(x: str) -> str:
    list_of_char = ["#", "$", "*", "?", "!"]
    for i in list_of_char:
        try:
            x = x.replace(i, "")
        except:
            return x
    return x


def check_if_isemail(x: str) -> str:
    find_ = False
    if str(x).find("@") != -1:
        x = str(clean(x))
        find_ = True

    return x, find_


def convert_date(x: str) -> str:
    try:
        x = str(pd.to_datetime(x, format="%Y%m%d", errors="raise"))[:10]
    except:
        try:
            x = str(pd.to_datetime(x, format="%d%m%Y", errors="raise"))[:10]
        except:
            x = str(pd.to_datetime(x, format="%Y%m%d", errors="ignore"))[:10]
    return x


def clean_and_convert_to(x: str) -> str:
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


def correct_nan(x: str) -> str:
    if str(x).find("nan") != -1:
        return np.nan
    else:
        return x


def check_dtypes(df: DataFrame, datatypes: Series) -> DataFrame:
    cols = df.columns
    j = 0
    for i in datatypes:
        if i == "object":
            df[cols[j]] = df[cols[j]].apply(clean_and_convert_to)
            df[cols[j]] = df[cols[j]].apply(correct_nan)
            try:
                df[cols[j]] = df[cols[j]].map(str.strip)
            except:
                try:
                    df[cols[j]] = df[cols[j]].astype("datetime64[ns]")
                except:
                    None
        j += 1
    return df


def create_yaml_tree(yaml_name: str, df_info: DataFrame) -> None:
    """Function that creates a yaml configuration file for database data type validation."""
    data = {}
    for name, t, sql_name in zip(
        df_info["column name"].to_list(),
        df_info["data type"],
        df_info["sql name"].to_list(),
    ):
        data[name] = {"type": [t], "sql_name": [sql_name]}

    yaml_data = yaml.dump({"database": data})

    with open(yaml_name, "w") as file:
        file.write(yaml_data)


def check_values(
    df_: DataFrame, df_name: str, sheet_name: str, mode: bool = True, cols_upper_case: bool = False
) -> Tuple[DataFrame, DataFrame]:
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
    for i in df.columns:
        try:
            if i.find("unnamed") == -1:
                nrows = df.isnull().sum()[i] + df[i].count()
                nrows_missing = df.isnull().sum()[i]
                percentage = nrows_missing / nrows
                datatype = df.dtypes[i]
                unique_vals = len(df[i].unique())
                info.append(
                    [
                        i,
                        datatype,
                        df_name,
                        sheet_name,
                        nrows,
                        nrows_missing,
                        percentage,
                        unique_vals,
                    ]
                )
                # print(str(i)+' : 'f"{(percentage)*100:.2f}%")
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
    cols_upper_case: bool = False,
    quality_report: bool = True,
    yaml_dtype: bool = True,
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
