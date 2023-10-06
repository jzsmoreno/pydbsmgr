import re
from typing import List, Tuple

import numpy as np
import pandas as pd
import pytest
from cleantext import clean
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pandas.core.series import Series


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


def clean_transform(
    col_index: Index, mode: bool = True, remove_spaces: bool = True, remove_numeric: bool = True
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
    col_name_list = []
    for i, col in enumerate(col_index):
        if mode:
            col_name_list.append(remove_char(str(clean(col)).title()))
        else:
            col_name_list.append(remove_char(clean(col)))
        if remove_numeric:
            col_name_list[i] = remove_numeric_char(col_name_list[i])
            col_name_list[i] = col_name_list[i].strip()
        if remove_spaces:
            col_name_list[i] = col_name_list[i].replace(" ", "_")
            col_name_list[i] = col_name_list[i].replace("-", "_")
            col_name_list[i] = col_name_list[i].replace("/", "_")
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
    input_string : str
        The string with specified characters removed.
    """
    list_of_char = ["#", "$", "*", "?", "!", "(", ")", "&", "%"]
    for char in list_of_char:
        try:
            input_string = input_string.replace(char, "")
        except:
            None
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
    x : str
        The input string to be cleaned and converted.

    Returns
    -------
    x : str
        The cleaned and converted string.
    """

    # Consider cases where a number is passed as a `str`
    if is_number_regex(str(x)):
        if str(x).find(".") != -1:
            try:
                return float(x)
            except:
                print(f"Could not convert to float, converted to `np.nan`.")
                return np.nan
        else:
            try:
                return int(x)
            except:
                print(f"Could not convert to `int`, converted to `np.nan`.")
                return np.nan
    else:
        # Consider cases in which a `float` number is passed as a `str` and is erroneous
        if str(x).find(".") != -1:
            try:
                return float(x)
            except:
                print(f"Could not convert {x} to float, converting to `str`...")
                x = str(x)
                print(f"Successfully converted {x} to `str`.")
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
                    print("No date found.")
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
    Corrects the format of missing values in a `str` to the correct `np.nan`.

    Parameters
    ----------
    check_missing : str
        The string to be checked for incorrect missing value format.

    Returns
    -------
    check_missing : str
        The corrected string format or `empty str`.
    """
    if str(check_missing).find("nan") != -1 or str(check_missing).find("Nan") != -1:
        if len(str(check_missing)) == 3:
            return ""
        else:
            return check_missing
    else:
        return check_missing


def test_clean_names():
    assert clean_names("#tes$ting") == "tes_ting"


def test_clean_transform():
    assert clean_transform(["TesTing", "PyTest"]) == ["Testing", "Pytest"]
    assert clean_transform(["1 TesTing", "(PyTest)"]) == ["Testing", "Pytest"]


def test_remove_char():
    assert remove_char("(#Tes$ting)") == "Testing"


def test_check_if_isemail():
    assert check_if_isemail("githubuser@testing.com") == ("githubuser@testing.com", True)


def test_convert_date():
    assert convert_date("31052023") == "2023-05-31"
    assert convert_date("20230531") == "2023-05-31"


def test_clean_and_convert_to():
    assert clean_and_convert_to("20/02/2022 02:03:42") == "2022-02-20"
    assert clean_and_convert_to("20-02-2022 02:03:42") == "2022-02-20"


def test_correct_nan():
    assert correct_nan("nan") == ""
    assert correct_nan("Nan") == ""
