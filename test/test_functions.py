import re
from typing import List, Tuple

import pandas as pd
import pytest
from cleantext import clean
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pandas.core.series import Series


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
                            x = clean(x_).title()
                else:
                    x = clean(x)
    except:
        None
    return x


def test_clean_names():
    assert clean_names("#tes$ting") == "tes_ting"


def test_clean_transform():
    assert clean_transform(["TesTing", "PyTest"]) == ["Testing", "Pytest"]


def test_remove_char():
    assert remove_char("#Tes$ting") == "Testing"


def test_check_if_isemail():
    assert check_if_isemail("githubuser@testing.com") == ("githubuser@testing.com", True)


def test_convert_date():
    assert convert_date("31052023") == "2023-05-31"
    assert convert_date("20230531") == "2023-05-31"


def test_clean_and_convert_to():
    assert clean_and_convert_to("20/02/2022 02:03:42") == "2022-02-20"
