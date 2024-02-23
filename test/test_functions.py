import re
from typing import List, Tuple

import numpy as np
import pandas as pd
import pytest
from cleantext import clean
from pandas.core.frame import DataFrame
from pandas.core.indexes.base import Index
from pandas.core.series import Series


def test_clean_names(_clean_names):
    assert _clean_names("#tes$ting") == "tes_ting"


def test_clean_transform(_clean_transform):
    assert _clean_transform(["TesTing", "PyTest"]) == ["Testing", "Pytest"]
    assert _clean_transform(["1 TesTing", "(PyTest)"]) == ["Testing", "Pytest"]


def test_remove_char(_remove_char):
    assert _remove_char("(#Tes$ting)") == "Testing"


def test_check_if_isemail(_check_if_isemail):
    assert _check_if_isemail("githubuser@testing.com") == ("githubuser@testing.com", True)


def test_convert_date(_convert_date):
    assert _convert_date("31052023") == "2023-05-31"
    assert _convert_date("20230531") == "2023-05-31"


def test_clean_and_convert_to(_clean_and_convert_to):
    assert _clean_and_convert_to("20/02/2022 02:03:42") == "2022-02-20"
    assert _clean_and_convert_to("20-02-2022 02:03:42") == "2022-02-20"


def test_correct_nan(_correct_nan):
    assert _correct_nan("nan") == ""
    assert _correct_nan("Nan") == ""


def test_columns_dtypes(columns_dtypes_with_data):
    df = columns_dtypes_with_data.correct(sample_frac=0.33)
    data_types = df.dtypes
    assert data_types[1] == "datetime64[ns]"


def test_lightest(lightest_with_data):
    fecha, first_date, anther_date, third_date = lightest_with_data
    comparison = ["1974-09-10", "1973-01-06", "1975-01-18", "2020-08-25"]
    assert fecha == comparison
    assert first_date == comparison
    assert anther_date == comparison
    assert third_date == comparison


def test_get_extraction_date(_get_extraction_date):
    assert _get_extraction_date("filename_2023-11-17") == "2023-11-17"
    assert _get_extraction_date(["filename_2023-11-17"]) == ["2023-11-17"]
