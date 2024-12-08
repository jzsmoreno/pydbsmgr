from typing import Callable

import pandas as pd
import pytest

from pydbsmgr.lightest import LightCleaner
from pydbsmgr.main import *
from pydbsmgr.utils.tools import ColumnsCheck, ColumnsDtypes, get_extraction_date


@pytest.fixture()
def _get_extraction_date() -> Callable:
    return get_extraction_date


@pytest.fixture()
def _clean() -> Callable:
    return clean


@pytest.fixture()
def _clean_transform() -> Callable:
    return clean_transform


@pytest.fixture()
def _remove_char() -> Callable:
    return remove_char


@pytest.fixture()
def _check_if_isemail() -> Callable:
    return check_if_isemail


@pytest.fixture()
def _convert_date() -> Callable:
    return convert_date


@pytest.fixture()
def _clean_and_convert_to() -> Callable:
    return clean_and_convert_to


@pytest.fixture()
def _correct_nan() -> Callable:
    return correct_nan


@pytest.fixture()
def columns_dtypes_with_data() -> Callable:
    """Passes a test dataframe to the class"""
    df = pd.DataFrame(
        {"index": ["0", "1", "2"], "fecha": ["2021-03-03", "2021-03-18", "2021-03-25"]}
    )
    columns_dtypes = ColumnsDtypes(df)

    return columns_dtypes


@pytest.fixture()
def lightest_with_data() -> Callable:
    """Passes a test dataframe to the class"""
    df = pd.DataFrame(
        {
            "index": ["0", "1", "2", "3", "4"],
            "fecha": ["10/09/1974", "06/01/1973", "18/01/1975", "25/08/2020", " fecha_no_valida"],
            "first_date": [
                "09/10/1974",
                "01/06/1973",
                "01/18/1975",
                "08/25/2020",
                " fecha_no_valida",
            ],
            "another_date": ["9/10/1974", "1/6/1973", "1/18/1975", "8/25/2020", " fecha_no_valida"],
            "third_date": ["10/9/1974", "6/1/1973", "18/1/1975", "25/8/2020", " fecha_no_valida"],
        }
    )

    handler = LightCleaner(df)
    df = handler.clean_frame(sample_frac=1.0, fast_execution=False, errors="raise")

    return (
        df["fecha"].astype(str).to_list(),
        df["first_date"].astype(str).to_list(),
        df["another_date"].astype(str).to_list(),
        df["third_date"].astype(str).to_list(),
    )


@pytest.fixture()
def columns_check_with_data() -> Callable:
    """
    Fixture that returns an instance of the class ColumnsCheck with data for testing
    """
    df = pd.DataFrame(
        {
            "index": ["0", "1", "2", "3"],
            "Raw Data!": ["10", "06", "18", "25"],
            "First Data #": ["09", "01", "01", "08"],
            "First Data 2": ["9", "1", "1", "8"],
            "Another Data?": ["10", "6", "18", "25"],
        }
    )

    handler = ColumnsCheck(df)
    df = handler.get_frame(surrounding=False)

    return df.columns.to_list()
