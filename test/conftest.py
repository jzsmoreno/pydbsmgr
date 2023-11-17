from typing import Callable

import pandas as pd
import pytest

from pydbsmgr.main import *
from pydbsmgr.utils.tools import ColumnsDtypes, get_extraction_date


@pytest.fixture()
def _get_extraction_date() -> Callable:
    return get_extraction_date


@pytest.fixture()
def _clean_names() -> Callable:
    return clean_names


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
