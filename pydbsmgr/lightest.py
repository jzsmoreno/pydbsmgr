import logging
import sys
from functools import partial

import numpy as np
import pandas as pd

from pydbsmgr.main import check_if_contains_dates, clean, get_date_format
from pydbsmgr.utils.tools import most_repeated_item

logging.basicConfig(level=logging.WARNING)


def process_dates(
    x: str, format_type: str, auxiliary_type: str = None, errors: str = "ignore"
) -> str:
    """Auxiliary function in date type string processing."""
    x = str(x)
    if format_type in ["dayfirst", "monthfirst"] and len(x) < 10:
        separator = "/" if "/" in x else "-"
        parts = x.split(separator)
        if format_type == "dayfirst":
            day, month, year = parts[0], parts[1], parts[-1]
        elif format_type == "monthfirst":
            month, day, year = parts[0], parts[1], parts[-1]

        day = f"{int(day):02d}"
        month = f"{int(month):02d}"
        try:
            date = pd.to_datetime(f"{year}{month}{day}", format="%Y%m%d", errors="coerce")
        except ValueError:
            if auxiliary_type:
                date = pd.to_datetime(x, format=auxiliary_type, errors="coerce")
            elif errors == "raise":
                raise ValueError("Date value does not match the expected format.")
    else:
        x = x.replace("/", "").replace("-", "")

        if len(x) == 8:
            try:
                date = pd.to_datetime(x, format=format_type, errors="coerce")
            except ValueError:
                if auxiliary_type:
                    date = pd.to_datetime(x, format=auxiliary_type, errors="coerce")
                elif errors == "raise":
                    raise ValueError("Date value does not match the expected format.")
        else:
            try:
                date = pd.to_datetime(x[:8], format=format_type, errors="coerce")
            except ValueError:
                if auxiliary_type:
                    date = pd.to_datetime(x[:8], format=auxiliary_type, errors="coerce")
                elif errors == "raise":
                    raise ValueError("Date value does not match the expected format.")

    if not pd.isnull(date):
        return date.strftime("%Y-%m-%d")
    else:
        return x  # Return original string if no valid date is found


class LightCleaner:
    """Performs a light cleaning on the table."""

    __slots__ = ["df", "dict_dtypes"]

    def __init__(self, df_: pd.DataFrame):
        self.df = df_.copy()
        self.dict_dtypes = {"float": "float64", "int": "int64", "str": "object"}

    def clean_frame(
        self,
        sample_frac: float = 0.1,
        fast_execution: bool = True,
        two_date_formats: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """DataFrame cleaning main function

        Parameters
        ----------
        sample_frac : `float`
            The fraction of rows to use for date type inference. Default is 0.1 i.e., 10%.
        fast_execution : `bool`
            If `False` use `applymap` pandas for extra text cleanup. Default is `True`.

        Keyword Arguments:
        ----------
        no_emoji : `bool`
            By default it is set to `False`. If `True`, removes all emojis from text data. Works only when `fast_execution` = `False`.
        title_mode : `bool`
            By default it is set to `True`. If `False`, converts the text to lowercase. Works only when `fast_execution` = `False`. By default, converts everything to `title`.
        """
        table = self.df.copy()
        cols = table.columns
        if sample_frac != 1.0:
            table_sample = table.sample(frac=sample_frac, replace=False)
        else:
            table_sample = table.copy()
        errors = kwargs.get("errors", "ignore")

        for column_index, datatype in enumerate(table.dtypes):
            if datatype == "object":
                datetype_column = (
                    (table_sample[cols[column_index]].apply(check_if_contains_dates))
                    .isin([True])
                    .any()
                )
                if datetype_column:
                    main_type, auxiliary_type = most_repeated_item(
                        list(
                            filter(
                                lambda item: item is not None,
                                table_sample[cols[column_index]].apply(get_date_format),
                            )
                        ),
                        two_date_formats,
                    )

                    format_type = auxiliary_type or main_type
                    try:
                        partial_dates = partial(
                            process_dates,
                            format_type=format_type,
                            auxiliary_type=None,
                            errors=errors,
                        )
                        vpartial_dates = np.vectorize(partial_dates)

                        table[cols[column_index]] = pd.to_datetime(
                            vpartial_dates(table[cols[column_index]]),
                            format="%Y-%m-%d",
                            errors="coerce",
                        ).normalize()
                    except:
                        partial_dates = partial(
                            process_dates, format_type=main_type, auxiliary_type=None, errors=errors
                        )
                        vpartial_dates = np.vectorize(partial_dates)

                        table[cols[column_index]] = pd.to_datetime(
                            vpartial_dates(table[cols[column_index]]),
                            format="%Y-%m-%d",
                            errors="coerce",
                        ).normalize()
                else:
                    try:
                        table[cols[column_index]] = (
                            table[cols[column_index]]
                            .replace(np.nan, "")
                            .astype(str)
                            .str.normalize("NFKD")
                            .str.encode("ascii", errors="ignore")
                            .str.decode("ascii")
                            .str.title()
                        )
                    except AttributeError as e:
                        msg = f"It was not possible to perform the cleaning, the column {cols[column_index]} is duplicated. Error: {e}"
                        logging.warning(msg)
                        sys.exit("Perform correction manually")

                    if not fast_execution:
                        no_emoji = kwargs.get("no_emoji", False)
                        title_mode = kwargs.get("title_mode", True)
                        partial_clean = partial(clean, no_emoji=no_emoji, title_mode=title_mode)
                        vpartial_clean = np.vectorize(partial_clean)
                        table[cols[column_index]] = vpartial_clean(table[cols[column_index]])

        table = self._remove_duplicate_columns(table)
        self.df = table.copy()
        return self.df

    def _correct_type(self, value, datatype):
        """General type correction function."""
        val_type = type(value).__name__
        if self.dict_dtypes[val_type] != datatype:
            try:
                return {"float": float, "int": int, "str": str}[datatype](value)
            except ValueError:
                return np.nan if datatype in ["float", "int"] else ""
        return value

    def _remove_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate columns based on column name."""
        seen = set()
        unique_cols = [col for col in df.columns if not (col in seen or seen.add(col))]
        return df[unique_cols]


if __name__ == "__main__":
    # Example usage
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
    breakpoint()
