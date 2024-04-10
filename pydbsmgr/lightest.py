import concurrent.futures
from functools import partial

from pydbsmgr.main import *
from pydbsmgr.utils.tools import coerce_datetime, most_repeated_item


def process_dates(x: str, format_type: str, auxiliary_type: str) -> str:
    """Auxiliary function in date type string processing

    Parameters
    ----------
        x : `str`
            character of type date.

    Returns
    ----------
        x : `str`
            character after processing with format `YYYY-MM-DD`.
    """
    # performing data type conversion
    x = str(x)
    if format_type in ["dayfirst", "monthfirst"] and len(x) < 10:
        # split by "/" or "-"
        if format_type == "dayfirst":
            dmy = x.split("/") if "/" in x else x.split("-")
            day = dmy[0] if len(dmy[0]) == 2 else "0" + dmy[0]
            month = dmy[1] if len(dmy[1]) == 2 else "0" + dmy[1]
            year = dmy[-1]
        elif format_type == "monthfirst":
            mdy = x.split("/") if "/" in x else x.split("-")
            month = mdy[0] if len(mdy[0]) == 2 else "0" + mdy[0]
            day = mdy[1] if len(mdy[1]) == 2 else "0" + mdy[1]
            year = mdy[-1]

        return str(pd.to_datetime(f"{year}{month}{day}", format="%Y%m%d", errors="raise"))[:10]

    x = x.replace("/", "")
    x = x.replace("-", "")

    if len(x) == 8:
        try:
            x = str(pd.to_datetime(x, format=format_type, errors="raise"))[:10]
        except:
            if auxiliary_type != None:
                x = str(pd.to_datetime(x, format=auxiliary_type, errors="ignore"))[:10]
    else:
        if str(x).find(":") != -1:
            try:
                x = str(pd.to_datetime(x[:8], format=format_type, errors="raise"))[:10]
            except:
                if auxiliary_type != None:
                    x = str(pd.to_datetime(x[:8], format=auxiliary_type, errors="ignore"))[:10]
    return x


class LightCleaner:
    """Performs a light cleaning on the table"""

    # Increase memory efficiency
    __slots__ = ["df", "dict_dtypes"]

    def __init__(self, df_: DataFrame):
        self.df = df_.copy()
        self.dict_dtypes = dict(zip(["float", "int", "str"], ["float64", "int64", "object"]))

    def clean_frame(
        self,
        sample_frac: float = 0.1,
        fast_execution: bool = True,
        two_date_formats: bool = True,
        **kwargs,
    ) -> DataFrame:
        """`DataFrame` cleaning main function

        Keyword Arguments:
        ----------
        - fix_unicode: (`bool`): By default it is set to `True`.
        - to_ascii: (`bool`): By default it is set to `True`.
        - lower: (`bool`): By default it is set to `True`.
        - normalize_whitespace: (`bool`): By default it is set to `True`.
        - no_line_breaks: (`bool`): By default it is set to `False`.
        - strip_lines: (`bool`): By default it is set to `True`.
        - keep_two_line_breaks: (`bool`): By default it is set to `False`.
        - no_urls: (`bool`): By default it is set to `False`.
        - no_emails: (`bool`): By default it is set to `False`.
        - no_phone_numbers: (`bool`): By default it is set to `False`.
        - no_numbers: (`bool`): By default it is set to `False`.
        - no_digits: (`bool`): By default it is set to `False`.
        - no_currency_symbols: (`bool`): By default it is set to `False`.
        - no_punct: (`bool`): By default it is set to `False`.
        - no_emoji: (`bool`): By default it is set to `False`.
        - replace_with_url: (`str`): For example, the following `<URL>`.
        - replace_with_email: (`str`): For example, the following `<EMAIL>`.
        - replace_with_phone_number: (`str`): For example, the following `<PHONE>`.
        - replace_with_number: (`str`): For example, the following `<NUMBER>`.
        - replace_with_digit: (`str`): For example, the following `0`.
        - replace_with_currency_symbol: (`str`): For example, the following `<CUR>`.
        - replace_with_punct: (`str`): = For example, the following `""`.
        - lang: (`str`): = By default it is set to `en`.
        """
        table = (self.df).copy()
        cols = table.columns
        table_sample = table.sample(frac=sample_frac)
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
                    if auxiliary_type != None:
                        format_type = auxiliary_type
                    else:
                        format_type = main_type
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        partial_dates = partial(
                            process_dates,
                            format_type=format_type,
                            auxiliary_type=None,
                        )
                        table[cols[column_index]] = list(
                            executor.map(partial_dates, table[cols[column_index]])
                        )
                        table[cols[column_index]] = list(
                            executor.map(coerce_datetime, table[cols[column_index]])
                        )
                    table[cols[column_index]] = pd.to_datetime(
                        table[cols[column_index]], format="%Y%m%d", errors="coerce"
                    ).dt.normalize()
                else:
                    if fast_execution == False:
                        partial_clean = partial(clean, **kwargs)
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            table[cols[column_index]] = list(
                                executor.map(partial_clean, table[cols[column_index]])
                            )
                            table[cols[column_index]] = list(
                                executor.map(remove_char, table[cols[column_index]])
                            )
                            try:
                                table[cols[column_index]] = list(
                                    executor.map(
                                        lambda text: text.title() if text is not None else text,
                                        table[cols[column_index]],
                                    )
                                )
                            except AttributeError as e:
                                warning_type = "UserWarning"
                                msg = (
                                    "It was not possible to perform the cleaning, the column {%s} is duplicated. "
                                    % cols[column_index]
                                )
                                msg += "Error: {%s}" % e
                                print(f"{warning_type}: {msg}")
                                sys.exit("Perform correction manually")

        table = self._remove_duplicate_columns(table)
        self.df = table.copy()
        return self.df

    def _correct_float(self, value, datatype):
        """float correction function"""
        val_type = type(value).__name__
        if self.dict_dtypes[val_type] != datatype:
            try:
                return float(value)
            except:
                return np.nan
        else:
            return value

    def _correct_int(self, value, datatype):
        """integer correction function"""
        val_type = type(value).__name__
        if self.dict_dtypes[val_type] != datatype:
            try:
                return int(value)
            except:
                return np.nan
        else:
            return value

    def _correct_str(self, value, datatype):
        """character correction function"""
        val_type = type(value).__name__
        if self.dict_dtypes[val_type] != datatype:
            try:
                return str(value)
            except:
                return ""
        else:
            return value

    def _remove_duplicate_columns(self, df: DataFrame) -> DataFrame:
        """Function that removes duplicate columns based on column name"""
        # Drop duplicate columns
        # df = df.T.drop_duplicates().T
        # df = df.loc[:,~df.columns.duplicated()]
        seen_columns = set()
        unique_columns = []

        for col in df.columns:
            if col not in seen_columns:
                unique_columns.append(col)
                seen_columns.add(col)

        return df[unique_columns]
