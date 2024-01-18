import concurrent.futures
from functools import cached_property, partial

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
        self, sample_frac: float = 0.1, fast_execution: bool = True, two_date_formats: bool = True
    ) -> DataFrame:
        """`DataFrame` cleaning main function"""
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
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            table[cols[column_index]] = list(
                                executor.map(clean, table[cols[column_index]])
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
