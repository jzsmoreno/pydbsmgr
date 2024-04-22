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
            if auxiliary_type is not None:
                x = str(pd.to_datetime(x, format=auxiliary_type, errors="ignore"))[:10]
            else:
                raise ValueError("Date value does not match the expected format.")
    else:
        if str(x).find(":") != -1:
            try:
                x = str(pd.to_datetime(x[:8], format=format_type, errors="raise"))[:10]
            except:
                if auxiliary_type is not None:
                    x = str(pd.to_datetime(x[:8], format=auxiliary_type, errors="ignore"))[:10]
                else:
                    raise ValueError("Date value does not match the expected format.")
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

        Parameters
        ----------
        - sample_frac (`float`): The fraction of rows to use for date type inference. Default is 0.1 i.e., 10%.
        - fast_execution (`bool`): If `False` use `applymap` pandas for extra text cleanup. Default is `True`.

        Keyword Arguments:
        ----------
        - no_emoji: (`bool`): By default it is set to `False`.
        If `True`, removes all emojis from text data. Works only when `fast_execution` = `False`.
        - title_mode: (`bool`): By default it is set to `True`.
        If `False`, converts the text to lowercase. Works only when `fast_execution` = `False`.
        By default, converts everything to `title`.
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
                        try:
                            format_type = auxiliary_type
                            partial_dates = partial(
                                process_dates,
                                format_type=format_type,
                                auxiliary_type=None,
                            )
                            vpartial_dates = np.vectorize(partial_dates)
                            table[cols[column_index]] = vpartial_dates(table[cols[column_index]])
                        except:
                            format_type = main_type
                            partial_dates = partial(
                                process_dates,
                                format_type=format_type,
                                auxiliary_type=None,
                            )
                            vpartial_dates = np.vectorize(partial_dates)
                            table[cols[column_index]] = vpartial_dates(table[cols[column_index]])
                    else:
                        format_type = main_type
                        partial_dates = partial(
                            process_dates,
                            format_type=format_type,
                            auxiliary_type=None,
                        )
                        vpartial_dates = np.vectorize(partial_dates)
                        table[cols[column_index]] = vpartial_dates(table[cols[column_index]])
                    vcoerce_datetime = np.vectorize(coerce_datetime)
                    table[cols[column_index]] = vcoerce_datetime(table[cols[column_index]])
                    table[cols[column_index]] = pd.to_datetime(
                        table[cols[column_index]], format="%Y%m%d", errors="coerce"
                    ).dt.normalize()
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
                        warning_type = "UserWarning"
                        msg = (
                            "It was not possible to perform the cleaning, the column {%s} is duplicated. "
                            % cols[column_index]
                        )
                        msg += "Error: {%s}" % e
                        print(f"{warning_type}: {msg}")
                        sys.exit("Perform correction manually")
                    if not fast_execution:
                        no_emoji = kwargs["no_emoji"] if "no_emoji" in kwargs else False
                        title_mode = kwargs["title_mode"] if "title_mode" in kwargs else True
                        partial_clean = partial(
                            clean,
                            no_emoji=no_emoji,
                            title_mode=title_mode,
                        )
                        vpartial_clean = np.vectorize(partial_clean)
                        table[cols[column_index]] = vpartial_clean(table[cols[column_index]])

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
