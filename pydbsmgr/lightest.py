import concurrent.futures

from pydbsmgr.main import *


class LightCleaner:
    """Performs a light cleaning on the table"""

    def __init__(self, df_: DataFrame):
        self.df = df_.copy()
        self.dict_dtypes = dict(zip(["float", "int", "str"], ["float64", "int64", "object"]))

    def clean_frame(self, sample_frac: float = 0.1) -> DataFrame:
        """`DataFrame` cleaning main function"""
        table = (self.df).copy()
        cols = table.columns
        table_sample = table.sample(frac=sample_frac)
        for column_index, datatype in enumerate(table.dtypes):
            if datatype == "object" or datatype == "datetime64[ns]":
                x = (table_sample[cols[column_index]].values)[0]
                datetype_column = True
                if isinstance(x, str):
                    if (
                        x == ""
                        or x.find("/") != -1
                        or x.find("-") != -1
                        or x == np.datetime64("NaT")
                    ):
                        datetype_column = (
                            (table_sample[cols[column_index]].apply(check_if_contains_dates))
                            .isin([True])
                            .any()
                        )
                    if not (x.find("//") or x.find("\\")) != -1 and datetype_column:
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            table[cols[column_index]] = list(
                                executor.map(clean_and_convert_to, table[cols[column_index]])
                            )
                    else:
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
