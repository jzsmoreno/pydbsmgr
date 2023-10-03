from pydbsmgr.main import *


class LightCleaner:
    def __init__(self, df_: DataFrame):
        self.df = df_.copy()
        self.dict_dtypes = dict(zip(["float", "int", "str"], ["float64", "int64", "object"]))

    def clean_frame(self) -> DataFrame:
        """`DataFrame` cleaning main function"""
        table = (self.df).copy()
        cols = table.columns
        for column_index, datatype in enumerate(table.dtypes):
            if datatype == "object" or datatype == "datetime64[ns]":
                if ((cols[column_index]).lower()).find("fecha") != -1 or (
                    (cols[column_index]).lower()
                ).find("date") != -1:
                    table[cols[column_index]] = table[cols[column_index]].apply(
                        clean_and_convert_to
                    )
                else:
                    table[cols[column_index]] = table[cols[column_index]].apply(clean)
                    table[cols[column_index]] = table[cols[column_index]].apply(remove_char)
                    try:
                        table[cols[column_index]] = table[cols[column_index]].apply(
                            lambda text: text.title()
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

                    table[cols[column_index]] = table[cols[column_index]].apply(
                        self._correct_str, datatype=datatype
                    )
            else:
                if datatype == "float64":
                    table[cols[column_index]] = table[cols[column_index]].apply(
                        self._correct_float, datatype=datatype
                    )
                elif datatype == "int64":
                    table[cols[column_index]] = table[cols[column_index]].apply(
                        self._correct_int, datatype=datatype
                    )
        table = self._remove_duplicate_columns(table)
        # table = table.infer_objects()
        # table = table.convert_dtypes()
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
