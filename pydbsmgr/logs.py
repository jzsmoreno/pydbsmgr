import os
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame


class EventLogger:
    """Allows you to store screen prints in a plain text file."""

    def __init__(self, FILE_NAME: str, FILE_PATH: str) -> None:
        self.FILE_NAME = FILE_NAME + ".txt"
        self.FILE_PATH = FILE_PATH
        if os.path.isfile(self.FILE_PATH + self.FILE_NAME):
            warning_type = "UserWarning"
            msg = "The log file {%s} already exists, the changes will be added." % self.FILE_NAME
            print(f"{warning_type}: {msg}")
        self.FILE = open(self.FILE_PATH + self.FILE_NAME, "a")

    def audit(self):
        if os.path.isfile(self.FILE_PATH + self.FILE_NAME):
            msg = (
                "The log file {%s} already exists, do you want to delete it? (True/False): "
                % self.FILE_NAME
            )
            delete = input(msg)
            if delete == "True":
                os.remove(self.FILE_PATH + self.FILE_NAME)
                warning_type = "UserWarning"
                msg = "The log file {%s} has been deleted successfully" % self.FILE_NAME
                print(f"{warning_type}: {msg}")
            else:
                msg = "Reading the log file: {%s}" % self.FILE_NAME
                print(f"{msg}")
                with open(self.FILE_PATH + self.FILE_NAME, "r") as file:
                    log = file.read()
                return log
        else:
            warning_type = "UserWarning"
            msg = "The log file {%s} does not exist" % self.FILE_NAME
            print(f"{warning_type}: {msg}")

    def writer(self, *chars: str) -> None:
        self.FILE = open(self.FILE_PATH + self.FILE_NAME, "a")
        date = str(datetime.now().strftime("%d/%m/%Y %H:%M:%S "))
        print(date, end="")
        for item in chars:
            print(item, end="")
        print("\n")
        print(date, end="", file=self.FILE)
        for item in chars:
            print(item, end="", file=self.FILE)
        print("\n", file=self.FILE)
        self.FILE.close()


class EventLogBook:
    """Allows you to create and write new lines in a logbook."""

    def __init__(self, FILE_NAME: str, FILE_PATH: str) -> None:
        self.FILE_NAME = FILE_NAME + ".csv"
        self.FILE_PATH = FILE_PATH
        if os.path.isfile(self.FILE_PATH + self.FILE_NAME):
            warning_type = "UserWarning"
            msg = (
                "The logbook file {%s} already exists, the changes will be added." % self.FILE_NAME
            )
            print(f"{warning_type}: {msg}")

    def create(self, df: DataFrame, encoding: str = "latin1") -> None:
        self._encoding = encoding
        df.to_csv(self.FILE_PATH + self.FILE_NAME, index=False, encoding=self._encoding)

    def audit(self, encoding: str = "latin1"):
        self._encoding = encoding
        if os.path.isfile(self.FILE_PATH + self.FILE_NAME):
            msg = (
                "The log file {%s} already exists, do you want to delete it? (True/False): "
                % self.FILE_NAME
            )
            delete = input(msg)
            if delete == "True":
                os.remove(self.FILE_PATH + self.FILE_NAME)
                warning_type = "UserWarning"
                msg = "The log file {%s} has been deleted successfully" % self.FILE_NAME
                print(f"{warning_type}: {msg}")
            else:
                msg = "Reading the log file: {%s}" % self.FILE_NAME
                print(f"{msg}")
                return pd.read_csv(self.FILE_PATH + self.FILE_NAME, encoding=self._encoding)
        else:
            warning_type = "UserWarning"
            msg = "The log file {%s} does not exist" % self.FILE_NAME
            print(f"{warning_type}: {msg}")

    def update(self, rows: List[str]) -> None:
        try:
            df = pd.read_csv(self.FILE_PATH + self.FILE_NAME, encoding=self._encoding)
            to_add = rows
            df_to_add = pd.DataFrame(np.array(to_add).reshape((1, -1)), columns=df.columns)
            df = pd.concat([df, df_to_add])
            df.to_csv(self.FILE_PATH + self.FILE_NAME, index=False, encoding=self._encoding)
        except:
            warning_type = "UserWarning"
            msg = (
                "The logbook {%s} has not been created, so it is going to be created"
                % self.FILE_NAME
            )
            print(f"{warning_type}: {msg}")
            to_add = rows
            df = pd.DataFrame(np.array(to_add).reshape((1, -1)), columns=df.columns)
            df.to_csv(to_add=(rows), index=False, encoding=self._encoding)
