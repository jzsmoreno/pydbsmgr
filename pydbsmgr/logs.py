import os
from datetime import datetime
from typing import List

import pandas as pd


class EventLogger:
    """Allows you to store screen prints in a plain text file."""

    def __init__(self, file_name: str, file_path: str) -> None:
        self.file_name = f"{file_name}.txt"
        self.file_path = file_path
        self._check_file_existence()

    def _check_file_existence(self):
        if os.path.isfile(self.full_path):
            warning_type = "UserWarning"
            msg = f"The log file {self.file_name} already exists, the changes will be added."
            print(f"{warning_type}: {msg}")

    @property
    def full_path(self) -> str:
        return os.path.join(self.file_path, self.file_name)

    def audit(self):
        if not os.path.isfile(self.full_path):
            warning_type = "UserWarning"
            msg = f"The log file {self.file_name} does not exist."
            print(f"{warning_type}: {msg}")
            return

        delete = (
            input(f"Do you want to delete the log file {self.file_name}? (y/N): ").strip().lower()
        )
        if delete == "y":
            os.remove(self.full_path)
            warning_type = "UserWarning"
            msg = f"The log file {self.file_name} has been deleted successfully."
            print(f"{warning_type}: {msg}")
        else:
            msg = f"Reading the log file: {self.file_name}"
            print(msg)
            with open(self.full_path, "r") as file:
                return file.read()

    def writer(self, *chars: str) -> None:
        date = datetime.now().strftime("%d/%m/%Y %H:%M:%S ")
        output_line = date + " ".join(chars) + "\n"

        print(output_line, end="")
        with open(self.full_path, "a") as file:
            file.write(output_line)


class EventLogBook:
    """Allows you to create and write new lines in a logbook."""

    def __init__(self, file_name: str, file_path: str) -> None:
        self.file_name = f"{file_name}.csv"
        self.file_path = file_path
        self._check_file_existence()

    @property
    def full_path(self) -> str:
        return os.path.join(self.file_path, self.file_name)

    def _check_file_existence(self):
        if os.path.isfile(self.full_path):
            warning_type = "UserWarning"
            msg = f"The logbook file {self.file_name} already exists, the changes will be added."
            print(f"{warning_type}: {msg}")

    def create(self, df: pd.DataFrame, encoding: str = "latin1") -> None:
        self._encoding = encoding
        df.to_csv(self.full_path, index=False, encoding=self._encoding)

    def audit(self, encoding: str = "latin1"):
        if not os.path.isfile(self.full_path):
            warning_type = "UserWarning"
            msg = f"The logbook file {self.file_name} does not exist."
            print(f"{warning_type}: {msg}")
            return

        delete = (
            input(f"Do you want to delete the logbook file {self.file_name}? (y/N): ")
            .strip()
            .lower()
        )
        if delete == "y":
            os.remove(self.full_path)
            warning_type = "UserWarning"
            msg = f"The logbook file {self.file_name} has been deleted successfully."
            print(f"{warning_type}: {msg}")
        else:
            return pd.read_csv(self.full_path, encoding=self._encoding)

    def update(self, rows: List[List[str]]) -> None:
        try:
            df = pd.read_csv(self.full_path, encoding=self._encoding)
            new_df = pd.DataFrame(rows, columns=df.columns)
            df = pd.concat([df, new_df], ignore_index=True)
            df.to_csv(self.full_path, index=False, encoding=self._encoding)

        except FileNotFoundError:
            warning_type = "UserWarning"
            msg = (
                f"The logbook {self.file_name} has not been created, so it is going to be created."
            )
            print(f"{warning_type}: {msg}")
            new_df = pd.DataFrame(rows)
            new_df.to_csv(self.full_path, index=False, encoding=self._encoding)


# Usage example
if __name__ == "__main__":
    logger = EventLogger("example_log", "./")
    logger.writer("This is a test log entry.")

    logbook = EventLogBook("example_logbook", "./")
    logbook.create(pd.DataFrame({"Column1": [1, 2], "Column2": ["A", "B"]}))
    logbook.update([[3, "C"]])
