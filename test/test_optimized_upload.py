from datetime import datetime
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from pydbsmgr.fast_upload import DataFrameToSQL, UploadToSQL

"""
Pre-test setup (required before running pytest):

1. Build the Docker image:
   docker build -t test .

2. Run the container in detached mode:
   docker run -d -p 1435:1433 test

Notes:
- Ensure Docker is installed and running.
- Port 1435 on the host maps to 1433 in the container.
- If the container is already running, you may need to stop/remove it first:
    docker ps
    docker stop <container_id>
    docker rm <container_id>

Optional automation:
You can wrap this in a Makefile or script to run before pytest.
"""


class TestDataFrameToSQL:

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "salary": [50000.0, 60000.0, 70000.0],
                "is_active": [True, False, True],
                "join_date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
            }
        )

    @pytest.fixture
    def mock_connection_string(self):
        return (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=localhost,1435;"
            "Database=master;"
            "UID=sa;"
            "PWD=vSk60DcYRU;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )

    # -------------------------
    # Preprocess tests
    # -------------------------
    def test_preprocess_dataframe_edge_cases(self, sample_df, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        df = sample_df.copy()
        df.loc[0, "name"] = ""
        df.loc[1, "name"] = "NULL"
        df.loc[2, "name"] = "nan"

        processed = uploader._preprocess_dataframe(df)

        assert processed["name"].isnull().all()

    def test_preprocess_inf_nan(self, sample_df, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        df = sample_df.copy()
        df.loc[0, "salary"] = np.nan
        df.loc[1, "salary"] = np.inf
        df.loc[2, "salary"] = -np.inf

        processed = uploader._preprocess_dataframe(df)

        assert pd.isna(processed["salary"].iloc[0])
        assert pd.isna(processed["salary"].iloc[1])
        assert pd.isna(processed["salary"].iloc[2])

    # -------------------------
    # Schema inference
    # -------------------------
    def test_infer_schema_types(self, sample_df, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        assert uploader._infer_schema("id", sample_df, 512, True) in ["INT", "BIGINT"]
        assert uploader._infer_schema("salary", sample_df, 512, True) in ["FLOAT", "INT", "BIGINT"]
        assert uploader._infer_schema("is_active", sample_df, 512, True) == "BIT"

        assert "VARCHAR" in uploader._infer_schema("name", sample_df, 512, True)
        assert "DATETIME2" in uploader._infer_schema("join_date", sample_df, 512, True)

    def test_infer_schema_all_nulls(self, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        df = pd.DataFrame({"col": [None, None, None]})
        schema = uploader._infer_schema("col", df, 512, True)

        assert isinstance(schema, str)

    # -------------------------
    # Identifier sanitization
    # -------------------------
    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("test_table", "test_table"),
            ("test'table", "testtable"),
            ("test;table", "testtable"),
            ("123table", "_123table"),
            ("my table", "mytable"),
        ],
    )
    def test_sanitize_identifier(self, mock_connection_string, input_name, expected):
        uploader = DataFrameToSQL(mock_connection_string)
        assert uploader._sanitize_identifier(input_name) == expected

    # -------------------------
    # Data preparation
    # -------------------------
    def test_prepare_data_types(self, sample_df, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        prepared = uploader._prepare_data_for_insertion(sample_df)

        assert len(prepared) == len(sample_df)

        # Check types
        assert isinstance(prepared[0][0], int)
        assert isinstance(prepared[0][3], float)
        assert isinstance(prepared[0][4], bool)
        assert isinstance(prepared[0][5], (datetime, type(None)))

    # -------------------------
    # Connection pooling
    # -------------------------
    @patch("pyodbc.connect")
    def test_connection_pool_reuse(self, mock_connect, mock_connection_string):
        mock_conn = Mock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn

        uploader = DataFrameToSQL(mock_connection_string)

        with uploader._get_connection():
            pass

        with uploader._get_connection():
            pass

        assert mock_connect.call_count == 1

    @patch("pyodbc.connect", side_effect=Exception("DB down"))
    def test_connection_failure(self, mock_connect, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        with pytest.raises(Exception):
            with uploader._get_connection():
                pass

    # -------------------------
    # SQL queries
    # -------------------------
    def test_create_table_query_structure(self, sample_df, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        query = uploader._create_table_query("test_table", sample_df, 512, True)

        assert query.strip().startswith("CREATE TABLE")
        assert "(" in query and ")" in query
        assert query.count(",") >= len(sample_df.columns) - 1

    def test_insert_query_structure(self, sample_df, mock_connection_string):
        uploader = DataFrameToSQL(mock_connection_string)

        query = uploader._insert_table_query("test_table", sample_df)

        assert query.startswith("INSERT INTO")
        assert "VALUES" in query
        assert "?" in query


class TestUploadToSQL:

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            {
                "id": list(range(100)),
                "name": [f"Person_{i}" for i in range(100)],
                "value": [i * 10.5 for i in range(100)],
            }
        )

    @pytest.fixture
    def conn_str(self):
        return (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=localhost,1435;"
            "Database=master;"
            "UID=sa;"
            "PWD=vSk60DcYRU;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )

    # -------------------------
    # Chunk size
    # -------------------------
    @pytest.mark.parametrize(
        "size,expected",
        [
            (5000, 1),
            (50000, 4),
            (500000, 10),
            (2000000, 20),
        ],
    )
    def test_calculate_chunk_size(self, conn_str, size, expected):
        uploader = UploadToSQL(conn_str)
        assert uploader._calculate_optimal_chunk_size(size) == expected

    # -------------------------
    # Execute
    # -------------------------
    def test_execute_new_table(self, sample_df, conn_str):
        uploader = UploadToSQL(conn_str)

        with patch.object(uploader, "_table_exists", return_value=False):
            with patch.object(
                uploader, "import_table", return_value={"rows_inserted": 100}
            ) as mock_import:
                with patch.object(
                    uploader, "upload_table", return_value={"rows_inserted": 10}
                ) as mock_upload:
                    uploader.execute(
                        sample_df, "test_table", chunk_size=1, method="override", verbose=False
                    )

                    mock_import.assert_called_once()
                    assert mock_upload.call_count == 0  # No upload_table calls with chunk_size=1

    def test_execute_append(self, sample_df, conn_str):
        uploader = UploadToSQL(conn_str)

        with patch.object(uploader, "_table_exists", return_value=True):
            with patch.object(
                uploader, "upload_table", return_value={"rows_inserted": 50}
            ) as mock_upload:

                result = uploader.execute(
                    sample_df, "test_table", chunk_size=2, method="append", verbose=False
                )

        assert result["operation"] == "append"
        assert mock_upload.call_count == 2

    def test_execute_invalid_method(self, sample_df, conn_str):
        uploader = UploadToSQL(conn_str)

        with pytest.raises(ValueError):
            uploader.execute(sample_df, "test", method="invalid")

    def test_execute_empty_df(self, conn_str):
        uploader = UploadToSQL(conn_str)

        with pytest.raises(ValueError):
            uploader.execute(pd.DataFrame(), "test", method="override")


class TestIntegration:

    def test_large_dataframe_chunking(self):
        df = pd.DataFrame({"a": range(10000)})

        uploader = UploadToSQL("mock")

        chunk_size = uploader._calculate_optimal_chunk_size(len(df))
        chunks = np.array_split(df, chunk_size)

        assert len(chunks) == chunk_size
        assert sum(len(c) for c in chunks) == len(df)

    def test_data_integrity_after_split(self):
        df = pd.DataFrame({"a": range(1000)})

        uploader = UploadToSQL("mock")
        chunks = np.array_split(df, uploader._calculate_optimal_chunk_size(len(df)))

        reconstructed = pd.concat(chunks).sort_index()

        pd.testing.assert_frame_equal(df, reconstructed)
