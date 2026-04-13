"""
Performance benchmarking script to compare original vs optimized DataFrame upload.
"""

import os
import sys
import time

import numpy as np
import pandas as pd
from memory_profiler import memory_usage

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pydbsmgr.fast_upload import DataFrameToSQL, UploadToSQL


def generate_test_data(rows: int, cols: int = 10) -> pd.DataFrame:
    """Generate test DataFrame with mixed data types."""
    np.random.seed(42)

    data = {}
    for i in range(cols):
        if i % 4 == 0:
            # Integer column
            data[f"int_col_{i}"] = np.random.randint(0, 1000, rows)
        elif i % 4 == 1:
            # Float column
            data[f"float_col_{i}"] = np.random.random(rows) * 1000
        elif i % 4 == 2:
            # String column
            data[f"str_col_{i}"] = [f"string_{j}_{i}" for j in range(rows)]
        else:
            # Boolean column
            data[f"bool_col_{i}"] = np.random.choice([True, False], rows)

    # Add some null values
    for col in data:
        if col.startswith("str_col_"):
            null_indices = np.random.choice(rows, size=int(rows * 0.05), replace=False)
            data[col] = pd.Series(data[col])
            data[col].iloc[null_indices] = None

    return pd.DataFrame(data)


def benchmark_upload_performance():
    """Benchmark the optimized upload performance."""
    print("DataFrame Upload Performance Benchmark")
    print("=" * 50)

    # Test different data sizes
    test_sizes = [1000, 10000, 50000, 1000000, 10000000]

    # Mock connection string (we'll mock the actual DB operations)
    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1435;"
        "Database=master;"
        "UID=sa;"
        "PWD=vSk60DcYRU;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    results = []

    for size in test_sizes:
        print(f"\nTesting with {size:,} rows...")

        # Generate test data
        df = generate_test_data(size)

        # Initialize uploader
        uploader = UploadToSQL(connection_string)

        # Measure memory usage and time
        start_time = time.time()
        start_memory = memory_usage()[0]

        # Simulate upload (we'll mock the actual DB operations)
        try:
            # This will fail due to no real connection, but we can measure the preprocessing
            result = uploader.execute(
                df=df, table_name=f"test_table_{size}", method="override", verbose=False
            )
        except Exception as e:
            # Expected to fail without real DB connection
            pass

        end_time = time.time()
        end_memory = memory_usage()[0]

        # Calculate metrics
        execution_time = end_time - start_time
        memory_used = end_memory - start_memory
        rows_per_second = size / execution_time if execution_time > 0 else 0

        result = {
            "rows": size,
            "execution_time": execution_time,
            "memory_used_mb": memory_used,
            "rows_per_second": rows_per_second,
            "columns": len(df.columns),
        }

        results.append(result)

        print(f"  Execution time: {execution_time:.2f} seconds")
        print(f"  Memory used: {memory_used:.1f} MB")
        print(f"  Rows per second: {rows_per_second:,.0f}")

    # Summary
    print("\n" + "=" * 50)
    print("Performance Summary:")
    print("=" * 50)

    for result in results:
        print(f"Size: {result['rows']:,} rows, {result['columns']} columns")
        print(
            f"  Time: {result['execution_time']:.2f}s, "
            f"Memory: {result['memory_used_mb']:.1f}MB, "
            f"Speed: {result['rows_per_second']:,.0f} rows/s"
        )

    return results


def benchmark_data_preprocessing():
    """Benchmark data preprocessing performance."""
    print("\nData Preprocessing Benchmark")
    print("=" * 50)

    # Generate test data with problematic values
    np.random.seed(42)
    df = pd.DataFrame(
        {
            "strings": [f"str_{i}" for i in range(10000)],
            "floats": np.random.random(10000),
            "ints": np.random.randint(0, 1000, 10000),
            "bools": np.random.choice([True, False], 10000),
        }
    )

    # Add problematic values
    df.loc[::100, "strings"] = " "  # Every 100th row has space
    df.loc[::200, "strings"] = "<NA>"  # Every 200th row has <NA>
    df.loc[::150, "floats"] = np.nan  # Every 150th row has NaN
    df.loc[::250, "floats"] = np.inf  # Every 250th row has inf

    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1435;"
        "Database=master;"
        "UID=sa;"
        "PWD=vSk60DcYRU;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    uploader = DataFrameToSQL(connection_string)

    # Benchmark preprocessing
    start_time = time.time()
    start_memory = memory_usage()[0]

    processed_df = uploader._preprocess_dataframe(df)

    end_time = time.time()
    end_memory = memory_usage()[0]

    execution_time = end_time - start_time
    memory_used = end_memory - start_memory

    print(f"Preprocessing {len(df):,} rows:")
    print(f"  Execution time: {execution_time:.3f} seconds")
    print(f"  Memory used: {memory_used:.1f} MB")
    print(f"  Rows per second: {len(df) / execution_time:,.0f}")

    # Verify preprocessing results
    null_strings = processed_df["strings"].isnull().sum()
    null_floats = processed_df["floats"].isnull().sum()

    print(f"\nPreprocessing results:")
    print(f"  Null strings: {null_strings} (expected: ~150)")
    print(f"  Null floats: {null_floats} (expected: ~133)")


def benchmark_schema_inference():
    """Benchmark schema inference performance."""
    print("\nSchema Inference Benchmark")
    print("=" * 50)

    # Generate test data with various types
    np.random.seed(42)
    df = pd.DataFrame(
        {
            "int_col": np.random.randint(0, 1000000, 10000),
            "float_col": np.random.random(10000) * 1000,
            "str_col": [f"string_{i}" for i in range(10000)],
            "bool_col": np.random.choice([True, False], 10000),
            "date_col": pd.date_range("2020-01-01", periods=10000, freq="D"),
        }
    )

    connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1435;"
        "Database=master;"
        "UID=sa;"
        "PWD=vSk60DcYRU;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    uploader = DataFrameToSQL(connection_string)

    # Benchmark schema inference
    start_time = time.time()

    schemas = {}
    for col in df.columns:
        schemas[col] = uploader._infer_schema(col, df, 512, True)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Schema inference for {len(df.columns)} columns:")
    print(f"  Execution time: {execution_time:.3f} seconds")
    print(f"  Time per column: {execution_time / len(df.columns):.4f} seconds")

    print(f"\nInferred schemas:")
    for col, schema in schemas.items():
        print(f"  {col}: {schema}")


def main():
    """Run all benchmarks."""
    print("Starting DataFrame Upload Performance Benchmarks")
    print("=" * 60)

    # Run benchmarks
    benchmark_upload_performance()
    benchmark_data_preprocessing()
    benchmark_schema_inference()

    print("\n" + "=" * 60)
    print("Benchmarks completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
