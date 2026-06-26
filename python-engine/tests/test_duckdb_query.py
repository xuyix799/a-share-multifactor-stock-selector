from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.storage.atomic_writer import write_parquet_local_atomic
from stock_selector.storage.duckdb_query import query_dataset_file, query_stock_price_files
from stock_selector.storage.partition import build_partition


def test_duckdb_reads_mock_parquet_dataset(tmp_path):
    partition = build_partition("daily_price", "2026-06-19", local_root=tmp_path)
    write_parquet_local_atomic(generate_mock_dataset("daily_price", "2026-06-19"), partition.local_path)

    rows = query_dataset_file(partition.local_path)

    assert len(rows) >= 5


def test_duckdb_queries_stock_price_by_code_and_date_range(tmp_path):
    partition = build_partition("daily_price", "2026-06-19", local_root=tmp_path)
    write_parquet_local_atomic(generate_mock_dataset("daily_price", "2026-06-19"), partition.local_path)

    rows = query_stock_price_files([partition.local_path], "000001.SZ", "2026-06-19", "2026-06-19")

    assert len(rows) == 1
    assert rows[0]["stock_code"] == "000001.SZ"
