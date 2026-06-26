from pathlib import Path

import pandas as pd

from stock_selector.storage.atomic_writer import AtomicObjectWriter, write_parquet_local_atomic


class FakeMinioClient:
    def __init__(self):
        self.objects = {}
        self.removed = []

    def fput_object(self, bucket, key, file_path):
        self.objects[(bucket, key)] = Path(file_path).read_bytes()

    def stat_object(self, bucket, key):
        if (bucket, key) not in self.objects:
            raise FileNotFoundError(key)
        return {"bucket": bucket, "key": key}

    def remove_object(self, bucket, key):
        self.removed.append((bucket, key))
        self.objects.pop((bucket, key), None)


def test_atomic_writer_uploads_temp_key_then_final_key_and_removes_temp(tmp_path):
    client = FakeMinioClient()
    writer = AtomicObjectWriter(client=client, tmp_dir=tmp_path)

    result = writer.write_bytes_atomic(
        bucket="stock-raw",
        final_key="smoke/trade_date=2026-06-19/smoke.parquet",
        data=b"parquet bytes",
    )

    assert result.bucket == "stock-raw"
    assert result.final_key == "smoke/trade_date=2026-06-19/smoke.parquet"
    assert result.temp_key.startswith("_raw_tmp/smoke/trade_date=2026-06-19/")
    assert ("stock-raw", result.final_key) in client.objects
    assert ("stock-raw", result.temp_key) not in client.objects
    assert ("stock-raw", result.temp_key) in client.removed


def test_local_atomic_parquet_replaces_existing_file_without_duplicate_rows(tmp_path):
    final_path = tmp_path / "data/raw/daily_price/trade_date=2026-06-19/part.parquet"
    first = pd.DataFrame([{"stock_code": "000001.SZ", "trade_date": "2026-06-19", "close": 10.0}])
    second = pd.DataFrame([{"stock_code": "000001.SZ", "trade_date": "2026-06-19", "close": 11.0}])

    write_parquet_local_atomic(first, final_path)
    write_parquet_local_atomic(second, final_path)

    loaded = pd.read_parquet(final_path)
    assert len(loaded) == 1
    assert loaded.iloc[0]["close"] == 11.0
