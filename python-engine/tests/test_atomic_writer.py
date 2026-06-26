from pathlib import Path

from stock_selector.storage.atomic_writer import AtomicObjectWriter


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
    assert result.temp_key.startswith("smoke/trade_date=2026-06-19/.tmp-")
    assert ("stock-raw", result.final_key) in client.objects
    assert ("stock-raw", result.temp_key) not in client.objects
    assert ("stock-raw", result.temp_key) in client.removed
