import stock_selector.storage.partition as partition
from stock_selector.storage.partition import DatasetValidationError
import pytest


def test_provider_smoke_partition_keeps_tushare_output_out_of_standard_raw_namespace(tmp_path):
    smoke_partition = partition.build_provider_smoke_partition("tushare", "daily_price", "2026-06-19", local_root=tmp_path)

    assert smoke_partition.object_key == "smoke/tushare/daily_price/trade_date=2026-06-19/part.parquet"
    assert smoke_partition.tmp_prefix == "_smoke_tmp/tushare/daily_price/trade_date=2026-06-19"
    assert smoke_partition.local_path == tmp_path / "smoke" / "tushare" / "daily_price" / "trade_date=2026-06-19" / "part.parquet"


def test_provider_smoke_partition_accepts_raw_smoke_dataset_but_standard_partition_rejects_it(tmp_path):
    smoke_partition = partition.build_provider_smoke_partition("akshare", "daily_price_raw_smoke", "2026-06-19", local_root=tmp_path)

    assert smoke_partition.object_key == "smoke/akshare/daily_price_raw_smoke/trade_date=2026-06-19/part.parquet"
    assert smoke_partition.tmp_prefix == "_smoke_tmp/akshare/daily_price_raw_smoke/trade_date=2026-06-19"
    assert smoke_partition.local_path == tmp_path / "smoke" / "akshare" / "daily_price_raw_smoke" / "trade_date=2026-06-19" / "part.parquet"

    with pytest.raises(DatasetValidationError):
        partition.build_partition("daily_price_raw_smoke", "2026-06-19", local_root=tmp_path)


def test_provider_smoke_partition_accepts_goal12b_tushare_interfaces_only_in_smoke_namespace(tmp_path):
    trade_cal = partition.build_provider_smoke_partition("tushare", "trade_cal", "2026-06-19", local_root=tmp_path)
    suspend_d = partition.build_provider_smoke_partition("tushare", "suspend_d", "2026-06-19", local_root=tmp_path)

    assert trade_cal.object_key == "smoke/tushare/trade_cal/trade_date=2026-06-19/part.parquet"
    assert suspend_d.object_key == "smoke/tushare/suspend_d/trade_date=2026-06-19/part.parquet"

    with pytest.raises(DatasetValidationError):
        partition.build_partition("trade_cal", "2026-06-19", local_root=tmp_path)
    with pytest.raises(DatasetValidationError):
        partition.build_partition("suspend_d", "2026-06-19", local_root=tmp_path)
