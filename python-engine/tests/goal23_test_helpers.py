from __future__ import annotations

from copy import deepcopy

import pandas as pd

from stock_selector.data.historical_backfill import dataframe_checksum
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.real_clean_universe import (
    InputArtifact,
    InputVersion,
    REQUIRED_INPUTS,
    build_goal22_processed_commit_key,
    build_goal22_processed_generation_key,
    run_real_clean_universe_range,
)
from stock_selector.factors.real_factor_daily import (
    build_goal23_factor_commit_key,
    build_goal23_factor_generation_key,
    load_goal22_manifest_catalog,
    run_real_factor_daily_range,
)


FACTOR_CONFIG = {
    "quality_score": 0.30,
    "growth_score": 0.25,
    "valuation_score": 0.20,
    "industry_score": 0.15,
    "trend_score": 0.10,
    "scoring": {
        "null_score_policy": "neutral",
        "neutral_score": 50,
        "top_n": 50,
    },
}


class Goal23MemoryStores:
    def __init__(self, trade_dates: list[str], *, empty_universe: bool = False):
        self.trade_dates = sorted(trade_dates)
        self.control_json: dict[str, dict] = {}
        self.canonical_objects: dict[str, pd.DataFrame] = {}
        self.goal22_processed_objects: dict[str, pd.DataFrame] = {}
        self.goal22_commits: dict[str, dict] = {}
        self.factor_objects: dict[str, pd.DataFrame] = {}
        self.factor_commits: dict[str, dict] = {}
        self.factor_object_writes: list[tuple[str, str]] = []
        self.factor_commit_writes: list[str] = []
        self.factor_events: list[tuple[str, str]] = []
        self.fail_factor_write_once: str | None = None

        inputs = self._build_inputs(empty_universe=empty_universe)
        trusted_lineage = _trusted_input_lineage(inputs, self.trade_dates)
        result = run_real_clean_universe_range(
            run_id="goal22-fixture",
            start_date=self.trade_dates[0],
            end_date=self.trade_dates[-1],
            trade_dates=self.trade_dates,
            trusted_input_lineage=trusted_lineage,
            input_read_fn=lambda dataset, date: _copy_artifact(
                inputs[(dataset, date)]
            ),
            artifact_read_json_fn=self.read_control_json,
            artifact_write_json_fn=self.write_control_json,
            processed_read_fn=self.read_goal22_processed,
            processed_object_read_fn=self.read_goal22_processed_object,
            processed_object_write_fn=self.write_goal22_processed_object,
            processed_commit_read_fn=self.read_goal22_commit,
            processed_commit_write_fn=self.write_goal22_commit,
            apply_processed_write=True,
            resume=True,
            generated_at_fn=lambda: "2026-06-20T00:00:00+00:00",
        )
        assert result["status"] == "COMPLETED"
        self.goal22_manifest_key = result["range_manifest_key"]

    def _build_inputs(
        self,
        *,
        empty_universe: bool,
    ) -> dict[tuple[str, str], InputArtifact]:
        result: dict[tuple[str, str], InputArtifact] = {}
        for trade_date in self.trade_dates:
            for dataset in REQUIRED_INPUTS:
                if dataset == "st_history":
                    frame = pd.DataFrame(
                        columns=[
                            "stock_code",
                            "st_type",
                            "start_date",
                            "end_date",
                            "source",
                        ]
                    )
                else:
                    frame = generate_mock_dataset(dataset, trade_date)
                if dataset == "daily_price":
                    frame["amount"] = 1.0 if empty_universe else 100_000_000.0
                if dataset == "financial":
                    frame["roe"] = 0.10
                    frame["debt_ratio"] = 0.40
                object_key = (
                    f"raw/{dataset}/trade_date={trade_date}/part.parquet"
                )
                self.canonical_objects[object_key] = frame.copy(deep=True)
                result[(dataset, trade_date)] = InputArtifact(
                    frame=frame.copy(deep=True),
                    versions=(
                        InputVersion(
                            object_key=object_key,
                            row_count=len(frame),
                            checksum=dataframe_checksum(frame),
                        ),
                    ),
                )
        return result

    def catalog(self):
        return load_goal22_manifest_catalog(
            manifest_keys=[self.goal22_manifest_key],
            read_json_fn=self.read_control_json,
        )

    def run_goal23(
        self,
        *,
        run_id: str = "goal23-test",
        target_dates: list[str] | None = None,
        apply: bool = False,
        resume: bool = True,
        force: bool = False,
        catalog=None,
        factor_config: dict | None = None,
    ) -> dict:
        dates = target_dates or [self.trade_dates[-1]]
        return run_real_factor_daily_range(
            run_id=run_id,
            start_date=dates[0],
            end_date=dates[-1],
            trade_dates=dates,
            goal22_manifest_catalog=catalog or self.catalog(),
            factor_config=deepcopy(factor_config or FACTOR_CONFIG),
            control_read_json_fn=self.read_control_json,
            control_write_json_fn=self.write_control_json,
            goal22_processed_object_read_fn=self.read_goal22_processed_object,
            canonical_object_read_fn=self.read_canonical_object,
            goal22_commit_read_fn=self.read_goal22_commit,
            factor_object_read_fn=self.read_factor_object if apply else None,
            factor_object_write_fn=self.write_factor_object if apply else None,
            factor_commit_read_fn=self.read_factor_commit if apply else None,
            factor_commit_write_fn=self.write_factor_commit if apply else None,
            apply_processed_write=apply,
            resume=resume,
            force=force,
            generated_at_fn=lambda: "2026-06-21T00:00:00+00:00",
        )

    def read_control_json(self, object_key: str) -> dict:
        try:
            return deepcopy(self.control_json[object_key])
        except KeyError as exc:
            raise FileNotFoundError(object_key) from exc

    def write_control_json(self, object_key: str, payload: dict) -> str:
        self.control_json[object_key] = deepcopy(payload)
        return object_key

    def read_canonical_object(self, object_key: str) -> pd.DataFrame:
        try:
            return self.canonical_objects[object_key].copy(deep=True)
        except KeyError as exc:
            raise FileNotFoundError(object_key) from exc

    def read_goal22_processed(self, dataset: str, trade_date: str) -> pd.DataFrame:
        commit = self.read_goal22_commit(trade_date)
        return self.read_goal22_processed_object(
            commit["outputs"][dataset]["object_key"]
        )

    def read_goal22_processed_object(self, object_key: str) -> pd.DataFrame:
        try:
            return self.goal22_processed_objects[object_key].copy(deep=True)
        except KeyError as exc:
            raise FileNotFoundError(object_key) from exc

    def write_goal22_processed_object(
        self,
        dataset: str,
        trade_date: str,
        generation_id: str,
        frame: pd.DataFrame,
    ) -> str:
        object_key = build_goal22_processed_generation_key(
            dataset,
            trade_date,
            generation_id,
        )
        self.goal22_processed_objects[object_key] = frame.copy(deep=True)
        return object_key

    def read_goal22_commit(self, trade_date: str) -> dict:
        try:
            return deepcopy(self.goal22_commits[trade_date])
        except KeyError as exc:
            raise FileNotFoundError(
                build_goal22_processed_commit_key(trade_date)
            ) from exc

    def write_goal22_commit(self, trade_date: str, payload: dict) -> str:
        self.goal22_commits[trade_date] = deepcopy(payload)
        return build_goal22_processed_commit_key(trade_date)

    def read_factor_object(self, object_key: str) -> pd.DataFrame:
        self.factor_events.append(("read_object", object_key))
        try:
            return self.factor_objects[object_key].copy(deep=True)
        except KeyError as exc:
            raise FileNotFoundError(object_key) from exc

    def write_factor_object(
        self,
        trade_date: str,
        generation_id: str,
        frame: pd.DataFrame,
    ) -> str:
        if self.fail_factor_write_once == trade_date:
            self.fail_factor_write_once = None
            raise RuntimeError(f"injected factor write failure for {trade_date}")
        object_key = build_goal23_factor_generation_key(
            trade_date,
            generation_id,
        )
        self.factor_objects[object_key] = frame.copy(deep=True)
        self.factor_object_writes.append((trade_date, object_key))
        self.factor_events.append(("write_object", object_key))
        return object_key

    def read_factor_commit(self, trade_date: str) -> dict:
        self.factor_events.append(("read_commit", trade_date))
        try:
            return deepcopy(self.factor_commits[trade_date])
        except KeyError as exc:
            raise FileNotFoundError(
                build_goal23_factor_commit_key(trade_date)
            ) from exc

    def write_factor_commit(self, trade_date: str, payload: dict) -> str:
        self.factor_commits[trade_date] = deepcopy(payload)
        self.factor_commit_writes.append(trade_date)
        self.factor_events.append(("write_commit", trade_date))
        return build_goal23_factor_commit_key(trade_date)


def _trusted_input_lineage(
    inputs: dict[tuple[str, str], InputArtifact],
    trade_dates: list[str],
) -> dict:
    codes = sorted(
        set(inputs[("stock_basic", trade_dates[0])].frame["stock_code"].astype(str))
    )
    canonical_versions = {}
    for trade_date in trade_dates:
        canonical_versions[trade_date] = {}
        for dataset in REQUIRED_INPUTS:
            artifact = inputs[(dataset, trade_date)]
            version = artifact.versions[0]
            canonical_versions[trade_date][dataset] = {
                "object_key": version.object_key,
                "object_row_count": version.row_count,
                "object_checksum": version.checksum,
                "scope_row_count": len(artifact.frame),
                "scope_checksum": dataframe_checksum(artifact.frame),
            }
    return {
        "schema_version": "goal22.trusted_input_lineage.v1",
        "trade_dates": list(trade_dates),
        "codes": codes,
        "readiness_receipts": [
            {
                "batch_id": "goal23-fixture-receipt",
                "readiness_report_key": (
                    "candidate/real_clean_inputs/readiness_report/"
                    "batch_id=goal23-fixture-receipt/report.json"
                ),
                "readiness_report_checksum": "a" * 64,
                "manifest_key": (
                    "candidate/real_clean_inputs/manifest/"
                    "batch_id=goal23-fixture-receipt/manifest.json"
                ),
                "manifest_checksum": "b" * 64,
            }
        ],
        "canonical_versions": canonical_versions,
    }


def _copy_artifact(artifact: InputArtifact) -> InputArtifact:
    return InputArtifact(
        frame=artifact.frame.copy(deep=True),
        versions=artifact.versions,
    )
