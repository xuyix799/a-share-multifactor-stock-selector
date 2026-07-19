from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta

import pandas as pd
import pyarrow as pa

from goal23_test_helpers import FACTOR_CONFIG, Goal23MemoryStores
from stock_selector.scoring.real_selection_result import (
    GOAL22_ARROW_SCHEMA_EVIDENCE_ATTR,
    build_goal24_selection_commit_key,
    build_goal24_selection_generation_key,
    load_goal23_manifest_catalog,
    run_real_selection_result_range,
    validate_goal22_selection_input_arrow_schema,
)


END_DATE = "2026-06-19"
_end = date.fromisoformat(END_DATE)
HISTORY_DATES = sorted(
    (_end - timedelta(days=index)).isoformat()
    for index in range(62)
)
SELECTION_DATES = HISTORY_DATES[-2:]


def with_goal22_arrow_schema_evidence(
    object_key: str,
    frame: pd.DataFrame,
) -> pd.DataFrame:
    dataset = object_key.split("/", 2)[1]
    result = frame.copy(deep=True)
    physical_schema = validate_goal22_selection_input_arrow_schema(
        dataset,
        pa.Table.from_pandas(
            result,
            preserve_index=False,
        ).schema,
    )
    result.attrs[GOAL22_ARROW_SCHEMA_EVIDENCE_ATTR] = {
        "dataset": dataset,
        "physical_schema": physical_schema,
    }
    return result


class Goal24MemoryStores:
    def __init__(self):
        self.upstream = Goal23MemoryStores(HISTORY_DATES)
        goal23_result = self.upstream.run_goal23(
            run_id="goal23-goal24-fixture",
            target_dates=SELECTION_DATES,
            apply=True,
        )
        assert goal23_result["status"] == "COMPLETED"
        self.goal23_manifest_key = goal23_result["range_manifest_key"]
        for trade_date in SELECTION_DATES:
            report = self.upstream.control_json[
                goal23_result["daily_report_keys"][trade_date]
            ]
            assert (
                report["factor_contract_audit"]["effective_factor_count"]
                >= 15
            )

        self.selection_objects: dict[str, pd.DataFrame] = {}
        self.selection_commits: dict[tuple[str, str], dict] = {}
        self.snapshots: dict[tuple[str, str], dict] = {}
        self.selection_object_writes: list[str] = []
        self.selection_commit_writes: list[tuple[str, str]] = []
        self.snapshot_writes: list[tuple[str, str]] = []
        self.events: list[tuple[str, str]] = []
        self.fail_selection_write_once: str | None = None
        self.fail_snapshot_once: tuple[str, str] | None = None
        self.tamper_written_generation_once: str | None = None

    def catalog(self):
        return load_goal23_manifest_catalog(
            manifest_keys=[self.goal23_manifest_key],
            read_json_fn=self.read_control_json,
        )

    def run_goal24(
        self,
        *,
        run_id: str = "goal24-test",
        target_dates: list[str] | None = None,
        rebalance_mode: str = "monthly",
        apply: bool = False,
        resume: bool = True,
        force: bool = False,
        selection_config: dict | None = None,
        catalog=None,
    ) -> dict:
        dates = target_dates or [SELECTION_DATES[-1]]
        return run_real_selection_result_range(
            run_id=run_id,
            start_date=dates[0],
            end_date=dates[-1],
            selection_dates=dates,
            rebalance_mode=rebalance_mode,
            goal23_manifest_catalog=catalog or self.catalog(),
            selection_config=deepcopy(selection_config or FACTOR_CONFIG),
            control_read_json_fn=self.read_control_json,
            control_write_json_fn=(
                self.write_control_json if apply else None
            ),
            goal23_factor_object_read_fn=self.read_goal23_factor_object,
            goal23_commit_read_fn=self.read_goal23_commit,
            goal22_processed_object_read_fn=self.read_goal22_object,
            goal22_commit_read_fn=self.read_goal22_commit,
            selection_object_read_fn=(
                self.read_selection_object if apply else None
            ),
            selection_object_write_fn=(
                self.write_selection_object if apply else None
            ),
            selection_commit_read_fn=(
                self.read_selection_commit if apply else None
            ),
            selection_commit_write_fn=(
                self.write_selection_commit if apply else None
            ),
            snapshot_read_fn=self.read_snapshot if apply else None,
            snapshot_upsert_fn=self.upsert_snapshot if apply else None,
            apply_processed_write=apply,
            resume=resume,
            force=force,
            generated_at_fn=lambda: "2026-06-22T00:00:00+00:00",
        )

    @property
    def control_json(self):
        return self.upstream.control_json

    @property
    def goal23_commits(self):
        return self.upstream.factor_commits

    @property
    def goal23_objects(self):
        return self.upstream.factor_objects

    @property
    def goal22_commits(self):
        return self.upstream.goal22_commits

    @property
    def goal22_objects(self):
        return self.upstream.goal22_processed_objects

    def read_control_json(self, object_key: str) -> dict:
        return self.upstream.read_control_json(object_key)

    def write_control_json(self, object_key: str, payload: dict) -> str:
        return self.upstream.write_control_json(object_key, payload)

    def read_goal23_factor_object(self, object_key: str) -> pd.DataFrame:
        return self.upstream.read_factor_object(object_key)

    def read_goal23_commit(self, trade_date: str) -> dict:
        return self.upstream.read_factor_commit(trade_date)

    def read_goal22_object(self, object_key: str) -> pd.DataFrame:
        return with_goal22_arrow_schema_evidence(
            object_key,
            self.upstream.read_goal22_processed_object(object_key),
        )

    def read_goal22_commit(self, trade_date: str) -> dict:
        return self.upstream.read_goal22_commit(trade_date)

    def read_selection_object(self, object_key: str) -> pd.DataFrame:
        self.events.append(("read_object", object_key))
        try:
            return self.selection_objects[object_key].copy(deep=True)
        except KeyError as exc:
            raise FileNotFoundError(object_key) from exc

    def write_selection_object(
        self,
        trade_date: str,
        rebalance_mode: str,
        generation_id: str,
        frame: pd.DataFrame,
    ) -> str:
        if self.fail_selection_write_once == trade_date:
            self.fail_selection_write_once = None
            raise RuntimeError(
                f"injected selection write failure for {trade_date}"
            )
        object_key = build_goal24_selection_generation_key(
            trade_date,
            rebalance_mode,
            generation_id,
        )
        written = frame.copy(deep=True)
        if self.tamper_written_generation_once == trade_date:
            self.tamper_written_generation_once = None
            written.loc[written.index[0], "total_score"] -= 1.0
        self.selection_objects[object_key] = written
        self.selection_object_writes.append(object_key)
        self.events.append(("write_object", object_key))
        return object_key

    def read_selection_commit(
        self,
        trade_date: str,
        rebalance_mode: str,
    ) -> dict:
        self.events.append(
            ("read_commit", f"{trade_date}:{rebalance_mode}")
        )
        try:
            return deepcopy(
                self.selection_commits[(trade_date, rebalance_mode)]
            )
        except KeyError as exc:
            raise FileNotFoundError(
                build_goal24_selection_commit_key(
                    trade_date,
                    rebalance_mode,
                )
            ) from exc

    def write_selection_commit(
        self,
        trade_date: str,
        rebalance_mode: str,
        payload: dict,
    ) -> str:
        self.selection_commits[(trade_date, rebalance_mode)] = deepcopy(
            payload
        )
        self.selection_commit_writes.append(
            (trade_date, rebalance_mode)
        )
        self.events.append(
            ("write_commit", f"{trade_date}:{rebalance_mode}")
        )
        return build_goal24_selection_commit_key(
            trade_date,
            rebalance_mode,
        )

    def read_snapshot(
        self,
        trade_date: str,
        rebalance_mode: str,
    ) -> dict | None:
        value = self.snapshots.get((trade_date, rebalance_mode))
        return None if value is None else deepcopy(value)

    def find_snapshot(
        self,
        trade_date: str,
        rebalance_mode: str,
    ) -> dict | None:
        return self.read_snapshot(trade_date, rebalance_mode)

    def upsert_snapshot(self, summary: dict) -> None:
        identity = (
            str(summary["trade_date"]),
            str(summary["rebalance_mode"]),
        )
        if self.fail_snapshot_once == identity:
            self.fail_snapshot_once = None
            raise RuntimeError("injected snapshot failure")
        self.snapshots[identity] = deepcopy(summary)
        self.snapshot_writes.append(identity)
        self.events.append(
            ("write_snapshot", f"{identity[0]}:{identity[1]}")
        )


_BASELINE: Goal24MemoryStores | None = None


def fresh_goal24_stores() -> Goal24MemoryStores:
    global _BASELINE
    if _BASELINE is None:
        _BASELINE = Goal24MemoryStores()
    return deepcopy(_BASELINE)
