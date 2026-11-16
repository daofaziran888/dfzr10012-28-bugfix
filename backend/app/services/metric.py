"""流作业指标采集业务规则：状态流转、字段校验与筛选口径都收在这里。

聚合指标数据集的查询与恢复判断统一收口在 metric_recovery 模块，本服务只做编排，
不再各写一遍字段处理；列表与详情侧栏读取同一采集器时拿到的是同一条有效记录。
"""
from __future__ import annotations

from typing import Any

from app.services import metric_recovery as recovery
from app.store import store

MODULE = "metric"
REQUIRED_FIELDS = ["采集器编号", "作业名称", "指标名称"]
STATUS_ORDER = ["草稿", "采集中", "已暂停", "已关闭"]
ACTION_RULES = {"启动采集": "草稿", "暂停采集": "采集中", "关闭采集": "已暂停"}
NEGATIVE_ACTIONS = []


class MetricService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("采集器编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        # 同一采集器只允许出现一次：按采集器编号去重，重复行保留 id 最小的首条，
        # 修复故障期重复落库导致列表里同一采集器出现多次的问题。
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            key = str(row.get("采集器编号", "")).strip()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(self._with_aggregate_snapshot(row))
        total = len(deduped)
        start = max(page - 1, 0) * size
        return deduped[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        # 详情侧栏与列表来自同一根因，共用同一份聚合指标数据集有效记录。
        return self._with_aggregate_snapshot(entry)

    def _with_aggregate_snapshot(self, collector: dict[str, Any]) -> dict[str, Any]:
        """把聚合指标数据集里的唯一有效记录（聚合口径、保留期、版本）挂到采集器视图上。"""
        view = dict(collector)
        aggregate = recovery.get_valid_aggregate(str(collector.get("采集器编号", "")))
        if aggregate is not None:
            view["数据集聚合口径"] = aggregate.get("聚合口径")
            view["数据集保留期"] = aggregate.get("保留期")
            view["数据集版本"] = aggregate.get("数据集版本")
        return view

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"指标采集器 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流作业指标采集可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"指标采集器已{action}"

    def recover(self, collector_id: str) -> tuple[dict[str, Any] | None, str, bool]:
        """恢复采集器：校验恢复字段、写入聚合指标数据集、同步链路状态。"""
        return recovery.recover_collector(collector_id)

    def list_aggregates(
        self,
        *,
        collector_id: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """聚合指标数据集查询：与恢复判断、告警消费共用同一套唯一有效记录规则。"""
        rows = recovery.list_aggregates_for_alarm(collector_id)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def list_samples(self, collector_id: str | None = None) -> list[dict[str, Any]]:
        """历史聚合样本：按原保留期、原口径完整保留，仅供追溯。"""
        return recovery.list_samples(collector_id)
