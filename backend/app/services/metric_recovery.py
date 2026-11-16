"""指标采集器恢复与聚合指标数据集的共用规则。

本模块是采集器恢复、聚合指标数据集查询、告警消费三条链路的唯一口径来源：

- 恢复字段（指标名称、聚合方式、保留天数）的校验与归一化只在这里写一遍；
- 聚合指标数据集的「同一采集器唯一一条有效记录」判断只在这里写一遍；
- 恢复写入在同一把锁内完成，并发恢复同一采集器只会落一条有效记录，
  重复请求拿到同一个终态结果（首个终态生效）；
- 恢复字段校验失败时直接返回，旧有效记录保持不动，绝不被空口径覆盖；
- 历史聚合样本按写入时的原保留期保留，恢复流程不删除、不改写任何样本。
"""
from __future__ import annotations

import threading
from typing import Any

from app.store import store

METRIC_MODULE = "metric"
STREAMIN_MODULE = "streamin"
AGGREGATE_MODULE = "metric_aggregate"
SAMPLE_MODULE = "metric_sample"

# 聚合口径的允许取值与兼容老数据的别名归一表（老数据里可能出现大写/中文写法）。
AGGREGATION_MODES = ("avg", "sum", "max", "min", "count")
AGGREGATION_ALIASES = {
    "average": "avg",
    "mean": "avg",
    "平均": "avg",
    "求和": "sum",
    "汇总": "sum",
    "最大值": "max",
    "最小值": "min",
    "计数": "count",
}

CURRENT_STATUS = "current"
SUPERSEDED_STATUS = "superseded"
INVALID_STATUS = "invalid"

# 恢复写入与终态判断共用同一把进程内锁，保证并发恢复同一采集器只写一次。
_recovery_lock = threading.Lock()
# 记录每个采集器已经落定的恢复终态，后续重复恢复只返回这一份结果。
_recovery_terminal: dict[str, dict[str, Any]] = {}


def normalize_aggregation(value: Any) -> str | None:
    """把接口/老数据里的聚合方式归一为受控枚举；无法识别时返回 None。"""
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in AGGREGATION_MODES:
        return text
    return AGGREGATION_ALIASES.get(text)


def normalize_retention(value: Any) -> int | None:
    """保留天数必须是正整数；布尔值、零、负数与非数字一律视为非法。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        days = int(text)
    except (TypeError, ValueError):
        return None
    return days if days > 0 else None


def validate_recovery_fields(values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    """校验并归一化恢复字段。

    聚合方式与保留天数必须成对合法落库：任何一个缺失或非法都整体判失败，
    由调用方保证不拿这份结果去覆盖旧口径。指标名称仅做非空校验。
    返回 (归一化字段, 错误信息列表)，失败时归一化字段为 None。
    """
    metric_name = str(values.get("指标名称") or "").strip()
    aggregation = normalize_aggregation(values.get("聚合方式"))
    retention = normalize_retention(values.get("保留天数"))

    errors: list[str] = []
    if not metric_name:
        errors.append("指标名称缺失")
    if aggregation is None:
        errors.append("聚合方式缺失或不在允许范围（avg、sum、max、min、count）内")
    if retention is None:
        errors.append("保留天数缺失或不是正整数")
    if errors:
        return None, errors
    return {
        "指标名称": metric_name,
        "聚合方式": aggregation,
        "保留天数": retention,
    }, []


def find_collector(collector_id: str) -> dict[str, Any] | None:
    """按采集器唯一标识（采集器编号）读取采集器，数字 id 也兼容。"""
    text = str(collector_id or "").strip()
    if not text:
        return None
    for row in store.rows(METRIC_MODULE):
        if str(row.get("采集器编号", "")).strip() == text:
            return row
    if text.isdigit():
        return store.find(METRIC_MODULE, int(text))
    return None


def _is_valid_aggregate(row: dict[str, Any]) -> bool:
    """一条聚合指标数据集记录是否可作为有效记录：状态标记与字段完整性同时满足。"""
    if str(row.get("记录状态", "")).strip() != CURRENT_STATUS or not row.get("是否有效"):
        return False
    return (
        normalize_aggregation(row.get("聚合口径")) is not None
        and normalize_retention(row.get("保留期")) is not None
    )


def get_valid_aggregate(collector_id: str) -> dict[str, Any] | None:
    """读取某采集器在聚合指标数据集中的唯一有效记录。

    数据集里同一采集器可能残留多条记录（故障期重复写入、缺字段记录、历史版本），
    这里统一收口为一条：仅取 current 且字段完整的记录；若出现多条 current，
    取数据集版本最大、id 最大的一条，并把其余重复项降级，保证下游永远只看到一条。
    """
    text = str(collector_id or "").strip()
    candidates = [
        row for row in store.rows(AGGREGATE_MODULE)
        if str(row.get("采集器唯一标识", "")).strip() == text and _is_valid_aggregate(row)
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda row: (int(row.get("数据集版本", 0)), int(row.get("id", 0))))
    winner = candidates[-1]
    for row in candidates[:-1]:
        row["记录状态"] = SUPERSEDED_STATUS
        row["是否有效"] = False
    return winner


def list_valid_aggregates() -> list[dict[str, Any]]:
    """全量有效记录视图：每个采集器只返回同一条唯一有效记录，供查询与告警侧共用。"""
    collector_ids = {
        str(row.get("采集器唯一标识", "")).strip()
        for row in store.rows(AGGREGATE_MODULE)
        if str(row.get("采集器唯一标识", "")).strip()
    }
    result: list[dict[str, Any]] = []
    for collector_id in sorted(collector_ids):
        record = get_valid_aggregate(collector_id)
        if record is not None:
            result.append(record)
    return result


def list_aggregates_for_alarm(collector_id: str | None = None) -> list[dict[str, Any]]:
    """告警处理链路的消费入口。

    与数据集查询共用 list_valid_aggregates 的同一结果：同一采集器只有一条有效记录、
    一个数据集版本，avg 与 sum 不可能同时被送进同一条告警规则。
    """
    records = list_valid_aggregates()
    if collector_id:
        text = str(collector_id).strip()
        records = [
            row for row in records
            if str(row.get("采集器唯一标识", "")).strip() == text
        ]
    return records


def list_samples(collector_id: str | None = None) -> list[dict[str, Any]]:
    """读取历史聚合样本；样本始终按写入时的原保留期与原口径保留，不做清理。"""
    rows = store.rows(SAMPLE_MODULE)
    if collector_id:
        text = str(collector_id).strip()
        rows = [
            row for row in rows
            if str(row.get("采集器唯一标识", "")).strip() == text
        ]
    return sorted(rows, key=lambda row: int(row.get("id", 0)))


def _next_row_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _clear_pause_token(collector: dict[str, Any]) -> bool:
    """跨业务链路同步：清除关联流数据源上的 pause-token。"""
    source_id = str(collector.get("关联源编号", "")).strip()
    if not source_id:
        return False
    cleared = False
    for source in store.rows(STREAMIN_MODULE):
        if str(source.get("源编号", "")).strip() != source_id:
            continue
        if "pause-token" in source:
            source.pop("pause-token", None)
            cleared = True
    return cleared


def recover_collector(collector_id: str) -> tuple[dict[str, Any] | None, str, bool]:
    """恢复单个指标采集器并把恢复字段写入聚合指标数据集。

    返回 (结果, 说明, 是否本次新写入)。并发恢复同一采集器时：
    - 首个请求完成校验与写入并落定终态；
    - 后续请求直接返回同一份终态，不会重复写入。
    校验失败时不改动任何旧记录（含旧有效口径、历史样本、pause-token）。
    """
    collector = find_collector(collector_id)
    if collector is None:
        return None, f"指标采集器 {collector_id} 不存在或已归档", False

    collector_key = str(collector.get("采集器编号", "")).strip()
    with _recovery_lock:
        terminal = _recovery_terminal.get(collector_key)
        if terminal is not None:
            return terminal, f"指标采集器 {collector_key} 已恢复，复用首个恢复终态", False

        normalized, errors = validate_recovery_fields(collector)
        if errors:
            return None, "恢复字段校验失败：" + "；".join(errors), False

        aggregate_rows = store.rows(AGGREGATE_MODULE)
        previous = get_valid_aggregate(collector_key)
        next_version = (
            max(
                (int(row.get("数据集版本", 0))
                 for row in aggregate_rows
                 if str(row.get("采集器唯一标识", "")).strip() == collector_key),
                default=0,
            )
            + 1
        )

        # 同一采集器只保留一条有效记录：旧 current 先整体降级为历史版本，可追溯但不再生效。
        for row in aggregate_rows:
            if (
                str(row.get("采集器唯一标识", "")).strip() == collector_key
                and str(row.get("记录状态", "")).strip() == CURRENT_STATUS
            ):
                row["记录状态"] = SUPERSEDED_STATUS
                row["是否有效"] = False

        record = {
            "id": _next_row_id(aggregate_rows),
            "采集器唯一标识": collector_key,
            "指标名称": normalized["指标名称"],
            "聚合口径": normalized["聚合方式"],
            "保留期": normalized["保留天数"],
            "数据集版本": next_version,
            "记录状态": CURRENT_STATUS,
            "是否有效": True,
            "样本编号": previous.get("样本编号") if previous else None,
            "创建时间": collector.get("最近采集"),
        }
        aggregate_rows.append(record)

        # 聚合方式与保留天数一起回写到采集器，消除恢复后字段丢失/错位。
        collector["指标名称"] = normalized["指标名称"]
        collector["聚合方式"] = normalized["聚合方式"]
        collector["保留天数"] = normalized["保留天数"]
        collector["status"] = "采集中"
        collector["pending"] = True
        collector["abnormal"] = False
        collector["采集器状态"] = "采集中"

        token_cleared = _clear_pause_token(collector)

        result = {
            "采集器": dict(collector),
            "聚合指标记录": dict(record),
            "历史版本数": sum(
                1
                for row in aggregate_rows
                if str(row.get("采集器唯一标识", "")).strip() == collector_key
                and row["记录状态"] == SUPERSEDED_STATUS
            ),
            "历史样本数": len(list_samples(collector_key)),
            "pause_token_已清除": token_cleared,
        }
        _recovery_terminal[collector_key] = result
        return result, f"指标采集器 {collector_key} 恢复完成，聚合口径已写入聚合指标数据集", True
