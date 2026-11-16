"""指标采集器恢复与聚合指标数据集的验收测试（纯标准库，python tests/test_metric_recovery.py）。"""
from __future__ import annotations

import threading

from app.services import metric_recovery as recovery
from app.services.alarm import AlarmService
from app.services.metric import MetricService
from app.store import store

AGG = "metric_aggregate"
SAMPLE = "metric_sample"


def setup_function() -> None:
    store.reset()
    recovery._recovery_terminal.clear()


def test_acceptance_metr_0002_recovery() -> None:
    """验收样例：METR-0002 恢复后只保留 avg/7 一条有效记录，pause-token 清除，历史样本保留。"""
    # 恢复前：streamin-02 带着 pause-token
    source = next(row for row in store.rows("streamin") if row["源编号"] == "streamin-02")
    assert source.get("pause-token") == "pause-token"

    # 恢复前：数据集里 METR-0002 的旧有效口径是 sum/3（版本1）
    old = recovery.get_valid_aggregate("METR-0002")
    assert old is not None and old["聚合口径"] == "sum" and old["保留期"] == 3

    # 历史样本 sample-2801..2803 在位，且是原口径 sum/3
    samples = recovery.list_samples("METR-0002")
    assert [row["样本编号"] for row in samples] == ["sample-2801", "sample-2802", "sample-2803"]
    assert all(row["聚合口径"] == "sum" and row["保留期"] == 3 for row in samples)

    result, message, written = recovery.recover_collector("METR-0002")
    assert written is True and result is not None, message
    record = result["聚合指标记录"]

    # 可核对结果：数据集保留采集器唯一标识、聚合口径、保留期
    assert record["采集器唯一标识"] == "METR-0002"
    assert record["聚合口径"] == "avg"
    assert record["保留期"] == 7
    assert record["记录状态"] == "current" and record["是否有效"] is True
    assert record["数据集版本"] == 2

    # 同一采集器只保留一条有效记录
    currents = [
        row for row in store.rows(AGG)
        if row["采集器唯一标识"] == "METR-0002" and row["记录状态"] == "current"
    ]
    assert len(currents) == 1
    valid = recovery.get_valid_aggregate("METR-0002")
    assert valid is record or valid["id"] == record["id"]
    assert valid["聚合口径"] == "avg" and valid["保留期"] == 7

    # 旧口径被降级但仍可追溯；采集器上的聚合方式与保留天数一起落库
    superseded = [
        row for row in store.rows(AGG)
        if row["采集器唯一标识"] == "METR-0002" and row["记录状态"] == "superseded"
    ]
    assert superseded and all(row["聚合口径"] == "sum" for row in superseded)
    collector = recovery.find_collector("METR-0002")
    assert collector["聚合方式"] == "avg" and collector["保留天数"] == 7
    assert collector["status"] == "采集中"

    # pause-token 已清除
    assert "pause-token" not in source
    assert result["pause_token_已清除"] is True

    # 历史样本继续保留，原保留期与口径不变
    samples_after = recovery.list_samples("METR-0002")
    assert [row["样本编号"] for row in samples_after] == ["sample-2801", "sample-2802", "sample-2803"]
    assert all(row["聚合口径"] == "sum" and row["保留期"] == 3 for row in samples_after)
    assert result["历史样本数"] == 3


def test_concurrent_recovery_writes_once() -> None:
    """并发恢复同一采集器：只写入一次，所有线程拿到同一个终态。"""
    barrier = threading.Barrier(8)
    outcomes: list[tuple[dict | None, str, bool]] = []

    def worker() -> None:
        barrier.wait()
        outcomes.append(recovery.recover_collector("METR-0002"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(outcomes) == 8
    written_flags = [item[2] for item in outcomes]
    assert written_flags.count(True) == 1
    record_ids = {item[0]["聚合指标记录"]["id"] for item in outcomes}
    assert record_ids == {max(row["id"] for row in store.rows(AGG))}
    currents = [
        row for row in store.rows(AGG)
        if row["采集器唯一标识"] == "METR-0002" and row["记录状态"] == "current"
    ]
    assert len(currents) == 1

    # 恢复后再次恢复：复用首个终态，不再写新记录
    rows_before = len(store.rows(AGG))
    result, _message, written = recovery.recover_collector("METR-0002")
    assert written is False and result is not None
    assert len(store.rows(AGG)) == rows_before


def test_failed_validation_keeps_old_caliber() -> None:
    """恢复字段校验失败：旧口径不被覆盖，pause-token 与历史样本不动。"""
    collector = recovery.find_collector("METR-0002")
    collector["聚合方式"] = None
    collector["保留天数"] = 0

    before = [dict(row) for row in store.rows(AGG)]
    source = next(row for row in store.rows("streamin") if row["源编号"] == "streamin-02")
    result, message, written = recovery.recover_collector("METR-0002")
    assert result is None and written is False
    assert "聚合方式" in message and "保留天数" in message

    # 数据集一条都没动，旧有效口径仍是 sum/3
    assert store.rows(AGG) == before
    old = recovery.get_valid_aggregate("METR-0002")
    assert old["聚合口径"] == "sum" and old["保留期"] == 3
    assert source.get("pause-token") == "pause-token"
    assert len(recovery.list_samples("METR-0002")) == 3

    # 单独缺保留天数同样整体失败，聚合方式不允许单独落库
    collector["聚合方式"] = "avg"
    result, _message, written = recovery.recover_collector("METR-0002")
    assert result is None and written is False
    assert recovery.get_valid_aggregate("METR-0002")["聚合口径"] == "sum"


def test_field_normalization_and_legacy_compat() -> None:
    """字段归一化：兼容老数据的大写/中文写法，非法保留天数被拒。"""
    assert recovery.normalize_aggregation("AVG") == "avg"
    assert recovery.normalize_aggregation(" 平均 ") == "avg"
    assert recovery.normalize_aggregation("sum") == "sum"
    assert recovery.normalize_aggregation("weird") is None
    assert recovery.normalize_retention("7") == 7
    assert recovery.normalize_retention(7) == 7
    assert recovery.normalize_retention(0) is None
    assert recovery.normalize_retention(-3) is None
    assert recovery.normalize_retention(True) is None
    assert recovery.normalize_retention("7天") is None

    normalized, errors = recovery.validate_recovery_fields(
        {"指标名称": " backpressure_ratio ", "聚合方式": "AVG", "保留天数": "7"}
    )
    assert errors == []
    assert normalized == {"指标名称": "backpressure_ratio", "聚合方式": "avg", "保留天数": 7}


def test_list_dedupes_collector_and_shares_aggregate_snapshot() -> None:
    """列表去重 + 列表与详情共用同一条有效记录。"""
    # 模拟故障期同一采集器重复落库
    duplicate = dict(store.rows("metric")[1])
    duplicate["id"] = 99
    store.rows("metric").append(duplicate)

    service = MetricService()
    items, total = service.list_entries(page=1, size=100)
    assert total == 3
    assert [row["采集器编号"] for row in items].count("METR-0002") == 1

    # 详情侧栏读取的是同一根因、同一份数据集结果
    detail = service.get_entry(2)
    list_row = next(row for row in items if row["采集器编号"] == "METR-0002")
    assert detail["数据集版本"] == list_row["数据集版本"]
    assert detail["数据集聚合口径"] == list_row["数据集聚合口径"] == "sum"
    assert detail["数据集保留期"] == list_row["数据集保留期"] == 3


def test_alarm_consumes_same_single_version_dataset() -> None:
    """告警链路与数据集查询共用同一结果，avg/sum 不会同时进入同一规则。"""
    recovery.recover_collector("METR-0002")

    service = MetricService()
    metric_items, _ = service.list_aggregates(collector_id="METR-0002")
    alarm_items = AlarmService().list_aggregate_dataset("METR-0002")

    assert metric_items == alarm_items
    assert len(metric_items) == 1
    record = metric_items[0]
    calibers = {row["聚合口径"] for row in alarm_items}
    assert calibers == {"avg"}  # 只有一个版本的一个口径，sum 已随旧版本失效
    versions = {row["数据集版本"] for row in alarm_items}
    assert versions == {2}


def test_missing_and_unknown_collector() -> None:
    result, _message, written = recovery.recover_collector("METR-4040")
    assert result is None and written is False
    assert recovery.get_valid_aggregate("METR-4040") is None


def main() -> None:
    tests = [
        test_acceptance_metr_0002_recovery,
        test_concurrent_recovery_writes_once,
        test_failed_validation_keeps_old_caliber,
        test_field_normalization_and_legacy_compat,
        test_list_dedupes_collector_and_shares_aggregate_snapshot,
        test_alarm_consumes_same_single_version_dataset,
        test_missing_and_unknown_collector,
    ]
    for test in tests:
        setup_function()
        test()
        print(f"PASS {test.__name__}")
    print(f"\n全部 {len(tests)} 项验收测试通过")


if __name__ == "__main__":
    main()
