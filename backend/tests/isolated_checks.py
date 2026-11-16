"""隔离模式（python -I）下的补充验收：失败不覆盖、并发只写一次。"""
import sys

sys.path.insert(0, "/workspace/backend")

import threading  # noqa: E402

from app.services import metric_recovery as r  # noqa: E402
from app.store import store  # noqa: E402

# 场景 A：校验失败，旧口径不被覆盖
r._recovery_terminal.clear()
collector = r.find_collector("METR-0002")
old = r.get_valid_aggregate("METR-0002")
collector["聚合方式"] = "bad-mode"
res, msg, written = r.recover_collector("METR-0002")
assert res is None and written is False
assert r.get_valid_aggregate("METR-0002")["id"] == old["id"]
print("A 校验失败不覆盖旧口径: OK ->", msg)

# 场景 B：并发 16 个恢复请求，只写一条
store.reset()
r._recovery_terminal.clear()
barrier = threading.Barrier(16)
results = []


def hit() -> None:
    barrier.wait()
    results.append(r.recover_collector("METR-0002"))


threads = [threading.Thread(target=hit) for _ in range(16)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
assert sum(written for _, _, written in results) == 1
currents = [
    row for row in store.rows("metric_aggregate")
    if row["采集器唯一标识"] == "METR-0002" and row["记录状态"] == "current"
]
assert len(currents) == 1 and currents[0]["聚合口径"] == "avg" and currents[0]["保留期"] == 7
print("B 16 并发只写一条 avg/7: OK")
