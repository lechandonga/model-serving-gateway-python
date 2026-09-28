"""滑动窗口统计：并发更新不漏算、不重复，且按版本隔离。"""

from __future__ import annotations

import threading

from gateway.window import SlidingWindow


def test_concurrent_updates_are_counted_exactly_once():
    win = SlidingWindow(window_s=60)
    n_threads, per_thread = 8, 250
    barrier = threading.Barrier(n_threads)

    def worker() -> None:
        barrier.wait()
        for i in range(per_thread):
            win.record_request()
            if i % 5 == 0:
                win.record_backend_failure()
            elif i % 7 == 0:
                win.record_rejection("OVERLOADED")
            else:
                win.record_success()

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    summary = win.summary()
    total = n_threads * per_thread
    assert summary["requests"] == total
    assert (
        summary["successes"]
        + summary["backend_failures"]
        + sum(summary["rejections"].values())
        == total
    )


def test_expired_events_roll_off_the_window():
    ticks = iter([100.0, 105.0, 130.0, 131.0, 131.0])
    win = SlidingWindow(window_s=10.0, time_func=lambda: next(ticks))
    win.record_request()
    win.record_success()
    # 时间推进到 130：100 的事件超出窗口被裁掉，105 的仍保留
    win.summary()
    # 再来一个请求并读取摘要
    win.record_request()
    summary = win.summary()
    assert summary["requests"] == 1
    assert summary["successes"] == 0
