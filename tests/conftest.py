"""测试公共工具：日志打印请求输入与关键判定依据。"""

from __future__ import annotations

import logging
import sys

import pytest

logger = logging.getLogger("gateway.test")


@pytest.fixture(autouse=True)
def _log_to_stdout(caplog):
    """让单测日志直接输出，便于查看请求输入与判定依据。"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield
    logger.removeHandler(handler)


def log_decision(msg: str, **kwargs) -> None:
    detail = ", ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.info("%s | %s", msg, detail)
