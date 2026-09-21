import logging

import pytest

from gateway.backend import DeterministicLocalBackend, ScriptedBackend
from gateway.gateway import Gateway, GatewayConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@pytest.fixture
def fast_config() -> GatewayConfig:
    return GatewayConfig(
        max_batch_size=4,
        max_wait_ms=10,
        max_queue=8,
        queue_timeout=1.0,
        request_timeout=5.0,
        breaker_failure_threshold=3,
        breaker_cooldown_seconds=0.1,
        idempotency_capacity=16,
    )


@pytest.fixture
async def gateway(fast_config):
    gw = Gateway(DeterministicLocalBackend(), fast_config)
    yield gw
    await gw.close()
