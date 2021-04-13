import asyncio

from audit_result import CheckResult

class ResultAggregator:

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    async def put(self, audit_results: list[CheckResult]) -> None:
        ...
