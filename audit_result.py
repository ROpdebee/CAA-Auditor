from __future__ import annotations

from typing import Any, ClassVar, TYPE_CHECKING

from enum import Enum
if TYPE_CHECKING:
    from collections.abc import Sequence

import attr

@attr.s(auto_attribs=True)
class CheckResult:
    check_state: ClassVar[str]
    mbid: str
    check_description: str
    additional_data: Any = None

    @property
    def category(self) -> Sequence[str]:
        return self.check_description.split('::')

class ItemSkipped(CheckResult):
    check_state = 'ITEM SKIPPED'

class CheckPassed(CheckResult):
    check_state = 'PASSED'

class CheckFailed(CheckResult):
    check_state = 'FAILED'
