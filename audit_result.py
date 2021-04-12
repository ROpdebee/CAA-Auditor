from typing import Sequence

from enum import Enum

import attr

@attr.s(auto_attribs=True)
class CheckResult:
    mbid: str
    description: str

    @property
    def category(self) -> Sequence[str]:
        return self.description.split('::')

class ItemSkipped(CheckResult):
    pass

class CheckPassed(CheckResult):
    pass

class CheckFailed(CheckResult):
    pass
