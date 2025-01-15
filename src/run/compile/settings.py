from dataclasses import dataclass
from typing import *


@dataclass
class CompileSettings:
    first_day_of_week: str = "SUNDAY"
    """
    The first day of the week, in all caps, such as SUNDAY.
    """
    # TODO: This option should be exposed to the client layer (as part of the
    # model and/or individual calls to `.by_week`). Modeling it as a
    # compilation setting was done for backwards compatibility with some logic
    # in the Hashboard application.
