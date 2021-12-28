from enum import Enum


class TradeStatuses(str, Enum):
    # Longs
    EnterLong = 'Enter Long'
    EnterExitLong = 'Enter/Exit Long'
    Long = 'Long'
    ExitLong = 'Exit Long'

    # Shorts
    EnterShort = 'Enter Short'
    EnterExitShort = 'Enter/Exit Short'
    Short = 'Short'
    ExitShort = 'Exit Short'


