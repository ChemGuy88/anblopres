"""
"""

from __future__ import annotations

from typing import cast

x = 1
reveal_type(x)
y = cast(str, x)
reveal_type(y)
y.upper()
