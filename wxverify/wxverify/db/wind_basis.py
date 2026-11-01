"""Wind helpers shared by the close and the refused-access hold.

It holds the ``source_raw`` prefix of a pair-max station row and the parse of
a stored JSON object.
"""

from __future__ import annotations

import json
from typing import Final, cast

#: ``source_raw`` prefix of a pair-max station row (9 characters). The old
#: figure always stores ``"<n> km/h"``, so no old row can start with it.
PAIR_MAX_SOURCE_PREFIX: Final = "pair-max "


def json_object(raw: str | None) -> dict[str, object] | None:
    """Parse a stored JSON object; None when absent, unparseable or not an object."""
    if raw is None:
        return None
    try:
        parsed: object = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return cast(dict[str, object], parsed)
