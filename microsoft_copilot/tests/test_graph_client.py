from __future__ import annotations

from datetime import UTC, datetime

from copilot_sync.config import timestamp_str_to_datetime
from copilot_sync.graph_client import _FILTER_EPSILON, _interactions_filter


def test_interactions_filter_maps_inclusive_bounds_to_strict_operators() -> None:
    gte = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)
    lte = datetime(2025, 6, 15, 11, 0, 0, tzinfo=UTC)

    result = _interactions_filter(gte, lte)

    assert result == (
        "createdDateTime gt 2025-06-15T09:59:59.999999Z "
        "and createdDateTime lt 2025-06-15T11:00:00.000001Z"
    )
    assert " ge " not in result
    assert " le " not in result


def test_interactions_filter_preserves_boundary_inclusivity() -> None:
    gte = datetime(2025, 6, 15, 10, 0, 0, tzinfo=UTC)
    lte = datetime(2025, 6, 15, 11, 0, 0, tzinfo=UTC)
    filter_str = _interactions_filter(gte, lte)

    lower_bound = timestamp_str_to_datetime(filter_str.split("gt ")[1].split(" and")[0])
    upper_bound = timestamp_str_to_datetime(filter_str.split("lt ")[1])

    assert lower_bound < gte
    assert gte - lower_bound == _FILTER_EPSILON
    assert upper_bound > lte
    assert upper_bound - lte == _FILTER_EPSILON

    assert lower_bound < gte < upper_bound
    assert lower_bound < lte < upper_bound
