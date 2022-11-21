import numpy as np
import pandas as pd
import pytest

from fpm_universe.pipeline import rolling_validity


@pytest.fixture
def start_datetime():
    return "2022-11-01"


@pytest.fixture
def last_datetime():
    return "2022-11-05"


@pytest.fixture
def frequency():
    return "B"


@pytest.fixture
def input_values(start_datetime, last_datetime):
    return pd.DataFrame(
        [
            [1, 2, np.nan],
            [np.nan, 5, 6],
            [np.nan, 8, 9],
            [10, 11, np.nan],
        ],
        index=pd.bdate_range(start_datetime, last_datetime, name="datetime"),
        columns=["A", "AAL", "AAPL"],
    )


def test_rolling_validity(input_values, start_datetime, last_datetime, frequency):
    result = rolling_validity(
        values=input_values,
        threshold_pct=0.5,
        rolling_window=2,
        start_datetime=start_datetime,
        last_datetime=last_datetime,
        frequency=frequency,
    )
    expected = pd.DataFrame(
        [
            [True, True, False],
            [True, True, True],
            [False, True, True],
            [True, True, True],
        ],
        index=input_values.index,
        columns=input_values.columns,
    )
    pd.testing.assert_frame_equal(
        expected,
        result,
    )
