import pandas as pd
import pytest

from fpm_universe.pipeline import ranking


@pytest.fixture
def start_datetime():
    return "2022-11-01"


@pytest.fixture
def last_datetime():
    return "2022-11-03"


@pytest.fixture
def frequency():
    return "B"


@pytest.fixture
def input_values(start_datetime, last_datetime):
    return pd.DataFrame(
        [
            [1.0, 2.0, 3.0],
            [6.0, 5.0, 4.0],
            [7.0, 9.0, 8.0],
        ],
        index=pd.bdate_range(start_datetime, last_datetime, name="datetime"),
        columns=["A", "AAL", "AAPL"],
    )


def test_pipeline_no_tolerance_timeframes(
    input_values, start_datetime, last_datetime, frequency
):
    result = ranking(
        values=input_values,
        threshold_pct=0.5,
        tolerance_timeframes=0,
        start_datetime=start_datetime,
        last_datetime=last_datetime,
        frequency=frequency,
    )
    expected = pd.DataFrame(
        [[False, False, True], [True, False, False], [False, True, False]],
        index=input_values.index,
        columns=input_values.columns,
    )
    pd.testing.assert_frame_equal(expected, result)


def test_pipeline_tolerance_timeframes(
    input_values, start_datetime, last_datetime, frequency
):
    result = ranking(
        values=input_values,
        threshold_pct=0.5,
        tolerance_timeframes=1,
        start_datetime=start_datetime,
        last_datetime=last_datetime,
        frequency=frequency,
    )
    expected = pd.DataFrame(
        [[False, False, True], [True, False, True], [True, True, False]],
        index=input_values.index,
        columns=input_values.columns,
    )
    pd.testing.assert_frame_equal(expected, result)
