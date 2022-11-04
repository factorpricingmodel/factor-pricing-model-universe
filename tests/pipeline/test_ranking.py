import pandas as pd
import pytest

from factor_pricing_model_universe.pipeline import ranking


@pytest.fixture
def input_values():
    return pd.DataFrame(
        [
            [1.0, 2.0, 3.0],
            [6.0, 5.0, 4.0],
            [7.0, 9.0, 8.0],
        ],
        index=pd.bdate_range("2022-11-01", "2022-11-03", name="date"),
        columns=["A", "AAL", "AAPL"],
    )


def test_pipeline_no_tolerance_timeframes(input_values):
    result = ranking(values=input_values, threshold_pct=0.5, tolerance_timeframes=0)
    expected = pd.DataFrame(
        [[False, False, True], [True, False, False], [False, True, False]],
        index=input_values.index,
        columns=input_values.columns,
    )
    pd.testing.assert_frame_equal(expected, result)


def test_pipeline_tolerance_timeframes(input_values):
    result = ranking(values=input_values, threshold_pct=0.5, tolerance_timeframes=1)
    expected = pd.DataFrame(
        [[False, False, True], [True, False, True], [True, True, False]],
        index=input_values.index,
        columns=input_values.columns,
    )
    pd.testing.assert_frame_equal(expected, result)
