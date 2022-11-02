import pytest

import numpy as np
import pandas as pd

from factor_pricing_model_universe.pipeline import rolling_validity


@pytest.fixture(scope="module")
def input_values():
    return pd.DataFrame(
        [
            [1, 2, np.nan],
            [np.nan, 5, 6],
            [np.nan, 8, 9],
            [10, 11, np.nan],
        ],
        index=pd.bdate_range("2022-11-01", "2022-11-04", name="date"),
        columns=["A", "AAL", "AAPL"],
    )


def test_rolling_validity(input_values):
    result = rolling_validity(
        values=input_values,
        threshold_pct=0.5,
        rolling_window=2,
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
