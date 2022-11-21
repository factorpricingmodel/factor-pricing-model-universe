import pandas as pd
import pytest

from fpm_universe.data import concat


@pytest.fixture
def prices():
    def _set_prices(price):
        return pd.DataFrame(
            price,
            columns=["Close", "Volume"],
            index=pd.bdate_range("2022-01-01", "2022-01-05", name="Date"),
        )

    return {
        "A": _set_prices(1.0),
        "AA": _set_prices(2.0),
        "AAPL": _set_prices(3.0),
    }


def test_concat(prices):
    result = concat(
        data=prices,
        column="Close",
    )
    expected = pd.DataFrame(
        {
            "A": 1.0,
            "AA": 2.0,
            "AAPL": 3.0,
        },
        index=pd.bdate_range("2022-01-01", "2022-01-05", name="Date"),
    )
    pd.testing.assert_frame_equal(result, expected)
