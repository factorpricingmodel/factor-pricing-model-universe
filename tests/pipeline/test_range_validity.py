import pandas as pd
import pytest

from fpm_universe.pipeline import range_validity


@pytest.fixture
def input_values():
    return [
        {
            "symbol": "A",
            "valid_start_datetime": "1999-11-18",
            "valid_last_datetime": "null",
        },
        {
            "symbol": "AA",
            "valid_start_datetime": "2016-10-18",
            "valid_last_datetime": "null",
        },
        {
            "symbol": "ZX",
            "valid_start_datetime": "2011-05-16",
            "valid_last_datetime": "2018-06-14",
        },
    ]


def test_rolling_validity_2010_2022(input_values):
    result = range_validity(
        values=input_values,
        start_datetime="2000-01-01",
        last_datetime="2022-01-01",
        frequency="B",
    )
    assert result.shape == (5740, 3)
    pd.testing.assert_series_equal(
        result.sum(),
        pd.Series(
            {
                "A": 5740,
                "AA": 1359,
                "ZX": 1849,
            }
        ),
    )


def test_rolling_validity_2019_2022(input_values):
    result = range_validity(
        values=input_values,
        start_datetime="2019-01-01",
        last_datetime="2022-01-01",
        frequency="B",
    )
    assert result.shape == (784, 3)
    pd.testing.assert_series_equal(
        result.sum(),
        pd.Series(
            {
                "A": 784,
                "AA": 784,
                "ZX": 0,
            }
        ),
    )
