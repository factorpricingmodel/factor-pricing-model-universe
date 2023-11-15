import pandas as pd
import pytest

from fpm_universe.pipeline import rolling_correlation_rank_validity


def create_test_data():
    # Helper function to create sample data
    dates = pd.date_range("2020-01-01", periods=5, freq="D")
    data = pd.DataFrame({"A": [1, 2, 3, 4, 5], "B": [5, 4, 3, 2, 1]}, index=dates)
    rankings = pd.DataFrame({"A": [1, 2, 3, 4, 5], "B": [2, 3, 4, 5, 1]}, index=dates)
    return data, rankings


def test_normal_case():
    values, rankings = create_test_data()
    result = rolling_correlation_rank_validity(
        values, rankings, 3, 0.5, "2020-01-01", "2020-01-05", "D"
    )
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(
        pd.DataFrame(
            [[True, True], [True, False], [True, False], [True, False], [False, True]],
            index=result.index,
            columns=result.columns,
        ),
        result,
    )
    # Add more assertions here to check if the result is as expected


def test_empty_dataframe():
    empty_df = pd.DataFrame()
    with pytest.raises(KeyError):
        rolling_correlation_rank_validity(
            empty_df, empty_df, 3, 0.5, "2020-01-01", "2020-01-05", "D"
        )
