from numpy import nan
import pandas as pd


def ranking(
    values: pd.DataFrame,
    threshold_pct: float,
    tolerance_timeframes: int = 21,
) -> pd.DataFrame:
    """
    Include the instrument into the universe by ranking.

    The instrument is selected by a threshold percentage of ranking,
    and allow to stay in the universe for a specified number of
    tolerance timeframes.

    For example, if the parameter `threshold_pct` is 0.4, and
    `tolerance_timeframes` is 21, the instrument will be selected into the
    universe only if it is ranked as the top 40% of the values,
    and will be stayed in the universe for the next 21 timeframes even
    if the condition is no longer fulfilled.

    :param values: The values are sorted in cross sectional rank. The
      columns are instruments, and the index are in datetime.
    :type values: class:`pandas.DataFrame`.
    :param threshold_pct: The threshold percentage which should be between
      0 and 1.
    :type threshold_pct: `float`.
    :param tolerance_timeframes: The number of timeframes to allow the
      instrument stays in the universe even its values is outside of the
      threshold percentage. Default is 21.
    :type tolerance_timeframes: `int`.
    :return: A dataframe indicating whether the instrument is included in
      the universe.
    :rtype: `pd.DataFrame`.
    """
    assert (
        0 <= threshold_pct <= 1
    ), f"Threshold percentage {threshold_pct} must be between 0 and 1"
    rank_values = values.rank(axis=1, ascending=False)
    num_instruments = values.notnull().sum(axis=1)
    threshold = num_instruments * threshold_pct
    result = rank_values.le(threshold, axis=0).replace(False, nan)

    if tolerance_timeframes > 0:
        result = result.ffill(limit=tolerance_timeframes)

    return result.fillna(False)


def rolling_validity(
    values: pd.DataFrame,
    threshold_pct: float,
    rolling_window: int,
) -> pd.DataFrame:
    """
    Include the instrument into the universe by rolling validity.

    The instrument is included into the universe if the values in the
    rolling timeseries exist in a specified timeframe and threshold
    percentage.

    For example, if the `threshold_pct` is 0.8 and `rolling_window` is
    21, the instrument is included into the universe only if the previous
    21 days have at least 80% valid values in the rolling basis.

    :param values: The values are sorted in cross sectional rank. The
      columns are instruments, and the index are in datetime.
    :type values: class:`pandas.DataFrame`.
    :param threshold_pct: The threshold percentage which should be between
      0 and 1.
    :type threshold_pct: `float`.
    :param rolling_window: The number of rolling timeframes that in the
      past the values exist. The number must be non-negaive.
    :type rolling_window: `int`.
    :return: A dataframe indicating whether the instrument is included in
      the universe.
    :rtype: `pd.DataFrame`.
    """
    return (
        values.notnull().rolling(window=rolling_window, min_periods=1).sum()
        >= threshold_pct * rolling_window
    )
