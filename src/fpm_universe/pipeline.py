import logging
from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from numpy import nan

from .utils import to_timestamp

LOGGER = logging.getLogger(__name__)


def range_validity(
    values: List[Dict[str, str]],
    start_datetime: Union[str, datetime, pd.Timestamp],
    last_datetime: Union[str, datetime, pd.Timestamp],
    frequency: str,
) -> pd.DataFrame:
    """
    Include the instrument into universe by the datetime range of validity.

    :param values: The list of instrument including the symbol, valid start
        datetime and valid last datetime.
    :type values: `list[dict[str, str]]`
    :param start_datetime: The universe start datetime.
    :type start_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param last_datetime: The universe last datetime.
    :type last_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param frequency: The frequency string supported in pandas. For further
        details, please refer to
        [link](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
    :type frequency: `str`
    :return: A dataframe indicating whether the instrument is included in
      the universe.
    :rtype: `pd.DataFrame`.
    """
    start_datetime = to_timestamp(start_datetime)
    last_datetime = to_timestamp(last_datetime)
    datetime_range = pd.date_range(
        start=start_datetime,
        end=last_datetime,
        freq=frequency,
        name="datetime",
    )
    default_validity = pd.Series(False, index=datetime_range)
    universe = {}
    for value in values:
        symbol = value["symbol"]
        valid_start_datetime = to_timestamp(value.get("valid_start_datetime"))
        valid_last_datetime = to_timestamp(value.get("valid_last_datetime"))
        if not valid_start_datetime:
            raise ValueError(f"Missing 'valid_start_datetime' key in value {value}")

        instrument_validity = default_validity.copy()
        if valid_start_datetime <= start_datetime:
            valid_start_datetime = start_datetime
        if valid_last_datetime is None or valid_last_datetime >= last_datetime:
            valid_last_datetime = last_datetime
        if valid_start_datetime >= valid_last_datetime:
            LOGGER.warning(
                f"No valid range is found between {start_datetime} and {last_datetime} "
                f"for {symbol}"
            )
        instrument_validity.loc[valid_start_datetime:valid_last_datetime] = True
        universe[symbol] = instrument_validity

    return pd.DataFrame(universe)


def ranking(
    values: pd.DataFrame,
    threshold_pct: float,
    tolerance_timeframes: int,
    start_datetime: Union[str, datetime, pd.Timestamp],
    last_datetime: Union[str, datetime, pd.Timestamp],
    frequency: str,
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
    :param start_datetime: The universe start datetime.
    :type start_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param last_datetime: The universe last datetime.
    :type last_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param frequency: The frequency string supported in pandas. For further
        details, please refer to
        [link](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
    :type frequency: `str`
    :rtype: `pd.DataFrame`.
    """
    if not (0 <= threshold_pct <= 1):
        raise ValueError(
            f"Threshold percentage {threshold_pct} must be between 0 and 1"
        )
    datetime_range = pd.date_range(
        start=start_datetime,
        end=last_datetime,
        freq=frequency,
        name="datetime",
    )
    rank_values = values.rank(axis=1, ascending=False)
    num_instruments = values.notnull().sum(axis=1)
    threshold = num_instruments * threshold_pct
    result = (
        rank_values.le(threshold, axis=0)
        .replace(False, nan)
        .reindex(index=datetime_range)
    )

    if tolerance_timeframes > 0:
        result = result.ffill(limit=tolerance_timeframes)

    return result.fillna(False)


def rolling_validity(
    values: pd.DataFrame,
    threshold_pct: float,
    rolling_window: int,
    tolerance_timeframes: int,
    start_datetime: Union[str, datetime, pd.Timestamp],
    last_datetime: Union[str, datetime, pd.Timestamp],
    frequency: str,
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
    :param tolerance_timeframes: The number of timeframes to allow the
      instrument stays in the universe even its values is outside of the
      threshold percentage. Default is 21.
    :type tolerance_timeframes: `int`.
    :param start_datetime: The universe start datetime.
    :type start_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param last_datetime: The universe last datetime.
    :type last_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param frequency: The frequency string supported in pandas. For further
        details, please refer to
        [link](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
    :type frequency: `str`
    :rtype: `pd.DataFrame`.
    """
    datetime_range = pd.date_range(
        start=start_datetime,
        end=last_datetime,
        freq=frequency,
        name="datetime",
    )
    result = (
        values.notnull().rolling(window=rolling_window, min_periods=1).sum()
        >= threshold_pct * rolling_window
    ).reindex(index=datetime_range)

    if tolerance_timeframes > 0:
        result = result.ffill(limit=tolerance_timeframes)

    return result


def combine_validity(*args: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine validity.

    Parameters
    ----------
    args : List[pd.DataFrame]
      List of validity dataframes, each of which is produced by
      a single pipeline.
    """
    final_validity = None

    for validity in args:
        if final_validity is None:
            final_validity = validity
        else:
            final_validity &= validity

    return final_validity


def rolling_correlation_rank_validity(
    values: pd.DataFrame,
    rankings: pd.DataFrame,
    rolling_window: int,
    threshold: float,
    start_datetime: Union[str, datetime, pd.Timestamp],
    last_datetime: Union[str, datetime, pd.Timestamp],
    frequency: str,
) -> pd.DataFrame:
    """
    Exclude instruments if the correlations are too high and only the higher
    ranked instruments are selected.

    :param values: The values to compare with their correlations, e.g. returns.
      The columns are instruments, and the index are in datetime.
    :type values: class:`pandas.DataFrame`.
    :param rankings: The rankings are sorted in cross sectional rank. The
      columns are instruments, and the index are in datetime.
    :type rankings: class:`pandas.DataFrame`.
    :param rolling_window: The number of rolling timeframes that in the
      past the values exist. The number must be non-negaive.
    :type rolling_window: `int`.
    :param threshold: The threshold correlation which should be between
      0 and 1.
    :type threshold: `float`.
    :param start_datetime: The universe start datetime.
    :type start_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param last_datetime: The universe last datetime.
    :type last_datetime: `str`, or any type convertible by pandas `Timestamp`.
    :param frequency: The frequency string supported in pandas. For further
        details, please refer to
        [link](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
    :type frequency: `str`.
    :return: A dataframe indicating whether the instrument is included in
      the universe.
    :rtype: `pd.DataFrame`.
    """

    def _validity(
        t_ranks,
        t_values,
        threshold_pct,
    ):
        if t_ranks.isnull().all():
            return pd.Series(nan, index=t_ranks.index)
        t_ranks = t_ranks.sort_values()
        t_validity = t_ranks.notnull()
        t_ranks = {index: r for r, index in enumerate(t_ranks.index)}
        t_corr = t_values.loc[:, t_validity].corr().stack()
        cp_t_ranks = t_corr.index.get_level_values(0).map(
            t_ranks
        ) < t_corr.index.get_level_values(1).map(t_ranks)
        too_correlated = t_corr.loc[cp_t_ranks & (t_corr.abs() > threshold_pct)]
        too_correlated_index = list(set(too_correlated.index.get_level_values(1)))
        t_validity[too_correlated_index] = False
        return t_validity

    datetime_range = pd.date_range(
        start=start_datetime,
        end=last_datetime,
        freq=frequency,
        name="datetime",
    )
    start_window_datetime_range = datetime_range.shift(-rolling_window)
    validity = {}
    for i in range(len(datetime_range)):
        st = start_window_datetime_range[i]
        et = datetime_range[i]
        validity[et] = _validity(
            t_ranks=rankings.loc[et],
            t_values=values.loc[st:et],
            threshold_pct=threshold,
        )
    return pd.concat(validity, axis=1).T
