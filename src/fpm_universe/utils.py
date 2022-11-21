from datetime import datetime
from typing import Optional, Union

from pandas import Timestamp


def to_timestamp(value: Optional[Union[str, datetime, Timestamp]]) -> Timestamp:
    """
    Convert a value to a Timestamp.

    :param value: The value to convert to a Timestamp.
    :type value: `str`, or any type convertible to `pandas.Timestamp`.
    :return: A Timestamp.
    :rtype: `pandas.Timestamp`
    """
    if value is None or value == "None" or value == "null":
        return None

    try:
        return Timestamp(value)
    except ValueError:
        raise ValueError(f"Failed to convert value {value} to Timestamp")
