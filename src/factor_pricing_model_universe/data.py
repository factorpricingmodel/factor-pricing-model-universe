import csv
import json
from enum import Enum
from functools import partial
from os import listdir
from os.path import join as fsjoin
from typing import Any, Dict, List, Optional, Union

import jq
import pandas as pd


class FileFormat(str, Enum):
    """
    Supported file formats.
    """

    json = "json"
    csv = "csv"


class ReturnFormat(str, Enum):
    """
    Supported return formats.
    """

    dict = "dict"
    dataframe = "dataframe"
    series = "series"

    def __eq__(self, __x: object) -> bool:
        if super().__eq__(__x):
            return True

        if isinstance(object, dict) and len(object) == 1:
            return super().__eq__(list(x.keys())[0])

        return False


def load_all_data(
    directory: str,
    from_format: FileFormat,
    to_format: ReturnFormat,
    index_col: Optional[str] = None,
    parse_dates: Optional[list] = None,
):
    """
    Load all data from a directory.
    """
    file_names = listdir(directory)

    if to_format == ReturnFormat.dict:
        if from_format == FileFormat.json:
            reader = json.load
        elif from_format == FileFormat.csv:
            reader = csv.DictReader
        else:
            raise ValueError(f"Unknown file format: {from_format}")
    elif to_format == ReturnFormat.dataframe:
        if from_format == FileFormat.json:
            reader = partial(pd.read_json, convert_dates=parse_dates)
        elif from_format == FileFormat.csv:
            reader = partial(pd.read_csv, parse_dates=parse_dates, index_col=index_col)
        else:
            raise ValueError(f"Unknown file format: {from_format}")
    else:
        raise ValueError(f"Unknown return format: {to_format}")

    for file_name in file_names:
        key_name = file_name.replace("." + from_format, "")
        path = fsjoin(directory, file_name)
        with open(path) as f:
            data = reader(f)
            yield key_name, data


def jq_compile(
    pattern: str,
    json_filename: Optional[str] = None,
    json_input: Optional[Dict] = None,
    includes: Optional[Dict] = None,
    to_format: Optional[ReturnFormat] = ReturnFormat.default,
) -> Union[List, Dict]:
    """
    Compile a jq expression.

    Parameters
    ----------
    pattern: str
        The jq expression.
    filename: Optional[str]
        The json filename. Default is None.
    input_json: Optional[Dict]
        The input json.  Default is None.
    includes: Optional[Dict]
        The items to filter out in the result. The dictionary should
        contain the key names including in each item, while the
        values are the list of possible values in each item. For example,
        `{"symbol": ["A", "B"]}` means to filter out the result contains
        only items with symbol `A` and `B`.
    Returns
    -------
    dict
        The compiled jq expression.
    """

    def _valid(item, includes):
        return all([item[key] in values for key, values in includes.items()])

    compile = jq.compile(pattern)

    if json_filename:
        with open(json_filename) as f:
            json_input = json.load(f)

    result = compile.input(json_input).all()
    if includes:
        result = [item for item in result if _valid(item, includes)]

    if not to_format:
        return result

    if ReturnFormat.dict == to_format and isinstance(result, dict):
        return result
    if ReturnFormat.series == to_format and isinstance(result, list):
        if not isinstance(to_format, dict) or len(to_format) > 1:
            raise ValueError(
                f"Invalid return format: {to_format}. "
                f"Expected a dict with a single key, "
                f"but got {type(result)}."
            )

        params = {
            name: (
                [item[to_format[name]] for item in result]
                if name in to_format
                else None
            )
            for name in ["data", "index"]
        }
        params = {
            **params,
            **{name: to_format[name] for name in ["dtype", "name", "copy", "fastpath"]},
        }
        return pd.Series(**params)

    raise ValueError(f"Invalid return format: {to_format}. ")


def concat(
    data: Dict[str, pd.DataFrame],
    column: str,
) -> pd.DataFrame:
    """
    Concatenate a dict of dataframe into a single dataframe.

    Parameters
    ----------
    data: Dict[str, pd.DataFrame]
        The data frame.
    column: str
        The column name in the values of dataframes.
    """
    return pd.DataFrame({key: df[column] for key, df in data.items()})


def dataframe_operator(
    df: pd.DataFrame, operator: str, parameters: Dict[str, Any]
) -> Any:
    """
    Create a dataframe operator.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe.
    operator: str
        The dataframe operator name.
    parameters: Dict[str, Any]
        The parameters of the operation.

    Returns
    -------
    Any
        Any type returned from the operator function.
    """
    return getattr(df, operator)(**parameters)


def flatten(values: List[List[Any]]):
    """
    Flatten a list of lists.

    Parameters
    ----------
    values: List[List[Any]]
        The list of lists.
    """
    return [item for value in values for item in value]
