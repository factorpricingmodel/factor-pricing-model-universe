import csv
import json
from enum import Enum
from functools import partial
from os import listdir
from os.path import basename
from os.path import join as fsjoin
from os.path import splitext
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

    default = "default"
    dict = "dict"
    dataframe = "dataframe"
    series = "series"

    def __eq__(self, __x: object) -> bool:
        if isinstance(__x, dict):
            if len(__x) != 1:
                raise TypeError(f"The dictionary {__x} length must be 1")

            return self.__eq__(list(__x.keys())[0])

        return super().__eq__(__x)


def load_all_data(
    directory: str,
    from_format: FileFormat,
    to_format: ReturnFormat,
    includes: Optional[List] = None,
):
    """
    Load all data from a directory.
    """
    file_names = listdir(directory)

    if includes:
        file_names = [
            name for name in file_names if basename(splitext(name)[0]) in includes
        ]

    if ReturnFormat.dict == to_format:
        if from_format == FileFormat.json:
            reader = json.load
        elif from_format == FileFormat.csv:
            reader = csv.DictReader
        else:
            raise ValueError(f"Unknown file format: {from_format}")
    elif ReturnFormat.dataframe == to_format:
        if from_format == FileFormat.json:
            reader = partial(
                pd.read_json,
                **(
                    {}
                    if not isinstance(to_format, dict)
                    else to_format[ReturnFormat.dataframe.value]
                ),
            )
        elif from_format == FileFormat.csv:
            reader = partial(
                pd.read_csv,
                **(
                    {}
                    if not isinstance(to_format, dict)
                    else to_format[ReturnFormat.dataframe.value]
                ),
            )
        else:
            raise ValueError(f"Unknown file format: {from_format}")
    else:
        raise ValueError(f"Unknown return format: {to_format}")

    result = {}
    for file_name in file_names:
        key_name = file_name.replace("." + from_format, "")
        path = fsjoin(directory, file_name)
        with open(path) as f:
            data = reader(f)
            result[key_name] = data

    return result


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

    if not to_format or to_format == ReturnFormat.default:
        return result

    if ReturnFormat.dict == to_format and isinstance(result, dict):
        return result
    if ReturnFormat.series == to_format and isinstance(result, list):
        to_format_parameter = to_format[ReturnFormat.series.value]
        params = {
            name: (
                [item[to_format_parameter[name]] for item in result]
                if name in to_format_parameter
                else None
            )
            for name in ["data", "index"]
        }
        params = {
            **params,
            **{
                name: to_format_parameter[name]
                for name in ["dtype", "name", "copy", "fastpath"]
                if name in to_format_parameter
            },
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


def flatten(
    values: List[List[Any]],
    ascending: Optional[bool] = None,
    unique: Optional[bool] = True,
):
    """
    Flatten a list of lists.

    Parameters
    ----------
    values: List[List[Any]]
        The list of lists.
    ascending: Optional[bool]
        Indicates to sort ascending or descending. Default is None
        which means sorting is not applied.
    unique: Optional[bool]
        Indicates to return unique values. Default is True.
    """
    result = [item for value in values for item in value]
    if unique:
        result = list(set(result))
    if isinstance(ascending, bool):
        result = sorted([r for r in result if r is not None], reverse=not ascending)
    return result


def convert_str_index_to_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a string index to a date index.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe with a datetime formatted string index.

    Returns
    -------
    pd.DataFrame
        Dataframe with a pandas datetime index.
    """
    df.index = df.index.map(lambda x: pd.to_datetime(x[:10]))
    return df
