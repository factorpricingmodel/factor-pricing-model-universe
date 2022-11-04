import csv
import json
from enum import Enum
from functools import partial
from os import listdir
from os.path import join as fsjoin
from typing import Optional

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


def load_all_data(
    directory: str,
    from_format: FileFormat,
    to_format: ReturnFormat,
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
            reader = partial(pd.read_csv, parse_dates=parse_dates)
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
    json_filename: str | None = None,
    json_input: dict | None = None,
) -> dict:
    """
    Compile a jq expression.

    Parameters
    ----------
    pattern: str
        The jq expression.
    filename: str | None
        The json filename. Default is None.
    input_json: dict | None
        The input json.  Default is None.

    Returns
    -------
    dict
        The compiled jq expression.
    """
    compile = jq.compile(pattern)

    if json_filename:
        with open(json_filename) as f:
            json_input = json.load(f)

    return compile.input(json_input)
