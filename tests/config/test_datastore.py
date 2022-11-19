from typing import Dict, List

import pytest

from factor_pricing_model_universe.config import (
    Configuration,
    DataStore,
    DelayedDataObject,
)


@pytest.fixture
def config_text():
    return """
output_filename: "output.parquet"
intermediate_directory: "intermediate/"
start_datetime: "2020-01-01"
last_datetime: "2020-01-31"
frequency: "B"
pipeline: []
data:
    a:
        function: a
    b:
        function: b
        parameters:
            a: !data a
            b:
              another_a: !data a
"""


def func_a(**kwargs) -> List[int]:
    return [1, 2, 3]


def func_b(a: List[int], b: Dict[str, List[int]], **kwargs) -> List[int]:
    return [item * 5 for item in a] + [item * 2 for item in b["another_a"]]


def test_data_store(config_text: str):
    config = Configuration(stream=config_text)
    data_store = DataStore(
        config=config,
        custom_functions={
            "a": func_a,
            "b": func_b,
        },
    )
    obj_b = data_store.get(name="b")
    assert obj_b == [5, 10, 15, 2, 4, 6]
    assert DelayedDataObject.get_values("a") == [1, 2, 3]
