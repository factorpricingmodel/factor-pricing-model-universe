from typing import List

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
pipeline: []
data:
    a:
        function: a
    b:
        function: b
        parameters:
            a: !data a
"""


def func_a() -> List[int]:
    return [1, 2, 3]


def func_b(a: List[int]) -> List[int]:
    return [item * 5 for item in a]


def test_data_store(config_text: str):
    config = Configuration(stream=config_text)
    data_store = DataStore(
        config=config,
        custom_functions={
            "a": func_a,
            "b": func_b,
        },
    )
    obj_a = data_store.get(name="a")
    assert obj_a == [1, 2, 3]
    obj_b = data_store.get(name="b")
    assert obj_b == [5, 10, 15]
