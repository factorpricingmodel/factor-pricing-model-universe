from tempfile import NamedTemporaryFile

import pytest

from factor_pricing_model_universe.config import Configuration


@pytest.fixture
def config_text():
    return """
output_filename: "{output_directory}/universe/{date}.parquet"
intermediate_directory: "{output_directory}/universe/{date}/"
pipeline: []
data: []
"""


@pytest.fixture
def parameters():
    return {
        "output_directory": ".data",
        "date": "2022-10-22",
    }


def test_config_basic(config_text, parameters):
    config = Configuration(stream=config_text, parameters=parameters)
    assert config.output_filename == ".data/universe/2022-10-22.parquet"
    assert config.intermediate_directory == ".data/universe/2022-10-22/"
    assert config.pipelines == []
    assert config.datas == []
