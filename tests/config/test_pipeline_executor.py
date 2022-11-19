import pytest

from factor_pricing_model_universe.config import (
    Configuration,
    DataStore,
    PipelineExecutor,
)


def pipeline_a(a, **kwargs):
    return a + 1


def data_a():
    return 105


@pytest.fixture
def config_text():
    return """
output_filename: "output.parquet"
intermediate_directory: "intermediate/"
start_datetime: "2020-01-01"
last_datetime: "2020-01-31"
frequency: "B"
pipeline:
    - name: "pipeline_a"
      function: "pipeline_a"
      parameters:
          a: !data a
data:
    a:
        function: data_a
"""


def test_pipeline_executor_execute(config_text):
    config = Configuration(stream=config_text)
    data_store = DataStore(config=config, custom_functions={"data_a": data_a})
    name, pipeline_result = PipelineExecutor.execute(
        config=config,
        pipeline=config.pipelines[0],
        data_store=data_store,
        custom_functions={"pipeline_a": pipeline_a},
    )
    assert name == "pipeline_a"
    assert pipeline_result == 106


def test_pipeline_executor_execute_all(config_text):
    config = Configuration(stream=config_text)
    data_store = DataStore(config=config, custom_functions={"data_a": data_a})
    pipeline_executor = PipelineExecutor(
        config=config,
        custom_functions={"pipeline_a": pipeline_a},
    )
    iter = pipeline_executor.execute_all(
        data_store=data_store,
    )
    name, pipeline_result = next(iter)
    assert name == "pipeline_a"
    assert pipeline_result == 106
