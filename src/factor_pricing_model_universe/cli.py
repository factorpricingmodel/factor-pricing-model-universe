import logging
from os import makedirs
from os.path import join as fsjoin

import click
import pandas as pd

from .config import Configuration, DataStore, PipelineExecutor

LOGGER = logging.getLogger(__name__)


@click.command()
@click.option("--config", "-c", help="Configuration file path.", required=True)
@click.option(
    "--parameter",
    "-p",
    multiple=True,
    help="Parameters to be formatted in the configuration.",
)
def main(config, parameter):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    LOGGER.info("Loading parameters")
    parsed_parameters = {}
    for param in parameter:
        try:
            key, value = param.split("=")
        except ValueError:
            raise click.BadParameter(
                f"Failed to parse the parameter {parameter} into key-value "
                "pair. Please pass the parameter in the format of `{key}={value}`"
            )
        parsed_parameters[key] = value
    LOGGER.info(f"Parsed parameters: {parsed_parameters}")

    LOGGER.info("Loading configuration")
    with open(config) as fp:
        config = Configuration(stream=fp.read(), parameters=parsed_parameters)

    LOGGER.info("Loading data store")
    data_store = DataStore(config=config)

    LOGGER.info("Loading pipeline executor")
    pipeline_executor = PipelineExecutor(config=config)

    LOGGER.info("Executing the pipelines")
    pipeline_results = {}
    final_result = None
    makedirs(config.intermediate_directory, exist_ok=True)
    for name, result in pipeline_executor.execute_all(data_store=data_store):
        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"Pipeline {name} does not return a DataFrame")
        pipeline_results[name] = result
        result.to_parquet(fsjoin(config.intermediate_directory, f"{name}.parquet"))
        if final_result is None:
            final_result = result
        else:
            final_result &= result

    LOGGER.info("Exporting the final pipeline results")
    final_result.to_parquet(config.output_filename)
    LOGGER.info("Completed")
