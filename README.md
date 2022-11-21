# Factor Pricing Model Universe

<p align="center">
  <a href="https://github.com/factorpricingmodel/factor-pricing-model-universe/actions?query=workflow%3ACI">
    <img src="https://img.shields.io/github/workflow/status/factorpricingmodel/factor-pricing-model-universe/CI/main?label=CI&logo=github&style=flat-square" alt="CI Status" >
  </a>
  <a href="https://factor-pricing-model-universe.readthedocs.io">
    <img src="https://img.shields.io/readthedocs/factor-pricing-model-universe.svg?logo=read-the-docs&logoColor=fff&style=flat-square" alt="Documentation Status">
  </a>
  <a href="https://codecov.io/gh/factorpricingmodel/factor-pricing-model-universe">
    <img src="https://img.shields.io/codecov/c/github/factorpricingmodel/factor-pricing-model-universe.svg?logo=codecov&logoColor=fff&style=flat-square" alt="Test coverage percentage">
  </a>
</p>
<p align="center">
  <a href="https://python-poetry.org/">
    <img src="https://img.shields.io/badge/packaging-poetry-299bd7?style=flat-square&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAASCAYAAABrXO8xAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAJJSURBVHgBfZLPa1NBEMe/s7tNXoxW1KJQKaUHkXhQvHgW6UHQQ09CBS/6V3hKc/AP8CqCrUcpmop3Cx48eDB4yEECjVQrlZb80CRN8t6OM/teagVxYZi38+Yz853dJbzoMV3MM8cJUcLMSUKIE8AzQ2PieZzFxEJOHMOgMQQ+dUgSAckNXhapU/NMhDSWLs1B24A8sO1xrN4NECkcAC9ASkiIJc6k5TRiUDPhnyMMdhKc+Zx19l6SgyeW76BEONY9exVQMzKExGKwwPsCzza7KGSSWRWEQhyEaDXp6ZHEr416ygbiKYOd7TEWvvcQIeusHYMJGhTwF9y7sGnSwaWyFAiyoxzqW0PM/RjghPxF2pWReAowTEXnDh0xgcLs8l2YQmOrj3N7ByiqEoH0cARs4u78WgAVkoEDIDoOi3AkcLOHU60RIg5wC4ZuTC7FaHKQm8Hq1fQuSOBvX/sodmNJSB5geaF5CPIkUeecdMxieoRO5jz9bheL6/tXjrwCyX/UYBUcjCaWHljx1xiX6z9xEjkYAzbGVnB8pvLmyXm9ep+W8CmsSHQQY77Zx1zboxAV0w7ybMhQmfqdmmw3nEp1I0Z+FGO6M8LZdoyZnuzzBdjISicKRnpxzI9fPb+0oYXsNdyi+d3h9bm9MWYHFtPeIZfLwzmFDKy1ai3p+PDls1Llz4yyFpferxjnyjJDSEy9CaCx5m2cJPerq6Xm34eTrZt3PqxYO1XOwDYZrFlH1fWnpU38Y9HRze3lj0vOujZcXKuuXm3jP+s3KbZVra7y2EAAAAAASUVORK5CYII=" alt="Poetry">
  </a>
  <a href="https://github.com/ambv/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square" alt="black">
  </a>
  <a href="https://github.com/pre-commit/pre-commit">
    <img src="https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=flat-square" alt="pre-commit">
  </a>
</p>
<p align="center">
  <a href="https://pypi.org/project/factor-pricing-model-universe/">
    <img src="https://img.shields.io/pypi/v/factor-pricing-model-universe.svg?logo=python&logoColor=fff&style=flat-square" alt="PyPI Version">
  </a>
  <img src="https://img.shields.io/pypi/pyversions/factor-pricing-model-universe.svg?style=flat-square&logo=python&amp;logoColor=fff" alt="Supported Python versions">
  <img src="https://img.shields.io/pypi/l/factor-pricing-model-universe.svg?style=flat-square" alt="License">
</p>

Package to build universes for factor pricing model. For further details, please refer
to the [documentation](https://factor-pricing-model-universe.readthedocs.io/en/latest/)

## Installation

Install this via pip (or your favourite package manager):

`pip install factor-pricing-model-universe`

## Usage

The library contains the pipelines to build the universe. You can
run the pipelines interactively in Jupyter Notebook.

```python
from fpm_universe import pipeline
```

Alternatively, for scheduled runs, you can create a configuration
and run the command line entry point to create the universe.

### Configuration

The configuration is in yaml format and contains a few inputs

|           Name           |                                                                                            Description                                                                                            |
| :----------------------: | :-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
|    `output_filename`     |                                                                                          Output filename                                                                                          |
| `intermediate_directory` |                                                                       Intermediate directory to export the pipeline outputs                                                                       |
|     `start_datetime`     |                                                                                  Start datetime of the universe                                                                                   |
|     `last_datetime`      |                                                                                   Last datetime of the universe                                                                                   |
|       `frequency`        | Frequency of the universe. For further details, please see the "Offset aliases" in pandas [documentation](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases) |
|        `pipeline`        |                                                                             List of pipelines to filter the universe                                                                              |
|          `data`          |                                                                Defines the data used by pipeline, or referred by yaml tag `!data`                                                                 |

Each pipeline returns a pandas dataframe indicating if the instrument is included into the
universe on the specified date / time. For example, the pipeline returns the following
dataframe

```
+------------+--------+-------+
|    date    |  AAPL  | GOOGL |
+------------+--------+-------+
| 2022-11-17 |  True  | False |
+------------+--------+-------+
| 2022-11-18 |  True  |  True |
+------------+--------+-------+
```

and it indicates AAPL is included in the universe on both 2022-11-17 and 2022-11-18
while GOOGL only on 2022-11-18.

By default, the pipeline functions are imported from module `fpm_universe.pipeline`.

Each data defines the method to retrieve from the source, or the operator on the
source data. The return type of each data is unconstrained. It can be a json-like dict,
a list, a pandas series, or even a pandas dataframe.

In the configuration, Each data can be referred by yaml tag `!data`, and it is loaded
in lazy only when it is referred by another data object or a pipeline.

### Command

The entry point `factor-pricing-model-universe` is to generate the universe regarding
the given configuration to the destination, with dynamically passing the parameters
to format the configuration.

The arguments of the entry point are

|        Argument        |                   Description                    |
| :--------------------: | :----------------------------------------------: |
|  `-c, --config TEXT`   |        Required. Configuration file path.        |
| `-p, --parameter TEXT` | Parameters to be formatted in the configuration. |

For example, given the configuration as follows,

```
output_filename: "{output_directory}/{date}.parquet"
intermediate_directory: "{output_directory}/{date}"
start_datetime: "2015-01-01"
last_datetime: "{date}"
frequency: "B"
pipeline:
  - name: range_validity
    function: range_validity
    parameters:
      values: !data initial_validity
data:
  symbols:
    function: jq_compile
    parameters:
      json_filename: "{data_directory}/index/sp500/default/{date}.json"
      pattern: "[.[] | .tickers[]] | sort | unique | .[]"
  initial_validity:
    function: jq_compile
    parameters:
      json_filename: "{data_directory}/listings/{date}.json"
      pattern: ".[] | {{ symbol: .symbol, valid_start_datetime: .ipoDate, valid_last_datetime: .delistingDate }}"
      includes:
        symbol: !data symbols
```

and run the following command

```
factor-pricing-model-universe \
  --config <path> \
  --parameter output_directory=$HOME/output \
  --parameter data_directory=$HOME/data \
  --parameter date=2022-10-20
```

the universe dataframe is output to `$HOME/output/2022-10-20.parquet`
(formatted with the parameter `output_directory` and `date`).
