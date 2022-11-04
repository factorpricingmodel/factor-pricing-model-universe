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

Package to build universes for factor pricing model

## Installation

Install this via pip (or your favourite package manager):

`pip install factor-pricing-model-universe`

## Usage

The library contains the pipelines to build the universe. You can
run the pipelines interactively in Jupyter Notebook.

Alternatively, for scheduled runs, you can create a configuration
and run the command line entry point to create the universe.

### Configuration

The configuration is in yaml format.

For example,

```
output_filename: ".data/universe/{date}.parquet"
intermediate_directory: ".data/universe/{date}"
pipeline:
  - name: rolling_validity
    function: rolling_validity
    parameters:
      values: !data initial_validity
      start_datetime: 2015-01-01
      last_datetime: {date}
      frequency: "B"
  - name: ranking
    function: ranking
    parameters:
      values: !marketcap
      threshold_pct: 0.4
      tolerance_timeframes: 21
  - name: rolling_validity
    function: rolling_validity
    parameters:
      values: !data daily_turnover
      threshold_pct: 0.9
      rolling_window: 63
data:
  initial_validity:
    function: jq_compile
    parameters:
      filename: ".data/universe/{date}/init.json"
      pattern: ".[] | { symbol: .symbol, valid_start_datetime: .ipoDate, valid_last_datetime: .delistingDate }"
  prices:
    function: load_all_data
    parameters:
      directory: ".data/prices/{date}"
      from_format: "csv"
      to_format: "dataframe"
  volumes:
    function: pivot_data
    parameters:
      values: !data prices
      index: "Date"
      values: "Volume"
  adjusted_close_prices:
    function: pivot_data
    parameters:
      values: !data prices
      index: "Date"
      values: "Close"
  companies:
    function: load_all_data
    parameters:
      directory: ".data/companies/finnhub/{date}"
      from_format: "json"
      to_format: "dict"
  outstanding_shares:
    - function: jq_compile
      parameters:
        input_json: !data companies
        pattern: ".[] | { symbol: .ticker, shareOutstanding: .shareOutstanding }"
    - function: to_series
      parameters:
        key: symbol
        value: shareOutstanding
  marketcap:
    function: dataframe_mul
    parameters:
      source: !data prices
      target: !data outstanding_shares
      axis: 0
```yaml