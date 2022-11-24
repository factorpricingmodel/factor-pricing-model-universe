# Configuration

The configuration is in yaml format and used in the command entry point.

## Parameters

The following parameters are mandatory in the configuration file.

|           Name           |                                                                                            Description                                                                                            |
| :----------------------: | :-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
|    `output_filename`     |                                                                                          Output filename                                                                                          |
| `intermediate_directory` |                                                                       Intermediate directory to export the pipeline outputs                                                                       |
|     `start_datetime`     |                                                                                  Start datetime of the universe                                                                                   |
|     `last_datetime`      |                                                                                   Last datetime of the universe                                                                                   |
|       `frequency`        | Frequency of the universe. For further details, please see the "Offset aliases" in pandas [documentation](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases) |
|        `pipeline`        |                                                                             List of pipelines to filter the universe                                                                              |
|          `data`          |                                                                Defines the data used by pipeline, or referred by yaml tag `!data`                                                                 |

## Examples

1. US Equities

The configuration [example](https://github.com/factorpricingmodel/factor-pricing-model-universe/blob/main/examples/us_equities.yaml) returns
an universe of US equities from the top marketcap and liquidity stocks
from the three major indexes (S&P, NASDAQ and DJ500).

The period is between 2015-01-01 and 2022-10-20 and the selection pipelines are

- Range validity: Select only instruments listed on the exchanges
- Marketcap ranking: Select the top 50% marketcap stocks from all the instruments
- Daily liquidity validity: Select the stocks trading actively in the exchange for 90%
  of time in the past 63 business days

The output result is stored as parquet format and exported in the [link](https://raw.githubusercontent.com/factorpricingmodel/factor-pricing-model-universe/main/examples/us_equities.parquet)
