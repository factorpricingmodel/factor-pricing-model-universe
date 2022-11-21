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
