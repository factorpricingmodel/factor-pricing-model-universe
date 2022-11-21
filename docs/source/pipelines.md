# Pipelines

## Introduction

The package contains pipelines to generate the indicators of universe inclusions.

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

The final universe inclusions is the intersection of instruments among all the
pipelines.

## Pipeline functions

```{eval-rst}
.. automodule:: fpm_universe.pipeline
    :members:
```