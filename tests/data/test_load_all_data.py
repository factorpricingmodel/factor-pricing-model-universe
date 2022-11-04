import json
import os
from tempfile import TemporaryDirectory

import pytest

import pandas as pd

from factor_pricing_model_universe.data import load_all_data


################################################################
# Test prices
################################################################


@pytest.fixture
def prices():
    default_df = pd.DataFrame(
        0.0,
        columns=["Close", "Volume"],
        index=pd.bdate_range("2022-01-01", "2022-01-05", name="Date"),
    ).reset_index()
    return {
        "A": default_df,
        "AA": default_df,
        "AAPL": default_df,
    }


@pytest.fixture
def prices_directory(prices):
    with TemporaryDirectory() as tmp_dir:
        for symbol, price in prices.items():
            filename = f"{symbol}.csv"
            path = os.path.join(tmp_dir, filename)
            price.to_csv(path, index=False)
        yield tmp_dir


def test_load_all_data_from_csv_to_df(prices_directory, prices):
    result = dict(
        load_all_data(
            directory=prices_directory,
            from_format="csv",
            to_format="dataframe",
            parse_dates=["Date"],
        )
    )

    for name, df in result.items():
        pd.testing.assert_frame_equal(
            prices[name],
            df,
        )


################################################################
# Test companies
################################################################
@pytest.fixture
def companies():
    return {
        "A": {
            "country": "US",
            "currency": "USD",
            "exchange": "NEW YORK STOCK EXCHANGE, INC.",
            "finnhubIndustry": "Life Sciences Tools & Services",
            "ipo": "1999-11-18",
            "logo": "https://static2.finnhub.io/file/publicdatany/finnhubimage/stock_logo/A.svg",
            "marketCapitalization": 37283.34858,
            "name": "Agilent Technologies Inc",
            "phone": "14083458296.0",
            "shareOutstanding": 296.041,
            "ticker": "A",
            "weburl": "https://www.agilent.com/",
        },
        "AA": {
            "country": "US",
            "currency": "USD",
            "exchange": "NEW YORK STOCK EXCHANGE, INC.",
            "finnhubIndustry": "Metals & Mining",
            "ipo": "2016-10-18",
            "logo": "https://static2.finnhub.io/file/publicdatany/finnhubimage/stock_logo/AA.svg",
            "marketCapitalization": 7058.89667292069,
            "name": "Alcoa Corp",
            "phone": "14123152900.0",
            "shareOutstanding": 179.925,
            "ticker": "AA",
            "weburl": "https://www.alcoa.com/global/en/home.asp",
        },
    }


@pytest.fixture
def companies_directory(companies):
    with TemporaryDirectory() as tmp_directory:
        for symbol, company in companies.items():
            filename = f"{symbol}.json"
            path = os.path.join(tmp_directory, filename)
            with open(path, mode="w+") as fp:
                json.dump(company, fp)

        yield tmp_directory


def test_load_all_data_from_json_to_dict(companies_directory, companies):
    result = dict(
        load_all_data(
            directory=companies_directory,
            from_format="json",
            to_format="dict",
        )
    )

    assert result == companies
