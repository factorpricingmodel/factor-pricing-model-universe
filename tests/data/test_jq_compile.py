import json
from tempfile import NamedTemporaryFile

import pytest

from factor_pricing_model_universe.data import jq_compile


@pytest.fixture
def input_values():
    return [
        {
            "symbol": "A",
            "name": "Agilent Technologies Inc",
            "exchange": "NYSE",
            "assetType": ["Stock"],
            "ipoDate": "1999-11-18",
            "delistingDate": "null",
            "status": "Active",
        },
        {
            "symbol": "ZX",
            "name": "China Zenix Auto International Ltd",
            "exchange": "NYSE",
            "assetType": ["Equity"],
            "ipoDate": "2011-05-16",
            "delistingDate": "2018-06-14",
            "status": "Delisted",
        },
    ]


@pytest.fixture
def expected_results():
    return [
        {
            "symbol": "A",
            "valid_start_datetime": "1999-11-18",
            "valid_last_datetime": "null",
        },
        {
            "symbol": "ZX",
            "valid_start_datetime": "2011-05-16",
            "valid_last_datetime": "2018-06-14",
        },
    ]


@pytest.fixture
def input_values_filename(input_values):
    with NamedTemporaryFile() as f:
        with open(f.name, "w") as fp:
            json.dump(input_values, fp, indent=2)
        yield f.name


def test_jq_compile_validity_json(input_values, expected_results):
    result = jq_compile(
        json_input=input_values,
        pattern=".[] | { symbol: .symbol, valid_start_datetime: .ipoDate, valid_last_datetime: .delistingDate }",
    )

    assert result == expected_results


def test_jq_compile_validity_filename(input_values_filename, expected_results):
    result = jq_compile(
        json_filename=input_values_filename,
        pattern=".[] | { symbol: .symbol, valid_start_datetime: .ipoDate, valid_last_datetime: .delistingDate }",
    )

    assert result == expected_results


def test_jq_compile_to_list(input_values):
    result = jq_compile(
        json_input=input_values,
        pattern=".[] | .symbol ",
    )

    assert result == ["A", "ZX"]


def test_jq_compile_flatten_list(input_values):
    result = jq_compile(
        json_input=input_values,
        pattern="[.[] | .assetType[]] | sort | unique | .[]",
    )

    assert result == ["Equity", "Stock"]
