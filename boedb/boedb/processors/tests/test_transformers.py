import pytest

from boedb.processors.transformers import extract_keys_with_metadata


def test_extract_keys_with_metadata_simple_item():
    data = {
        "item": {
            "data": "data1",
        }
    }

    item = list(extract_keys_with_metadata(data, "item"))
    assert item == [({"data": "data1"}, {})]


def test_extract_keys_with_metadata_keyed_item():
    data = {
        "section": {
            "@attr1": "value1",
            "department": {
                "@attr2": "value2",
                "item": {
                    "data": "data1",
                },
            },
        }
    }

    item = list(extract_keys_with_metadata(data, "item"))
    assert item == [
        (
            {"data": "data1"},
            {"section": {"@attr1": "value1"}, "department": {"@attr2": "value2"}},
        )
    ]


def test_extract_keys_with_metadata_simple_list_item():
    data = {
        "item": [
            {
                "data": "data1",
            },
            {
                "data": "data2",
            },
        ]
    }

    item = list(extract_keys_with_metadata(data, "item"))
    assert item == [
        ({"data": "data1"}, {}),
        ({"data": "data2"}, {}),
    ]


def test_extract_keys_with_metadata_keyed_list_item():
    data = {
        "section": {
            "@attr1": "value1",
            "item": [
                {
                    "data": "data1",
                },
                {
                    "data": "data2",
                },
            ],
        }
    }

    item = list(extract_keys_with_metadata(data, "item"))
    assert item == [
        ({"data": "data1"}, {"section": {"@attr1": "value1"}}),
        ({"data": "data2"}, {"section": {"@attr1": "value1"}}),
    ]
