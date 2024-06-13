# SPDX-FileCopyrightText: 2024-present OVRO-LWA
#
# SPDX-License-Identifier: MIT
import pandas as pd
import pytest
from db_inspector.dataframe import (
    aggregate_dataframe,
    get_sortable_type,
    natural_keys,
)


@pytest.mark.parametrize(
    ("expected_type", "val"),
    [("U100", "hello"), ("U100", ""), ("U100", b"test"), (int, 3), (float, 2.2)],
)
def test_sortable_keys(expected_type, val):
    assert expected_type == get_sortable_type(val)


def test_natural_keys():
    words = ["test1", "test10", "test11", "test2", "test20"]
    words.sort(key=natural_keys)
    assert ["test1", "test2", "test10", "test11", "test20"] == words


def test_aggregate_df(capsys):
    df = pd.DataFrame(
        [
            ("dr1", 1),
            ("dr2", 2),
            ("dr3", 3),
            ("dr4", 4),
            ("dr1", 3),
            ("dr2", 7),
            ("dr3", 8),
            ("dr4", 0),
        ],
        columns=["tag", "val"],
        index=pd.date_range("20240501", periods=8, freq="6H"),
    )

    _ = capsys.readouterr()
    aggregate_dataframe(df)
    captured = capsys.readouterr()
    expected_out = [
        "          \t    dr1         dr2         dr3         dr4     ",
        "2024-05-01\t     1.0         2.0         3.0         4.0    ",
        "2024-05-02\t     3.0         7.0         8.0         0.0    ",
    ]
    expected_out = "\n".join(expected_out) + "\n"
    assert expected_out == captured.out
