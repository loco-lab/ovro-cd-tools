# SPDX-FileCopyrightText: 2024-present OVRO-LWA
#
# SPDX-License-Identifier: MIT

import re
from enum import Enum

import numpy as np
from astropy.time import Time
from influxdb import DataFrameClient
from pandas import DataFrame


class Subsystem(Enum):
    DataRecorders = 1
    XEngines = 2
    Snaps = 3


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    """
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    """
    return [atoi(c) for c in re.split(r"(\d+)", text)]


def get_sortable_type(val):
    if isinstance(val, (str, bytes)):
        return "U100"
    else:
        return type(val)


def aggregate_dataframe(frame: DataFrame):
    """Sort input dataframe by date and tag and print to terminal."""

    # floor of index groups up the time by the day.
    mean = frame.groupby([frame.index.floor("D"), frame["tag"]]).mean()

    all_tags = mean.index.get_level_values(1).unique()

    # sort the tags in a human readable format
    # this ensures we don't get things like
    # item1, item10, item2....
    # basically sorts by (text, integer, text) after splitting items
    tag_sort_indices = np.argsort(
        np.asarray(
            [tuple(natural_keys(x)) for x in all_tags],
            dtype=[
                (str(i), get_sortable_type(x))
                for i, x in enumerate(natural_keys(all_tags[0]))
            ],
        )
    )

    all_tags = all_tags[tag_sort_indices]

    all_times = mean.index.get_level_values(0)
    unique_times = all_times.unique()

    output_str = f"{ '':>10}\t" + "".join(f"{tag:^12}" for tag in all_tags) + "\n"

    for time in unique_times:
        output_str += f"{time.strftime('%Y-%m-%d'):}\t"
        df_time = mean[all_times == time]
        selected_tags = df_time.index.get_level_values(1)

        for tag in all_tags:
            if tag in selected_tags:
                # print the data
                output_str += f"{df_time.loc[(time, tag)].values.item(0):^ 12.1f}"
            else:
                # skip
                output_str += f"{ '':>12} "

        output_str += "\n"

    print(output_str, end="", flush=True)


def query_subsystem(
    system: Subsystem, influx_client: DataFrameClient, start_time: Time, end_time: Time
):
    if system == Subsystem.DataRecorders:
        query = (
            f'SELECT "recorder_rate",dr as "tag" FROM "drmon" WHERE '
            f'"time" >= {int(start_time.unix*1000)}ms and time < {int(end_time.unix*1000)}ms and "recorder_rate_recent"=True'
        )
        return influx_client.query(query)["drmon"]

    elif system == Subsystem.XEngine:
        query = (
            f'SELECT "capture_rate",pipelinehost as "tag" FROM "xengmon" WHERE '
            f'"time" >= {int(start_time.unix*1000)}ms and time < {int(end_time.unix*1000)}ms and "capture_recent"=True'
        )
        return influx_client.query(query)["xengmon"]
    elif system == Subsystem.Snaps:
        query = (
            f'SELECT "eth_gbps",snap as "tag" FROM "snapmon" WHERE '
            f'"time" >= {int(start_time.unix*1000)}ms and time < {int(end_time.unix*1000)}ms and "eth_recent"=True'
        )
        return influx_client.query(query)["snapmon"]


def print_stats_for_subsystem(
    system: Subsystem, influx_client: DataFrameClient, start_time: Time, end_time: Time
):
    dataframe = query_subsystem(system, influx_client, start_time, end_time)
    aggregate_dataframe(dataframe)
