import argparse

from astropy.time import Time, TimeDelta
from mnc.influx import influx as influx_client

from db_inspector.dataframe import Subsystem, print_stats_for_subsystem


class DefaultRaw(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawTextHelpFormatter,
):
    pass


def main():
    """Command line script to automatically group data and perform imaging every night."""
    parser = argparse.ArgumentParser(
        prog="influx-inspect",
        description=(
            "Query OVRO-LWA influxdb database for go/no-go satistics on the given subsystem.\n"
            "Will compute the MEAN value of the statistic for each day in the range [start-date, end_date) or [start_date, start_date + delta)"
        ),
        formatter_class=DefaultRaw,
    )

    parser.add_argument(
        "subsystem",
        type=Subsystem,
        choices=list(Subsystem),
        help=(
            "OVRO-LWA subsystem for which statistics will be queried.\n"
            "This tool will print the following statistics depending on which subsystem is chosen:\n"
            " - data-recorders: recorder_rate\n"
            " - x-engines: capture_rate\n"
            " - snaps: eth_gbps"
        ),
    )

    parser.add_argument(
        "--start-date",
        "-s",
        type=lambda x: Time(x, format="iso", scale="utc"),
        help=(
            "The starting date for the influx query.\n"
            "Expected to be in the form YYYY-MM-DD"
        ),
        dest="start_date",
    )

    time_delta_group = parser.add_mutually_exclusive_group()

    time_delta_group.add_argument(
        "--delta",
        "-d",
        type=lambda x: TimeDelta(x, format="jd"),
        default=TimeDelta(1, format="jd"),
        help=(
            "The number of days for which to query the given subsystem.\n"
            "This is mutually exclusive with end_date."
        ),
    )

    time_delta_group.add_argument(
        "--end-date",
        "-e",
        type=lambda x: Time(x, format="iso", scale="utc"),
        dest="end_date",
        help=(
            "The ending date to query the DB.\n"
            "Expected to be in the form YYYY-MM-DD.\n"
            "Mutually exclusive with delta"
        ),
    )
    args = parser.parse_args()

    if args.end_date is None:
        end_date = args.start_date + args.delta
    else:
        end_date = args.end_date

    print_stats_for_subsystem(args.subsystem, influx_client, args.start_date, end_date)
