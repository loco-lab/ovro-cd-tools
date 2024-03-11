import argparse
import sys
import time
import traceback

from mnc_aggregator.interface import MonitorAggregator

from .subsystems import DataRecorderMonitor, SnapMonitor, XEngineMonitor


class DefaultRaw(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawTextHelpFormatter,
):
    pass


# This is the list of Subsystems for which MonitorAggregator have been defined.
# These will be iterated over and the summaries written to etcd.
# If a new subsystem is implemented it only needs to be added to this list.
MonitorClasses: list[MonitorAggregator] = [
    SnapMonitor,
    DataRecorderMonitor,
    XEngineMonitor,
]


def main():
    """Command line script used to periodically write summary points to etcd."""
    parser = argparse.ArgumentParser(
        prog="mnc_aggregator",
        description=(
            "Aggregates statistics on various classes of etcd MonitorPoints into a summary point."
            "Writes summary points to /mon/<sub-system>/summary/<tagname> for each independent measurement "
            "in subsystems with interfaces defined in the mnc-aggregator python package."
        ),
        formatter_class=DefaultRaw,
    )

    parser.add_argument(
        "--interval",
        "-i",
        required=False,
        type=float,
        default=60.0,
        help=(
            "The interval in seconds at which to poll all subsystems for new statistics."
            "This script will wait this long in between each aggregation run."
        ),
    )

    args = parser.parse_args()

    try:
        while True:
            time.sleep(args.interval)

            for monitor_class in MonitorClasses:
                try:
                    instance = monitor_class()
                    instance.write_monitor_points()
                except Exception:
                    print(
                        f"{time.asctime()} -- error making summary for {monitor_class}",
                        file=sys.stderr,
                    )
                    traceback.print_exc(file=sys.stderr)
    except KeyboardInterrupt:
        print("Exiting mnc_aggregator summary.")
        sys.exit()
