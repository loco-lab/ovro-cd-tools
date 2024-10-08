import json
from datetime import datetime, timedelta, timezone
from typing import List

from dateutil.parser import parse

from ..interface import AggregateMonitorPoint, MonitorAggregator


class SnapMonitor(MonitorAggregator):
    """The Snap (e.g. f-engine) interface for Monitor Point aggregation.

    Looks at all snaps with the prefix /mon/snap/ for snaps and takes statistcs from
    /mon/snap/<snapnum>/status and /mon/snap/<snapnum> and collects them into /mon/snap/summary/<snapnum>
    """

    stale_timestamp = 120.0

    def aggregate_monitor_points(self) -> List[AggregateMonitorPoint]:
        points = {}

        for val, metadata in self.client.get_prefix("/mon/snap/"):
            key = metadata.key.decode("utf-8")

            # tag will be the snap number
            snapnum = key.split("/")[3]

            if key.casefold().endswith("summary"):
                # ignore the outputs created by this function
                continue

            val = json.loads(val)

            if key.casefold().endswith("status"):
                recent = recent = parse(val["timestamp"]) - datetime.now(
                    timezone.utc
                ) < timedelta(seconds=self.stale_timestamp)
                point = AggregateMonitorPoint(
                    f"/mon/snap/summary/{snapnum}",
                    ("snap", snapnum),
                    {"status_ok": val["ok"], "status_recent": recent},
                )

            # look for keys ending in the snap number too
            elif key.casefold().endswith(snapnum):
                recent = recent = parse(val["timestamp"]) - datetime.now(
                    timezone.utc
                ) < timedelta(seconds=self.stale_timestamp)

                eth_gbps = None
                overflow_count = None
                clip_count = None

                stats = val.get("stats")
                if isinstance(stats, dict):
                    eth = stats.get("eth")
                    if isinstance(eth, dict):
                        eth_gbps = eth.get("gbps")

                    pfb = stats.get("pfb")
                    if isinstance(pfb, dict):
                        overflow_count = pfb.get("overflow_count")

                    eq = stats.get("eq")
                    if isinstance(eq, dict):
                        clip_count = eq.get("clip_count")

                point = AggregateMonitorPoint(
                    f"/mon/snap/summary/{snapnum}",
                    ("snap", snapnum),
                    {
                        "eth_gbps": eth_gbps,
                        "eth_recent": recent,
                        "clip_count": clip_count,
                        "overflow_count": overflow_count,
                    },
                )

            else:
                # ignore any other keys for now
                continue

            # input the points into the mapping dict
            # making use of the addition we defined for this
            # class to combine the stats together.
            if snapnum in points:
                points[snapnum].__iadd__(point, True)
            else:
                points[snapnum] = point

        return list(points.values())
