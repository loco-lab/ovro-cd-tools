import json
from datetime import datetime, timedelta, timezone

from .interface import AggregateMonitorPoint, MonitorAggregator


class SnapMonitor(MonitorAggregator):
    stale_timestamp = 120.0

    def aggregate_monitor_points(self) -> list[AggregateMonitorPoint]:
        points = {}

        for val, metadata in self.client.get_prefix("/mon/snap/"):
            key = metadata.key.decode("utf-8")

            # tag will be the snap number
            tagname = f"snap{key.split('/')[2]}"

            if key.casefold().endswith("summary"):
                # ignore the outputs created by this function
                continue

            val = json.loads(val)

            recent = recent = datetime.fromtimestamp(
                val["timestamp"], timezone.utc
            ) - datetime.now(timezone.utc) < timedelta(seconds=self.stale_timestamp)

            if key.casefold().endswith("status"):
                point = AggregateMonitorPoint(
                    f"/mon/snap/summary/{tagname}",
                    ("snap", tagname),
                    {"status_ok": val["ok"], "status_recent": recent},
                )

            # look for keys ending in the snap number too
            elif key.casefold().endswith(tagname[-2:]):
                eth_gbps = None
                stats = val.get("stats")
                if isinstance(stats, dict):
                    eth = stats.get("eth")
                    if isinstance(eth, dict):
                        eth_gbps = eth.get("gbps")

                point = AggregateMonitorPoint(
                    f"/mon/snap/summary/{tagname}",
                    ("snap", tagname),
                    {"eth_gbps": eth_gbps, "eth_recent": recent},
                )
            else:
                # ignore any other keys for now
                continue

            # input the points into the mapping dict
            # making use of the addition we defined for this
            # class to combine the stats together.
            if tagname in points:
                points[tagname].__iadd__(point, True)
            else:
                points[tagname] = point

        return list(points.values())
