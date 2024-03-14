import json
import re
from datetime import datetime, timedelta, timezone
from typing import List

from ..interface import AggregateMonitorPoint, MonitorAggregator

DATA_RECORDER_REGEX = re.compile(r"\/mon\/(?P<dr>dr[a-z]*\d{0,4})\/.*")


class DataRecorderMonitor(MonitorAggregator):
    """Data Recorder interface to monitor point aggregation.

    Reads all datarecorders with the prefix /mon/dr in etcd and collects
    - pipeline_lag
    - rx_rate
    - summary: status

    """

    key_suffixes = ("/bifrost/pipeline_lag", "/bifrost/rx_rate", "/summary")

    field_mapping = {
        "pipeline_lag": "recorder_lag",
        "rx_rate": "recorder_rate",
        "summary": "recorder_status",
    }

    stale_timestamp = 120.0

    def aggregate_monitor_points(self) -> List[AggregateMonitorPoint]:
        points = {}

        for val, metadata in self.client.get_prefix("/mon/dr"):
            key = metadata.key.decode("utf-8")
            if not key.endswith(self.key_suffixes):
                continue

            val = json.loads(val)
            tagname = DATA_RECORDER_REGEX.match(key).groupdict()["dr"]

            # take the last bit off the key to get the influx entry name
            field_name = self.field_mapping[key.split("/")[-1]]
            recent = datetime.fromtimestamp(
                val["timestamp"], timezone.utc
            ) - datetime.now(timezone.utc) < timedelta(seconds=self.stale_timestamp)

            point = AggregateMonitorPoint(
                f"/mon/dr/summary/{tagname}",
                ("dr", tagname),
                {field_name: val["value"], f"{field_name}_recent": recent},
            )
            # input the points into the mapping dict
            # making use of the addition we defined for this
            # class to combine the stats together.
            if tagname in points:
                points[tagname].__iadd__(point, True)
            else:
                points[tagname] = point

        return list(points.values())
