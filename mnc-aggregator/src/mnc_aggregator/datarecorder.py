import json
import re

from .interface import AggregateMonitorPoint, MonitorAggregator

DATA_RECORDER_REGEX = re.compile(r"\/mon\/(?P<dr>dr[a-z]*\d{0,4})\/.*")


class DataRecorderMonitor(MonitorAggregator):
    key_suffixes = ("/bifrost/pipeline_lag", "/bifrost/rx_rate", "/summary")

    field_mapping = {
        "pipeline_lag": "recorder_lag",
        "rx_rate": "recorder_rate",
        "summary": "recorder_status",
    }

    def aggregate_monitor_points(self) -> list[AggregateMonitorPoint]:
        points = {}

        for val, metadata in self.client.get_range("/mon/dr00", "/mon/dr99"):
            key = metadata.key.decode("utf-8")
            if not key.endswith(self.key_suffixes):
                continue

            val = json.loads(val)
            tagname = DATA_RECORDER_REGEX.match(key).groupdict()["dr"]

            # take the last bit off the key to get the influx entry name
            field_name = self.field_mapping[key.split("/")[-1]]
            point = AggregateMonitorPoint(
                f"/mon/dr/summary/{tagname}",
                ("dr", tagname),
                {field_name: val["value"], "recorder_timestamp": val["timestamp"]},
            )
            if tagname in points:
                points[tagname].__iadd__(point, True)
            else:
                points[tagname] = point

        for prefix in ["/mon/drvf", "/mon/drvs", "/mon/drt"]:
            for val, metadata in self.client.get_prefix(prefix):
                key = metadata.key.decode("utf-8")
                if not key.endswith(self.key_suffixes):
                    continue

                val = json.loads(val)
                tagname = DATA_RECORDER_REGEX.match(key).groupdict()["dr"]

                # take the last bit off the key to get the influx entry name
                field_name = self.field_mapping[key.split("/")[-1]]
                point = AggregateMonitorPoint(
                    f"/mon/dr/summary/{tagname}",
                    ("dr", tagname),
                    {field_name: val["value"], "recorder_timestamp": val["timestamp"]},
                )
                if tagname in points:
                    points[tagname].__iadd__(point, True)
                else:
                    points[tagname] = point

        return points
