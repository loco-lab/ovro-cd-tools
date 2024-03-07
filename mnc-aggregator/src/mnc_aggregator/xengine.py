import json
from datetime import datetime, timedelta, timezone

from .interface import AggregateMonitorPoint, MonitorAggregator


class XEngingeMonitor(MonitorAggregator):
    hostnames = [
        f"lxdlwagpu{gpu:0>2d}-{pipeline:0>1d}"
        for gpu in range(1, 9)
        for pipeline in range(4)
    ]

    stale_timestamp = 120.0

    def aggregate_monitor_points(self) -> list[AggregateMonitorPoint]:
        monitor_points = []

        for hostname in self.hostnames:
            tagname = ("pipelinehost", hostname)
            path = f"/mon/x/summary/{hostname}"
            gpu, pipeline = hostname.split("-")

            capture_stats = json.loads(
                self.client.get(
                    f"/mon/corr/x/{gpu}/pipeline/{pipeline}/udp_verbs_capture/0"
                )[0]
            )

            corr_stats = json.loads(
                self.client.get(f"/mon/corr/x/{gpu}/pipeline/{pipeline}/Corr/0")[0]
            )

            copy_stats = json.loads(
                self.client.get(f"/mon/corr/x/{gpu}/pipeline/{pipeline}/Copy/0")[0]
            )

            if "stats" in corr_stats and isinstance(corr_stats["stats"], dict):
                state_dict = corr_stats["stats"]
                if "state" in state_dict and isinstance(state_dict["state"], str):
                    corr_running = (
                        corr_stats["stats"]["state"].casefold() == "running".casefold()
                    )
                else:
                    corr_running = False
            else:
                corr_running = False

            corr_timestamp = corr_stats["time"]
            corr_recent = datetime.fromtimestamp(
                corr_timestamp, timezone.utc
            ) - datetime.now(timezone.utc) < timedelta(seconds=self.stale_timestamp)
            corr_running = corr_running & corr_recent

            capture_recent = datetime.fromtimestamp(
                capture_stats["timestamp"], timezone.utc
            ) - datetime.now(timezone.utc) < timedelta(seconds=self.stale_timestamp)

            copy_recent = datetime.fromtimestamp(
                copy_stats["timestamp"], timezone.utc
            ) - datetime.now(timezone.utc) < timedelta(seconds=self.stale_timestamp)

            fields = {
                "capture_recent": capture_recent,
                "capture_rate": capture_stats["gbps"],
                "corr_recent": corr_recent,
                "corr_rate": corr_stats["gbps"],
                "corr_is_running": corr_running,
                "copy_recent": copy_recent,
                "copy_rate": copy_stats["gbps"],
            }
            monitor_points.append(AggregateMonitorPoint(path, tagname, fields))

        return monitor_points
