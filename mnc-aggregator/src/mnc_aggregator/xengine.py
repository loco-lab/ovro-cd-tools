import json

from .interface import AggregateMonitorPoint, MonitorAggregator


class XEngingeMonitor(MonitorAggregator):
    hostnames = [
        f"lxdlwagpu{gpu:0>2d}-{pipeline:0>1d}"
        for gpu in range(1, 9)
        for pipeline in range(4)
    ]

    def aggregate_monitor_points(self) -> list[AggregateMonitorPoint]:
        monitor_points = []

        for hostname in self.hostnames:
            tagname = ("pipelinehost", hostname)
            path = f"mon/x/summary/{hostname}"
            gpu, pipeline = hostname.split("-")

            capture_stats = json.loads(
                self.client.get(
                    f"/mon/corr/x/{gpu}/pipeline/{pipeline}/udp_verbs_capture/0"
                )[0]
            )

            corr_stats = json.loads(
                self.client.get(f"/mon/corr/x/{gpu}/pipeline/{pipeline}/Corr/0")[0]
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

            fields = {
                "capture_timestamp": capture_stats["time"],
                "capture_rate": capture_stats["gbps"],
                "corr_timestamp": corr_stats["time"],
                "corr_rate": corr_stats["gbps"],
                "corr_is_running": corr_running,
            }
            monitor_points.append(AggregateMonitorPoint(path, tagname, fields))

        return monitor_points
