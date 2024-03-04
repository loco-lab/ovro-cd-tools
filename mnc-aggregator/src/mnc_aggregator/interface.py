import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TypeAlias

import etcd3
from mnc.common import ETCD_HOST, ETCD_PORT

influxtag: TypeAlias = tuple[str, str]


@dataclass
class AggregateMonitorPoint:
    timestamp: datetime
    path: str
    tagname: influxtag
    fields: dict

    def __init__(
        self, path: str, tagname: influxtag, fields: dict | None = None, **kwargs
    ):
        """An aggregated monitor point meant to be summary of a subsystem.

        Arguments
        ---------
        path: (str, str)
            A tuple pair of strings containing the name of the influx tag and its value.
            e.g. ("pipelinehost", "lxdlwagpu02-1")
        fields: dict
            A dictionary of key, value pairs for all fields in this datapoint.
            The key, value pairs may also be passed as additional kwargs to init.
        """
        self.timestamp = datetime.now(timezone.utc)
        self.path = path
        self.tagname = tagname
        if fields is None:
            fields = {}
        self.fields = fields | kwargs

    def to_json(self) -> str:
        return json.dumps(
            {
                "time": self.timestamp.timestamp(),
                self.tagname[0]: self.tagname[1],
                **self.fields,
            }
        )


class MonitorAggregator(ABC):
    """The Absract interface used to aggregate distributed
    monitor points into as summary of set of summary points.
    """

    client: etcd3.Etcd3Client

    def __init__(self) -> None:
        super().__init__()

        self.client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)

    @abstractmethod
    def aggregate_monitor_points(self) -> list[AggregateMonitorPoint]:
        """
        Turns the distributed points associated with the given class into a aggregated point.

        This function is used as a generic interface for multiple MonitorPoint aggregators.

        Returns
        -------
        list of AggregateMonitorPoints associated with this subsystem.
        """
        pass

    def write_monitor_points(self):
        """Calls self.aggregate_monitor_points and writes the (path, MonitorPoint) pairs to etcd3."""

        for point in self.aggregate_monitor_points():
            self.client.put(point.path, point.to_json())
