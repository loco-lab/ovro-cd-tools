import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Self, TypeAlias

import etcd3
from mnc.common import ETCD_HOST, ETCD_PORT

influxtag: TypeAlias = tuple[str, str]


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
        path: str
            The new key path for the AggregateMonitorPoint to be placed into etcd3
        tagname: (str, str)
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

    def __add__(
        self: Self, other: Self, overwrite_timestamp=False, inplace=False
    ) -> Self:
        """Add two Monitor Points by joining their fields together.

        Any duplicated fields will be overwritten with data from other.

        Parameters
        ----------
        other: AggregateMonitorPoint
            The other point to add to this one.
        overwrite_timestamp: bool
            By default, all metadata must match to combine two monitor points.
            However by setting this to True, the timestamp on other will be ignored
            and its fields added to those in self.
        inplace: bool
            When true, updates the fields in self without creating a new object.


        """
        if self.__class__ != other.__class__:
            raise ValueError(
                "Only AggregeateMonitorPoints of the same class may be added together."
            )
        if not overwrite_timestamp and self.timestamp != other.timestamp:
            raise ValueError(
                "AggregateMonitorPoints must have the same timestamp to add together."
            )
        if self.tagname != other.tagname:
            raise ValueError(
                "AggregateMonitorPoints must have the same tagname to add together."
            )
        if self.path != other.path:
            raise ValueError(
                "AggregateMonitorPoints must have the same etcd3 path to add together."
            )

        fields = self.fields | other.fields

        if inplace:
            out = self
        else:
            out = AggregateMonitorPoint(self.path, self.tagname)
            out.timestamp = self.timestamp

        out.fields = fields
        if not inplace:
            return out

    def __iadd__(self: Self, other: Self, overwrite_timestamp=False) -> Self:
        self.__add__(other, overwrite_timestamp, inplace=True)
        return self

    def to_json(self) -> str:
        return json.dumps(
            {
                "time": self.timestamp.isoformat(),
                self.tagname[0]: self.tagname[1],
                **self.fields,
            }
        )


class MonitorAggregator(ABC):
    """The Absract interface used to aggregate distributed monitor points into as summary of set of summary points."""

    client: etcd3.Etcd3Client

    def __init__(self) -> None:
        super().__init__()

        # allow big message lengths from the server
        # there are sometimes lots of keys (like from the datarecorders)
        # this allows us to receive all key, value pairs
        self.client = etcd3.client(
            host=ETCD_HOST,
            port=ETCD_PORT,
            grpc_options=[
                ("grpc.max_receive_message_length", -1),
                ("grpc.max_send_message_length", -1),
            ],
        )

    @abstractmethod
    def aggregate_monitor_points(self) -> list[AggregateMonitorPoint]:
        """
        Turns the distributed points associated with the given class into a aggregated point.

        This function is used as a generic interface for multiple MonitorPoint aggregators.
        When defining new subsystems to interfaces with this is the only method which needs to be overwritten.


        Returns
        -------
        list of AggregateMonitorPoints associated with this subsystem.
        """
        pass

    def write_monitor_points(self):
        """Calls self.aggregate_monitor_points and writes the (path, MonitorPoint) pairs to etcd3."""

        for point in self.aggregate_monitor_points():
            self.client.put(point.path, point.to_json())
