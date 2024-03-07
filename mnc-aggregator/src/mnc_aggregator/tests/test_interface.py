import pytest

from mnc_aggregator import AggregateMonitorPoint


@pytest.mark.parametrize(
    ["point2", "error"],
    [
        (
            3,
            "of the same class",
        ),
        (
            AggregateMonitorPoint("/test2/", ("tag", "test"), foo="bar"),
            "must have the same etcd3 path",
        ),
        (
            AggregateMonitorPoint("/test/", ("tag", "test2"), foo="bar"),
            "must have the same tagname",
        ),
        (
            AggregateMonitorPoint("/test/", ("tag", "test"), foo="bar"),
            "must have the same timestamp",
        ),
    ],
)
def test_addition_errors(point2, error):
    point1 = AggregateMonitorPoint("/test/", ("tag", "test"), foo="bar")

    if isinstance(point2, AggregateMonitorPoint) and "timestamp" not in error:
        point1.timestamp = point2.timestamp

    with pytest.raises(ValueError, match=error):
        point1 + point2


def test_addition():
    point1 = AggregateMonitorPoint("/test/", ("tag", "test"), foo="bar")
    point2 = AggregateMonitorPoint("/test/", ("tag", "test"), bar="foo")
    # force the timestamps to match
    point2.timestamp = point1.timestamp

    point3 = point1 + point2

    assert "foo" in point3.fields
    assert "bar" in point3.fields


def test_inplace_additions():
    point1 = AggregateMonitorPoint("/test/", ("tag", "test"), foo="bar")
    point2 = AggregateMonitorPoint("/test/", ("tag", "test"), bar="foo")
    # force the timestamps to match
    point2.timestamp = point1.timestamp

    point1 += point2

    assert "foo" in point1.fields
    assert "bar" in point1.fields


def test_inplace_additions_different_timestamps():
    point1 = AggregateMonitorPoint("/test/", ("tag", "test"), foo="bar")
    point2 = AggregateMonitorPoint("/test/", ("tag", "test"), bar="foo")
    # force the timestamps to match

    point1.__iadd__(point2, True)
    print(f"{point1.fields=:}")

    assert "foo" in point1.fields
    assert "bar" in point1.fields
