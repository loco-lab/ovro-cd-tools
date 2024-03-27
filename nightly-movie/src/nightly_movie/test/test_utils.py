from pathlib import Path

import pytest
from astropy import units
from astropy.time import TimeDelta

from nightly_movie import utils


@pytest.mark.parametrize(
    "time_window", [TimeDelta(5 * units.min), TimeDelta(10 * units.min)]
)
def test_grouping(time_window):
    filenames = [
        Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030016_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030026_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030437_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030447_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030457_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030507_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030517_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030527_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030938_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030948_13MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030958_13MHz.ms"),
    ]

    grouped_data = utils.group_files(filenames, time_window)

    if time_window == TimeDelta(5 * units.min):
        expected_groups = {
            2460392.6267534723: [
                Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030016_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030026_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030437_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030447_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030457_13MHz.ms"),
            ],
            2460392.6302372687: [
                Path("13MHz/2024-03-23/03/20240323_030507_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030517_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030527_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030938_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030948_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030958_13MHz.ms"),
            ],
        }
    else:
        expected_groups = {
            2460392.6284953705: [
                Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030016_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030026_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030437_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030447_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030457_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030507_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030517_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030527_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030938_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030948_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030958_13MHz.ms"),
            ]
        }

    assert grouped_data == expected_groups


def test_one_group():
    filenames = [
        Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
        Path("18MHz/2024-03-23/03/20240323_030006_18MHz.ms"),
        Path("23MHz/2024-03-23/03/20240323_030006_23MHz.ms"),
        Path("27MHz/2024-03-23/03/20240323_030006_27MHz.ms"),
        Path("32MHz/2024-03-23/03/20240323_030006_32MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030507_13MHz.ms"),
        Path("18MHz/2024-03-23/03/20240323_030507_18MHz.ms"),
        Path("23MHz/2024-03-23/03/20240323_030507_23MHz.ms"),
        Path("27MHz/2024-03-23/03/20240323_030507_27MHz.ms"),
        Path("32MHz/2024-03-23/03/20240323_030507_32MHz.ms"),
    ]

    grouped_data = utils.group_files(filenames, TimeDelta(5 * units.min))

    expected_groups = {
        2460392.6250694445: [
            Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
            Path("18MHz/2024-03-23/03/20240323_030006_18MHz.ms"),
            Path("23MHz/2024-03-23/03/20240323_030006_23MHz.ms"),
            Path("27MHz/2024-03-23/03/20240323_030006_27MHz.ms"),
            Path("32MHz/2024-03-23/03/20240323_030006_32MHz.ms"),
        ],
        2460392.6285532406: [
            Path("13MHz/2024-03-23/03/20240323_030507_13MHz.ms"),
            Path("18MHz/2024-03-23/03/20240323_030507_18MHz.ms"),
            Path("23MHz/2024-03-23/03/20240323_030507_23MHz.ms"),
            Path("27MHz/2024-03-23/03/20240323_030507_27MHz.ms"),
            Path("32MHz/2024-03-23/03/20240323_030507_32MHz.ms"),
        ],
    }

    assert grouped_data == expected_groups
