from pathlib import Path

import numpy as np
import pytest
from astropy import units
from astropy.time import Time, TimeDelta

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
            Time("2024-03-23T03:02:31.50", format="isot"): [
                Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030016_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030026_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030437_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030447_13MHz.ms"),
                Path("13MHz/2024-03-23/03/20240323_030457_13MHz.ms"),
            ],
            Time("2024-03-23T03:07:32.500", format="isot"): [
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
            Time("2024-03-23T03:05:02.000", format="isot"): [
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

    for key1, key2 in zip(expected_groups, grouped_data):
        assert np.abs(key1 - key2) < TimeDelta(0.01, format="sec")
        assert expected_groups[key1] == grouped_data[key2]


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
        Time("2024-03-23T03:00:06.000", format="isot"): [
            Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
            Path("18MHz/2024-03-23/03/20240323_030006_18MHz.ms"),
            Path("23MHz/2024-03-23/03/20240323_030006_23MHz.ms"),
            Path("27MHz/2024-03-23/03/20240323_030006_27MHz.ms"),
            Path("32MHz/2024-03-23/03/20240323_030006_32MHz.ms"),
        ],
        Time("2024-03-23T03:05:07.000", format="isot"): [
            Path("13MHz/2024-03-23/03/20240323_030507_13MHz.ms"),
            Path("18MHz/2024-03-23/03/20240323_030507_18MHz.ms"),
            Path("23MHz/2024-03-23/03/20240323_030507_23MHz.ms"),
            Path("27MHz/2024-03-23/03/20240323_030507_27MHz.ms"),
            Path("32MHz/2024-03-23/03/20240323_030507_32MHz.ms"),
        ],
    }

    for key1, key2 in zip(expected_groups, grouped_data):
        assert np.abs(key1 - key2) < TimeDelta(0.01, format="sec")
        assert expected_groups[key1] == grouped_data[key2]


def test_copy_files(tmp_path):
    indir = tmp_path / "data.ms"
    indir.mkdir()

    outdir = tmp_path / "out"

    infile = indir / "test.txt"
    infile.write_text("Hello, World!")

    outfile = outdir / "data.ms"
    outdata = outfile / "test.txt"
    assert infile.exists()
    assert not outdata.exists()

    outfiles = utils.copy_files(indir, outdir)

    assert outfile.exists()
    assert outdata.exists()
    assert outfiles[0] == outfile

    assert outdata.read_text() == "Hello, World!"


def test_central_int():
    filenames = [
        Path("13MHz/2024-03-23/03/20240323_030006_13MHz.ms"),
        Path("18MHz/2024-03-23/03/20240323_030006_18MHz.ms"),
        Path("23MHz/2024-03-23/03/20240323_030006_23MHz.ms"),
        Path("27MHz/2024-03-23/03/20240323_030006_27MHz.ms"),
        Path("32MHz/2024-03-23/03/20240323_030006_32MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030036_13MHz.ms"),
        Path("18MHz/2024-03-23/03/20240323_030036_18MHz.ms"),
        Path("23MHz/2024-03-23/03/20240323_030036_23MHz.ms"),
        Path("27MHz/2024-03-23/03/20240323_030036_27MHz.ms"),
        Path("32MHz/2024-03-23/03/20240323_030036_32MHz.ms"),
        Path("13MHz/2024-03-23/03/20240323_030066_13MHz.ms"),
        Path("18MHz/2024-03-23/03/20240323_030066_18MHz.ms"),
        Path("23MHz/2024-03-23/03/20240323_030066_23MHz.ms"),
        Path("27MHz/2024-03-23/03/20240323_030066_27MHz.ms"),
        Path("32MHz/2024-03-23/03/20240323_030066_32MHz.ms"),
    ]
    expected = [
        Path("13MHz/2024-03-23/03/20240323_030036_13MHz.ms"),
        Path("18MHz/2024-03-23/03/20240323_030036_18MHz.ms"),
        Path("23MHz/2024-03-23/03/20240323_030036_23MHz.ms"),
        Path("27MHz/2024-03-23/03/20240323_030036_27MHz.ms"),
        Path("32MHz/2024-03-23/03/20240323_030036_32MHz.ms"),
    ]

    central_time = Time("2024-03-23T03:00:36", format="isot")

    returned = utils.get_central_integration(filenames, central_time)

    assert expected == returned
