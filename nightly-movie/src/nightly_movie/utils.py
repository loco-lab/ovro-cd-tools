# -*- mode: python; coding: utf-8 -*-
# Copyright (c) 2024, Owens Valley Radio Observatory Long Wavelength Array
# All rights reserved.

# these are necessary to get the images to not need
# a gui backend
import matplotlib  # noqa:

matplotlib.use("Agg")  # noqa:

import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Tuple

import numpy as np
from astropy import units
from astropy.coordinates import AltAz, Angle, EarthLocation, SkyCoord, get_body
from astropy.io import fits
from astropy.time import Time, TimeDelta
from astropy.wcs import WCS
from casatools import componentlist
from matplotlib import pyplot as plt
from matplotlib.colors import Normalize

TIME_REGEX = re.compile(r".*(?P<date>\d{8})_(?P<hms>\d{6})_(?P<band>\d{2}MHz).ms")
NAME_REGEX = re.compile(r".*\d{8}_\d{6}_(?P<name>[a-zA-Z]*)-.*\.fits$")

OVRO_LOCATION = EarthLocation.from_geodetic(
    lat=Angle("37.239777271d"),
    lon=Angle("-118.281666695d"),
    height=1183.48,
)

ATEAM_SOURCES = {
    name: SkyCoord.from_name(name)
    for name in [
        "Cas A",
        "Cyg A",
        "Pic A",
        "Her A",
        "For A",
        "Cen A",
        "Hydra A",
        "Sgr A",
        "Pup A",
        "Tau A",
        "Vir A",
    ]
}


def partition_files(filenames: List[Path]) -> Tuple[List[Path], List[Path]]:
    """Parition a list of OVRO-LWA files into a high and low bands.

    Highband is defined as freq > 40MHz
    Lowband is defined as freq < 40MHz

    Parameters
    ----------
    filenames : List[Path]
        The list of filenames to partition.

    Returns
    -------
    lowband
        All lowband filenames
    highband
        All highbnd filenames
    """
    lowband_freqs = ["18MHz.ms", "23MHz.ms", "27MHz.ms", "32MHz.ms", "36MHz.ms"]
    lowband = []
    highband = []
    for fname in filenames:
        if fname.name.split("_")[-1].strip() in lowband_freqs:
            lowband.append(fname)
        else:
            highband.append(fname)

    return lowband, highband


def group_files(filenames: List[Path], time_window: TimeDelta = None) -> dict:
    """Group all the files in the input directory into 5 minute windows.


    Paramters
    ----------
    path : list[pathlib.Path]
        A list of all data file paths from the OVRO-LWA.
    time_window : TimeDelta
        The window length to group the time files into.
        Defaults to 5 minutes

    Returns
    --------
    dict
        The files found in the input path grouped into time_window windows.
        The keys are the center time in this window.
        The values are the file names in this window.
    """

    times_to_fnames = dict()
    if time_window is None:
        time_window = TimeDelta(5 * units.min)

    for fname in filenames:
        regex_match = TIME_REGEX.match(fname.name)
        groups = regex_match.groupdict()
        date, hms = groups["date"], groups["hms"]
        timeval = Time(
            f"{date[:4]}-{date[4:6]}-{date[6:8]}T{hms[:2]}:{hms[2:4]}:{hms[4:6]}",
            format="isot",
        )
        if timeval not in times_to_fnames:
            times_to_fnames[timeval] = [fname]

        else:
            times_to_fnames[timeval] += [fname]

    time_array = Time(sorted(times_to_fnames.keys()))
    grouped_data = dict()
    group_start_ind = 0

    while group_start_ind < len(time_array):
        group_inds = (
            np.nonzero(
                time_array[group_start_ind:] - time_array[group_start_ind]
                <= time_window
            )[0]
            + group_start_ind
        )
        central_time = np.mean(time_array[group_inds])
        grouped_data[central_time] = []
        for ind in group_inds:
            grouped_data[central_time] += times_to_fnames[time_array[ind]]

        group_start_ind = group_inds[-1] + 1

    return grouped_data


def get_central_integration(filenames: List[Path], central_time: Time) -> List[Path]:
    """Subselect a list of data files to get the integrations closes to the central time.

    If a subband is missing from the integration it is ignored.


    Paramters
    ---------
    filename : List[Path]
        List of files in this integration to downselect.
    central_time : Time
        The timestamp of the central time of the window


    Returns
    -------
    List[Path]
        All files whose timestamp is closest to the desired time
    """

    all_times = []
    for fname in filenames:
        regex_match = TIME_REGEX.match(fname.name)
        groups = regex_match.groupdict()
        date, hms = groups["date"], groups["hms"]
        timeval = Time(
            f"{date[:4]}-{date[4:6]}-{date[6:8]}T{hms[:2]}:{hms[2:4]}:{hms[4:6]}",
            format="isot",
        )
        if timeval not in all_times:
            all_times.append(timeval)
    all_times = Time(all_times)

    closest_integration = all_times[(np.abs(all_times - central_time)).argmin()]
    time_str = closest_integration.strftime("%Y%m%d_%H%M%S")

    return [fname for fname in filenames if time_str in fname.name]


def copy_files(filenames: List[Path], outdir: Path) -> List[Path]:
    """Copy All files to the outdir directory.


    Parameters
    -----------
    filenames : List[Path] | Path
        Files to copy
    outdir : Path
        Directory to copy the files to

    Returns
    -------
    List[Path]
        New filename locations
    """
    if isinstance(filenames, Path):
        filenames = [filenames]

    outnames = [outdir / fname.name for fname in filenames]

    for inname, outname in zip(filenames, outnames):
        subprocess.check_output(f"rsync -rz {inname}/ {outname}", shell=True)

    return outnames


def generate_componentlist(componentlist_name: Path):
    src_list = [
        {
            "label": "CasA",
            "flux": 16530,
            "alpha": -0.72,
            "ref_freq": "80.0MHz",  # MHz
            "position": "J2000 23h23m24s +58d48m54s",
        },
        {
            "label": "CygA",
            "flux": 16300,
            "alpha": -0.58,
            "ref_freq": "80.00MHz",  # MHz
            "position": "J2000 19h59m28.35663s +40d44m02.0970s",
        },
        {
            "label": "TauA",
            "flux": 1770,
            "alpha": -0.27,
            "ref_freq": "80.00MHz",  # MHz
            "position": "J2000 05h34m31.94s +22d00m52.2s",
        },
        {
            "label": "VirA",
            "flux": 2400,
            "alpha": -0.86,
            "ref_freq": "80.00MHz",  # MHz
            "position": "J2000 12h30m49.42338s +12d23m28.0439s",
        },
    ]

    cl = componentlist()
    cl.done()
    for src in src_list:
        cl.addcomponent(
            flux=src["flux"],
            dir=src["position"],
            index=src["alpha"],
            spectrumtype="spectral index",
            freq=src["ref_freq"],
            label=src["label"],
        )
    cl.rename(componentlist_name)
    cl.done()


def plot_snapshot(filename: List[Path], outname: str):
    """Plot the input snapshot with WCS and timestamp"""

    if "highband" in outname:
        norm = Normalize(vmin=-5, vmax=50)
    else:
        norm = Normalize(vmin=-5, vmax=250)

    hdu = fits.open(filename[0])[0]
    central_freq = hdu.header["CRVAL3"] / 1e6
    hdu.header["TIMESYS"] = "utc"
    hdu.header["RADESYSa"] = "ICRS"
    wcs = WCS(hdu.header).slice(np.s_[0, 0])  # .dropaxis(3).dropaxis(2)

    obstime = Time(
        hdu.header["DATE-OBS"],
        format="isot",
        scale="utc",
        location=OVRO_LOCATION,
    )
    ovro_altaz = AltAz(obstime=obstime, location=OVRO_LOCATION)

    fig, axes = plt.subplots(
        1,
        2,
        dpi=200,
        figsize=(6.4, 3.5),
        sharex=True,
        sharey=True,
        subplot_kw={"projection": wcs},
    )

    axes[0].imshow(hdu.data[0, 0, :, :], norm=norm, origin="lower", aspect="equal")
    axes[0].set_title("I")

    hdu = fits.open(filename[1])[0]
    axes[1].imshow(hdu.data[0, 0, :, :] * 10, norm=norm, origin="lower")
    axes[1].set_title("V * 10")

    for body in ["Sun", "Moon", "Jupiter"]:
        body_coord = get_body(body, obstime).transform_to(ovro_altaz)
        # WSClean writes things in FK5 which is barycentric despite the fact we're observing
        # from eath so the FK5 will project to the wrong spot.
        # This is worked around by ignoring their distance

        body_coord = SkyCoord(alt=body_coord.alt, az=body_coord.az, frame=ovro_altaz)
        pixel_coord = wcs.world_to_pixel(body_coord)

        for ax in axes:
            ax.plot_coord(
                body_coord,
                marker="o",
                ms=3,
                markerfacecolor="none",
                markeredgewidth=0.5,
                color="white",
            )
            ax.annotate(
                body.capitalize(),
                pixel_coord,
                color="white",
                fontsize="xx-small",
                horizontalalignment="right",
                verticalalignment="bottom",
            )

    for source, coord in ATEAM_SOURCES.items():
        pixel_coords = wcs.world_to_pixel(coord)
        for ax in axes:
            ax.plot_coord(
                coord,
                marker="o",
                ms=3,
                markerfacecolor="none",
                markeredgewidth=0.5,
                color="white",
            )
            ax.annotate(
                source,
                pixel_coords,
                color="white",
                fontsize="xx-small",
                horizontalalignment="right",
                verticalalignment="bottom",
            )

    fig.text(
        0.5,
        0.075,
        hdu.header["DATE-OBS"] + " UTC",
        horizontalalignment="center",
        backgroundcolor="k",
        color="w",
        verticalalignment="center",
    )
    for ax in axes:
        xmax = ax.get_xlim()[1]
        ymax = ax.get_ylim()[1]
        ax.text(
            xmax // 2,
            -150,
            "S",
            horizontalalignment="center",
            verticalalignment="center",
        )
        ax.text(
            -150,
            ymax // 2,
            "E",
            horizontalalignment="center",
            verticalalignment="center",
        )
        ax.text(
            xmax + 150,
            ymax // 2,
            "W",
            horizontalalignment="center",
            verticalalignment="center",
        )

    bandname = NAME_REGEX.match(filename[0]).group("name")
    plt.suptitle(hdu.header["TELESCOP"] + f"\n{bandname} {central_freq:.3f}MHz")
    plt.savefig(outname)
    plt.close()


def check_for_bcal(date_str: str, subbands: List[str]):
    bcal_stub = Path("/lustre/celery/bcal/")
    date_name = "".join(date_str.split("-")) + ".bcal"
    for band in subbands:
        bcal_name = bcal_stub / band / date_name
        if not bcal_name.exists():
            print(
                f"No Bandpass calibration found for {band}. Performing naive calibration."
            )
            return False

    return True


def get_bcal(
    filename: str,
    output_prefix: Path,
):
    match = TIME_REGEX.match(filename)
    subband = match.group("band")
    date_name = match.group("date") + ".bcal"

    return str(output_prefix / subband / date_name)
