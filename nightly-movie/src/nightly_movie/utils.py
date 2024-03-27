# -*- mode: python; coding: utf-8 -*-
# Copyright (c) 2024, Owens Valley Radio Observatory Long Wavelength Array
# All rights reserved.


import re
from collections import defaultdict
from pathlib import Path
from typing import List

import numpy as np
from astropy import units
from astropy.time import Time, TimeDelta

TIME_REGEX = re.compile(r".*(?P<date>\d{8})_(?P<hms>\d{6})_\d{2}MHz.ms")


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
        The files found in the input path grouped into 5 minutes windows.
        The keys are the center time in this window.
        The values are the file names in this window.
    """

    times_to_fnames = defaultdict(list)
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

        times_to_fnames[timeval] += [fname]

    time_array = Time(sorted(times_to_fnames.keys()))

    grouped_data = defaultdict(list)
    group_start_ind = 0

    while group_start_ind < len(time_array):
        group_inds = (
            np.nonzero(
                time_array[group_start_ind:] - time_array[group_start_ind]
                <= time_window
            )[0]
            + group_start_ind
        )
        central_time = np.mean(time_array[group_inds].jd)

        for ind in group_inds:
            grouped_data[central_time] += times_to_fnames[time_array[ind]]

        group_start_ind = group_inds[-1] + 1

    return grouped_data
