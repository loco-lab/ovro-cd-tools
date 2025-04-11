import matplotlib  # noqa:

matplotlib.use("Agg")  # noqa:

import argparse
import re
import shutil
import subprocess
import sys
from functools import partial
from multiprocessing import Pool
from pathlib import Path

import ffmpeg
from astropy import units
from astropy.time import TimeDelta
from casatasks import applycal, clearcal

from . import utils


class DefaultRaw(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawTextHelpFormatter,
):
    pass


DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
COMPONENT_LIST = str(Path("/lustre/mkolopanis/movies") / "ovro_ateam.cl")

WSCLEAN_CMD = (
    "OPENBLAS_NUM_THREADS=1 /opt/bin/wsclean -j 16 -mem 30 -multiscale "
    "-multiscale-scale-bias 0.8 -pol IV -size 4096 4096 -scale 0.03125 "
    "-niter 0 -casa-mask /home/pipeline/cleanmask.mask/ -mgain 0.85 "
    "-weight briggs 0 -name "
)


def main():
    """Command line script to automatically group data and perform imaging every night."""
    parser = argparse.ArgumentParser(
        prog="ovro_nightly_movie",
        description=(
            "Groups data in windowed chunks, images each chunk, then stitches all the imgaes into a movie."
            "This program searches for all data corresponding to <date> below."
        ),
        formatter_class=DefaultRaw,
    )

    parser.add_argument(
        "date",
        type=str,
        help=(
            "The date for which a movie will be made."
            "This should look like YYYY-MM-DD the same as the dates in data directories."
        ),
    )

    parser.add_argument(
        "--interval",
        "-i",
        required=False,
        type=float,
        default=5.0,
        help=("The interval in minutes the data will be grouped into."),
    )

    parser.add_argument(
        "--datapath",
        "-d",
        required=False,
        type=Path,
        default=Path("/lustre/pipeline/slow"),
    )

    args = parser.parse_args()

    # group all files

    if DATE_REGEX.match(args.date) is None:
        raise ValueError("Input date must be a date in the format YYYY-MM-DD")

    filelist = list(args.datapath.glob(f"*[!13MHz]*/{args.date}/*/*MHz.ms"))

    subbands = list(
        set(map(lambda x: utils.TIME_REGEX.match(str(x)).group("band"), filelist))
    )

    bcal_exists = utils.check_for_bcal(args.date, subbands)
    calibration_function = partial(apply_cal, bcal_exists)

    print("Grouping data files")
    # switch to a central time in a 5min window. Don't use the entire window.
    grouped_data = utils.group_files(filelist, TimeDelta(args.interval * units.min))

    # TODO: General bleach the absolute paths somehow
    date_dir = Path("/lustre/mkolopanis/movies") / args.date
    output_prefix = date_dir / "data"

    # make the date's directory in the staging area.
    output_prefix.mkdir(parents=True, exist_ok=True)

    if not bcal_exists:
        print("No bcal files found. generating naive calibration")
        utils.naive_calibration(grouped_data, output_prefix)

    for central_time, file_group in grouped_data.items():
        print(f"Working on {central_time.iso}")
        file_group = utils.get_central_integration(file_group, central_time)
        print("\tCopying Files")
        working_file_group = utils.copy_files(file_group, output_prefix)
        # Split into high and low bands
        lowband, highband = utils.partition_files(working_file_group)

        # output time in YYYYMMDD_HHMMSS
        time_str = central_time.strftime("%Y%m%d_%H%M%S")

        highband_name_stem = f"{time_str}_highband"
        lowband_name_stem = f"{time_str}_lowband"

        highband_image = str(date_dir / highband_name_stem)
        lowband_image = str(date_dir / lowband_name_stem)

        highband_jpg = str(date_dir / (highband_name_stem + ".jpg"))
        lowband_jpg = str(date_dir / (lowband_name_stem + ".jpg"))

        for filename in lowband + highband:
            calibration_function(filename)

        subprocess.run(
            WSCLEAN_CMD + f"{highband_image} {' '.join(map(str, highband))}",
            shell=True,
            check=True,
        )
        subprocess.run(
            WSCLEAN_CMD + f"{lowband_image} {' '.join(map(str, lowband))}",
            shell=True,
            check=True,
        )

        with Pool(2) as p:
            p.starmap(
                utils.plot_snapshot,
                [
                    (
                        [
                            highband_image + "-I-dirty.fits",
                            highband_image + "-V-dirty.fits",
                        ],
                        highband_jpg,
                    ),
                    (
                        [
                            lowband_image + "-I-dirty.fits",
                            lowband_image + "-V-dirty.fits",
                        ],
                        lowband_jpg,
                    ),
                ],
            )
        print("Removing data files")
        for path in lowband + highband:
            shutil.rmtree(path)

        for image_type in [highband_image, lowband_image]:
            for pol in ["I", "V"]:
                Path(image_type + "-" + pol + "-dirty.fits").unlink()

    date_str = "".join(args.date.split("-"))
    for name in ["highband", "lowband"]:
        ffmpeg.input(
            f"{date_dir}/*{name}.jpg",
            pattern_type="glob",
            framerate=12.5,
        ).output(
            str(
                Path("/lustre/mkolopanis/movies")
                / f"ovro_nightly_{name}_{date_str}.mp4"
            ),
        ).overwrite_output().run(cmd=str(Path(sys.executable).parent / "ffmpeg"))

    print("Removing intermediate JPG files")
    for jpg_file in Path(f"{date_dir}").glob("*.jpg"):
        jpg_file.unlink()


def apply_cal(bcal_exists: bool, filename: Path):
    filename = str(filename)

    clearcal(filename, addmodel=True)

    if bcal_exists:
        bcal = utils.get_bcal(filename, Path("/lustre/celery/bcal/"))
    else:
        bcal = utils.get_bcal(filename, Path(filename).parent)

    applycal(filename, gaintable=[bcal], flagbackup=False)
