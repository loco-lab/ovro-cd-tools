import itertools
import tarfile

import matplotlib  # noqa:

matplotlib.use("Agg")  # noqa:

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path

import ffmpeg
from astropy import units
from astropy.time import Time, TimeDelta
from casatasks import applycal, clearcal

from . import utils


class DefaultRaw(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawTextHelpFormatter,
):
    pass


log = utils.log


DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")
COMPONENT_LIST = str(Path("/lustre/mkolopanis/movies") / "ovro_ateam.cl")

WSCLEAN_CMD = (
    "OPENBLAS_NUM_THREADS=1 /opt/bin/wsclean -j 1 -abs-mem 12 -multiscale "
    "-multiscale-scale-bias 0.8 -pol IV -size 4096 4096 -scale 0.03125 "
    "-niter 0 -casa-mask /home/pipeline/cleanmask.mask/ -mgain 0.85 "
    "-weight briggs 0 -name "
)


def image_snapshot():
    parser = argparse.ArgumentParser(
        prog="ovro_nightly_image_snapshot",
        description=(
            "Create a jpg image from the time snapshot. YOU SHOULD NOT BE CALLING THIS DIRECTLY.\n "
            "This script is called as part of ovro_nightly_movie when submitting a slurm job."
        ),
        formatter_class=DefaultRaw,
    )

    parser.add_argument(
        "staging_prefix",
        type=Path,
    )

    parser.add_argument(
        "output_prefix",
        type=Path,
    )
    parser.add_argument(
        "bcal",
        type=Path,
    )

    parser.add_argument(
        "central_time",
        type=lambda d: Time(d, format="isot"),
        help="ISOT formatted central integration time in this group",
    )

    parser.add_argument(
        "file_group",
        type=Path,
        nargs="+",
        help="List of files to calibration",
    )

    args = parser.parse_args()
    log.info("Starting Image Snapshot")

    central_time = args.central_time
    file_group = args.file_group
    staging_prefix = args.staging_prefix
    output_prefix = args.output_prefix
    bcal = args.bcal

    log.info(f"Working on {args.central_time.iso}")
    file_group = utils.get_central_integration(args.file_group, central_time)
    log.info("\tCopying Files")

    date_dir = staging_prefix.parent
    # make directories in the staging/output area for this node if necessary

    staging_prefix.mkdir(parents=True, exist_ok=True)
    working_file_group = utils.copy_files(file_group, staging_prefix)

    try:
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

        log.info("Applying calibration", flush=True)
        for filename in lowband + highband:
            apply_cal(filename, bcal)

        log.info("Creating Fits output", flush=True)
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

        log.info("Plotting jpgs", flush=True)
        for image_type, jpg_name in [
            (highband_image, highband_jpg),
            (lowband_image, lowband_jpg),
        ]:
            utils.plot_snapshot(
                [
                    image_type + "-I-dirty.fits",
                    image_type + "-V-dirty.fits",
                ],
                jpg_name,
            )

        log.info(" Moving output data products", flush=True)
        #  move the fits and jpg files from staging to output
        for fname in itertools.chain(
            date_dir.glob(f"{time_str}*.jpg"), date_dir.glob(f"{time_str}*.fits")
        ):
            shutil.move(fname, output_prefix / fname.name)

    finally:
        log.info("Removing Raw data files")
        for path in lowband + highband:
            shutil.rmtree(path)

    log.info("Image Snapshot Complete", flush=True)


def create_mp4():
    parser = argparse.ArgumentParser(
        prog="ovro_nightly_create_movie",
        description=(
            "Stitches all image files into an mp4. YOU SHOULD NOT BE CALLING THIS DIRECTLY.\n "
            "This script is called as part of ovro_nightly_movie when submitting a slurm job."
        ),
        formatter_class=DefaultRaw,
    )

    parser.add_argument(
        "date_dir",
        type=Path,
    )
    args = parser.parse_args()
    log.info("Starting Movie stitching")

    date_dir = args.date_dir
    staging_date_dir = date_dir.name

    fast_dir = f"/fast/mkolopanis/movies/{staging_date_dir}"
    log.info("Removing /fast staging areas")
    subprocess.check_output(
        f"pdsh -w lwacalim[00-10] 'if [ -d \"{fast_dir}\" ]; then rm -r {fast_dir}; fi'",
        shell=True,
    )

    log.info("Cleaning up any remaining flag files")
    for fname in (date_dir / "data").glob("*.flagversions"):
        shutil.rmtree(fname)

    log.info("Starting Movie Stitching", flush=True)

    date_str = date_dir.name.replace("-", "")
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

    log.info("Removing intermediate JPG files", flush=True)
    for jpg_file in Path(f"{date_dir}").glob("*.jpg"):
        jpg_file.unlink()

    log.info("Consolidating Fits Files")
    # combine all the fits files into their own group
    for image_band in ["highband", "lowband"]:
        for pol in ["I", "V"]:
            for image_type in ["image", "dirty"]:
                file_stub = f"{image_band}-{pol}-{image_type}.fits"
                filenames = sorted(Path(f"{date_dir}").glob("*" + file_stub))

                outfile = date_dir / (date_str + "_" + file_stub)

                utils.combine_fits_files(filenames, outfile)

    log.info("Compressing Logs")
    with tarfile.open(date_dir / "logs.tgz", "w:gz") as tar:
        tar.add(date_dir / "logs", arcname="logs")  # cspell:disable-line
    # remove the individual log files
    log.info("Movie Stitching Complete", flush=True)
    shutil.rmtree(date_dir / "logs")


def apply_cal(filename: Path, bcal_path: Path):
    filename = str(filename)

    clearcal(filename, addmodel=True)

    bcal = utils.get_bcal(filename, bcal_path)

    applycal(filename, gaintable=[bcal], flagbackup=False)


def naive_calibration():
    parser = argparse.ArgumentParser(
        prog="ovro_nightly_naive_calibration",
        description=(
            "Performs a naive calibration of ovro data for nightly movie making. YOU SHOULD NOT BE CALLING THIS DIRECTLY.\n "
            "This script is called as part of ovro_nightly_movie when submitting a slurm job."
        ),
        formatter_class=DefaultRaw,
    )

    parser.add_argument(
        "staging_dir",
        type=Path,
    )

    parser.add_argument(
        "output_prefix",
        type=Path,
    )
    parser.add_argument(
        "calibration_file_group",
        type=Path,
        nargs="+",
        help="List of files to calibration",
    )

    args = parser.parse_args()
    log.info("Starting Calibration", flush=True)
    utils.naive_calibration(
        args.calibration_file_group, args.staging_dir, args.output_prefix
    )
    log.info("Calibration Finished", flush=True)


def main():
    """Command line script to automatically group data and perform imaging every night."""
    parser = argparse.ArgumentParser(
        prog="ovro_nightly_movie",
        description=(
            "Groups data in windowed chunks, images each chunk, then stitches all the images into a movie."
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
        "--data_path",
        "-d",
        required=False,
        type=Path,
        default=Path("/lustre/pipeline/slow"),
    )

    args = parser.parse_args()

    # group all files

    if DATE_REGEX.match(args.date) is None:
        raise ValueError("Input date must be a date in the format YYYY-MM-DD")

    filelist = list(args.data_path.glob(f"*[!13MHz]*/{args.date}/*/*MHz.ms"))
    # an empty list evaluates to false.
    if not filelist:
        raise ValueError(f"Unable to find any data files for: {args.date}")

    date_dir = Path("mkolopanis/movies") / args.date
    date_str = args.date.replace("-", "")

    output_date_dir = "/lustre" / date_dir
    staging_date_dir = "/fast" / date_dir

    output_prefix = output_date_dir / "data"
    slurm_logs = output_date_dir / "logs"

    staging_data = staging_date_dir / "data"

    sub_bands = list(
        set(map(lambda x: utils.TIME_REGEX.match(str(x)).group("band"), filelist))
    )
    bcal_stub = Path("/lustre/celery/bcal/")
    bcal_exists = utils.check_for_bcal(args.date, sub_bands, bcal_stub)
    # if ew don't have the global calibration files, see if we need to make our own
    if not bcal_exists:
        log.info(
            "No Bandpass calibration found or some are missing. Performing naive calibration."
        )
        bcal_stub = output_prefix
        bcal_exists = utils.check_for_bcal(args.date, sub_bands, bcal_stub)

    log.info("Grouping data files")
    # switch to a central time in a 5min window. Don't use the entire window.
    grouped_data = utils.group_files(filelist, TimeDelta(args.interval * units.min))

    # make the date's directory in the staging area.
    output_prefix.mkdir(parents=True, exist_ok=True)
    job_name = f"nightly_movie_{date_str}"
    slurm_logs.mkdir(exist_ok=True)

    cal_job_id = None
    if not bcal_exists:
        calibration_file_group = utils.get_calibration_files(grouped_data)
        log.info("No bcal files found. generating naive calibration")
        # create calibration job assign it to job_id
        cal_executable = str(
            Path(sys.executable).parent / "ovro_nightly_naive_calibration"
        )
        cal_log = slurm_logs / "calibration.out"
        status, cal_job_id = subprocess.getstatusoutput(
            f"sbatch --job-name={job_name} --output={str(cal_log)} --mem=20G --cpus-per-task=1 {cal_executable} "
            f"{str(output_prefix)} {' '.join(map(str, calibration_file_group))}"
        )
        if status != 0:
            raise ValueError(f"Error spawning calibration job: {cal_job_id}")
        else:
            log.info(f"{cal_job_id}")
            # parse the int from the output
            cal_job_id = int(cal_job_id.split(" ")[-1])

    # submit each job had have them depend on calibration job if it exists
    for cnt, (central_time, file_group) in enumerate(grouped_data.items()):
        snapshot_executable = str(
            Path(sys.executable).parent / "ovro_nightly_image_snapshot"
        )

        # check if we need a dependency here
        dependency = (
            f"--dependency=afterok:{cal_job_id}" if cal_job_id is not None else ""
        )
        snapshot_log = slurm_logs / f"snapshot_{cnt:0>4}.out"

        status, snapshot_id = subprocess.getstatusoutput(
            f"sbatch {dependency} --job-name={job_name} --output={str(snapshot_log)} --mem=12G --cpus-per-task=1 "
            f"{snapshot_executable} {str(staging_data)} {str(output_date_dir)} {str(bcal_stub)} {central_time.isot} {' '.join(map(str, file_group))}"
        )
        if status != 0:
            raise ValueError(f"Error spawning image snapshot job: {snapshot_id}")
        else:
            log.info(f"{snapshot_id}")
            # parse the int from the output
            snapshot_id = int(snapshot_id.split(" ")[-1])

    # singleton job that depends on everything else running
    mp4_executable = str(Path(sys.executable).parent / "ovro_nightly_create_movie")
    movie_log = slurm_logs / "movie.out"
    status, movie_id = subprocess.getstatusoutput(
        f"sbatch --dependency=singleton --output={str(movie_log)} --job-name={job_name} --mem=5G --cpus-per-task=1 "
        f"{mp4_executable} {str(output_date_dir)}"
    )
    if status != 0:
        raise ValueError(f"Error spawning movie stitching job: {movie_id}")
    else:
        log.info(f"{movie_id}")
