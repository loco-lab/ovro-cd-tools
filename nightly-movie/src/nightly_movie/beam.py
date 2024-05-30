import glob
import os

import numpy as np
from astropy.coordinates import AltAz, Angle, EarthLocation, SkyCoord

BEAM_FILE_PATH = os.path.abspath("/opt/beam")

OVRO_LOCATION = EarthLocation.from_geodetic(
    lat=Angle("37.239777271d"),
    lon=Angle("-118.281666695d"),
    height=1183.48,
)


class Beam:
    """
    For loading and returning LWA dipole beam values (derived from DW beam simulations) on the ASTM.
    Last edit: 08 August 2016
    """

    def __init__(self, CRFREQ, obstime):
        self.freq = CRFREQ
        self.obstime = obstime
        self.altaz = AltAz(location=OVRO_LOCATION, obstime=self.obstime)

        # load 4096x4096 grid of azimuth,elevation values
        self.azelgrid = np.load(BEAM_FILE_PATH + "/azelgrid.npy")
        self.gridsize = self.azelgrid.shape[-1]
        # load 4096x4096 grid of IQUV values, for given msfile CRFREQ
        beamIQUVfile = BEAM_FILE_PATH + "/beamIQUV_" + str(CRFREQ) + ".npz"
        if not os.path.exists(beamIQUVfile):
            print(
                f"Beam .npz file does not exist at {CRFREQ}. Using closest existing frequency beam file."
            )
            beamfiles = np.sort(glob.glob(f"{BEAM_FILE_PATH}/beamIQUV_*.npz"))
            freqs_beamfiles = np.char.strip(
                np.char.strip(beamfiles, chars="/opt/beam/beamIQUV_"), chars=".npz"
            ).astype(float)
            beamIQUVfile = beamfiles[np.argmin(np.abs(freqs_beamfiles - CRFREQ))]
        beamIQUV = np.load(beamIQUVfile)
        self.Ibeam = beamIQUV["I"]
        self.Qbeam = beamIQUV["Q"]
        self.Ubeam = beamIQUV["U"]
        self.Vbeam = beamIQUV["V"]

    def srcIQUV(self, az, el):
        """Compute beam scaling factor
        Args:
            az: azimuth in degrees
            el: elevation in degrees

        Returns: [I,Q,U,V] flux factors, where for an unpolarized source [I,Q,U,V] = [1,0,0,0]

        """

        def knn_search(arr, grid):
            """
            Find 'nearest neighbor' of array of points in multi-dimensional grid
            Source: glowingpython.blogspot.com/2012/04/k-nearest-neighbor-search.html
            """
            gridsize = grid.shape[1]
            dists = np.sqrt(((grid - arr[:, :gridsize]) ** 2.0).sum(axis=0))
            return np.argsort(dists)[0]

        # index where grid equals source az el values
        index = knn_search(
            np.array([[az], [el]]),
            self.azelgrid.reshape(2, self.gridsize * self.gridsize),
        )
        Ifctr = self.Ibeam.flat[index]
        Qfctr = self.Qbeam.flat[index]
        Ufctr = self.Ubeam.flat[index]
        Vfctr = self.Vbeam.flat[index]
        return Ifctr, Qfctr, Ufctr, Vfctr

    def apply_beam(self, source):
        """Apply beam scaling factors to the source.


        Returns
        -------
        np.ndarray:
            Apparent [I, Q, U, V] values of source flux
        """
        altaz = SkyCoord.from_name(source["label"]).transform_to(self.altaz)
        if altaz.alt.deg >= 10:
            scale = np.array(self.srcIQUV(altaz.az.deg, altaz.alt.deg))
        else:
            scale = np.ones(4)

        return (
            _flux80_47(source["flux"], source["alpha"], self.freq, source["ref_freq"])
            * scale
        )


def _flux80_47(flux_hi, sp, output_freq, ref_freq):
    # given a flux at 80 MHz and a sp_index,
    # return the flux at MS center-frequency.
    return float(flux_hi) * 10 ** (sp * np.log10(output_freq / ref_freq))
