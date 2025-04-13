
from pyuvdata import UVData,utils
from glob import glob
import numpy as np
import os, time
import matplotlib.pyplot as plt
import sys
def length(X):
    return np.array([np.sqrt(np.dot(x,x)) for x in X])

# Script prototype
# input a single file
#  go find all other subbands
#  make a qa file
# exit
#  batch parallel in slurm by inputting a flat list of files by subband  (eg /lustre/pipeline/cosmology/82MHz/2025-04-04/*/20250404_*_82MHz.ms)
#inputfilename = '/lustre/pipeline/cosmology/82MHz/2025-04-04/06/20250404_060158_82MHz.ms'
inputfilename = sys.argv[1]
assert os.path.exists(inputfilename), f'error: {inputfilename} not found'

# replace the subbands with *
subbandglob = []
for chunk in inputfilename.split('MHz')[:-1]:
    print(chunk)
    subbandglob.append(chunk[:-2]+'*')
subbandglob = ''.join(subbandglob)+inputfilename.split('MHz')[-1]
bandfiles = glob(subbandglob)
outfile = os.path.basename(inputfilename).split('_')
outfile = 'tp.'+'_'.join([outfile[0],outfile[1]])+'.npz'
print(outfile)

# move on to calculation
D = [] #total power spectrum
freqs = []
tstart = time.time()
#iterate over all freqs
for filename in bandfiles:
    UV = UVData()    
    UV.read_ms(filename)
    assert UV.Ntimes == 1,"This script only works if there is one integration"
    lengths = length(UV.uvw_array)
    d = np.sum(np.abs(UV.data_array[lengths>0,:]),axis=0) #total power spectrum of a sub band
    D.append(d)
    freqs.append(UV.freq_array)
    pols = UV.polarization_array
print(f'finished in {(time.time() - tstart)/60} minutes')
D = np.vstack(D)
freqs = np.hstack(freqs)

D = D[np.argsort(freqs)]
freqs = freqs[np.argsort(freqs)]
np.savez(outfile, data=D,freqs=freqs,pols=pols)



