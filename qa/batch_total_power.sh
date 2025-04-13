#! /usr/bin/env bash

# give a night directory like /lustre/pipeline/cosmology/82MHz/2025-04-04

date_str=$(basename $1)

#SBATCH --job-name=nightly_QA_${date_str}
#SBATCH --output=/lustre/djacobs/QA/TP/batch_${date_str}.log
#SBATCH --partition=general

#SBATCH --time=40:00:00 
#SBATCH --ntasks=100
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=20GB

source ~/.bashrc
set -e
set -o pipefail
echo "TASK ID" $SLURM_ARRAY_TASK_ID

logfile=/lustre/djacobs/QA/TP/${date_str}.log

if mamba activate LWA2024 |& tee -a $logfile ; then
    : # everything OK
else
    ec=$?
    echo "error: had a problem activating LWA2024 environment." |& tee -a $logfile
    exit $ec
fi
echo $(which python)
#RUN SCRIPT
# use slurm env variables to divide over list of input files
ALLFILES=$(ls -d $1/*/*ms)
TASKFILES=$(~/src/ovro-cd-tools/qa/array_select.py $SLURM_ARRAY_TASK_ID $SLURM_ARRAY_TASK_COUNT $ALLFILES)
for TASKFILE in $TASKFILES;
do 
echo $TASKFILE
time python ~/src/ovro-cd-tools/qa/total_power.py $TASKFILE
done




