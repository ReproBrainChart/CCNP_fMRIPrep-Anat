#!/bin/bash
#SBATCH --chdir=/cbica/projects/pennlinc_rbc/rbc_release/CCNP_fMRIPrep-Anat/analysis
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --tmp=50G
#SBATCH --output=logs/slurm-%A_%a.out
#SBATCH --error=logs/slurm-%A_%a.err
# =============================================================================
# SLURM array wrapper. Each array task unzips one subject-session.
#
#   Usage:  sbatch --array=1-N%THROTTLE code/submit_array.sh [PARAMS_FILE]
#
# PARAMS_FILE defaults to code/all_subses.txt; pass code/missing.txt to
# resubmit only the failed jobs (see code/find_missing.sh). N must equal the
# number of lines in PARAMS_FILE; %THROTTLE caps concurrent jobs.
#
# The working dir and the --mem/--time/--tmp directives above are filled in by
# bootstrap.sh from the dataset config (PROJECTROOT, JOB_MEM, JOB_TIME, JOB_TMP).
# =============================================================================
set -e -u -x

PARAMS_FILE="${1:-code/all_subses.txt}"
# shellcheck disable=SC1091
source code/job_config.sh

subses=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "${PARAMS_FILE}")
if [ -z "${subses}" ]; then
    echo "ERROR: no entry at row ${SLURM_ARRAY_TASK_ID} of ${PARAMS_FILE}" 1>&2
    exit 1
fi

bash code/participant_job.sh "${dssource}" "${pushgitremote}" "${subses}" "${DSLOCKFILE}"
