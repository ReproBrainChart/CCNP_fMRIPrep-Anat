#!/bin/bash
# =============================================================================
# Run the publication jobs listed in a params file: one participant_job.sh per
# subject-session (zip) or BIDS subject.
#
#   Usage:  bash code/submit_jobs.sh [--local] [--n-cpus N] [--workdir DIR] [PARAMS_FILE]
#
# By default the jobs are submitted as a SLURM array, throttled to THROTTLE
# concurrent tasks (exactly `sbatch --array=1-N%THROTTLE code/submit_array.sh`).
# With --local, the same job script runs here with plain bash instead, keeping
# at most N job scripts running at once (--n-cpus, default 8).
#
# PARAMS_FILE defaults to code/all_subses.txt; pass code/missing.txt to rerun
# only the failed jobs (see code/find_missing.sh). Environment variables such as
# FORCE_UPLOAD_PATHS and SKIP_RESULT_PUSH are passed through to every job.
#
# Run from the analysis dataset directory.
# =============================================================================
set -e -u

usage() {
    echo "Usage: bash code/submit_jobs.sh [--local] [--n-cpus N] [--workdir DIR] [PARAMS_FILE]" 1>&2
}

LOCAL=0
N_CPUS=8
WORKDIR="${TMPDIR:-/cbica/comp_space/pennlinc_rbc}"
PARAMS_FILE="code/all_subses.txt"
PARAMS_GIVEN=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --local)
            LOCAL=1
            ;;
        --n-cpus)
            N_CPUS="${2:?--n-cpus needs a value}"
            shift
            ;;
        --workdir)
            WORKDIR="${2:?--workdir needs a value}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "ERROR: unknown option: $1" 1>&2
            usage
            exit 1
            ;;
        *)
            PARAMS_FILE="$1"
            PARAMS_GIVEN=1
            ;;
    esac
    shift
done

if ! printf '%s\n' "${N_CPUS}" | grep -Eq '^[1-9][0-9]*$'; then
    echo "ERROR: --n-cpus must be a positive integer, got '${N_CPUS}'." 1>&2
    exit 1
fi
if [ ! -f code/job_config.sh ]; then
    echo "ERROR: code/job_config.sh not found; run from the analysis dataset directory." 1>&2
    exit 1
fi
# shellcheck disable=SC1091
source code/job_config.sh

if [ ! -f "${PARAMS_FILE}" ]; then
    echo "ERROR: params file not found: ${PARAMS_FILE}" 1>&2
    exit 1
fi

# Repair runs must name the units to rerun; never fall back to every unit.
# Jobs read FORCE_UPLOAD_PATHS after changing into their own clone, so pass an
# absolute path.
if [ -n "${FORCE_UPLOAD_PATHS:-}" ]; then
    if [ "${PARAMS_GIVEN}" -eq 0 ]; then
        echo "ERROR: FORCE_UPLOAD_PATHS is set; pass the params file listing the units to repair." 1>&2
        exit 1
    fi
    if [ ! -s "${FORCE_UPLOAD_PATHS}" ]; then
        echo "ERROR: FORCE_UPLOAD_PATHS is empty or unreadable: ${FORCE_UPLOAD_PATHS}" 1>&2
        exit 1
    fi
    case "${FORCE_UPLOAD_PATHS}" in
        /*) ;;
        *) FORCE_UPLOAD_PATHS="${PWD}/${FORCE_UPLOAD_PATHS}" ;;
    esac
    export FORCE_UPLOAD_PATHS
fi

# Count rows like submit_array.sh reads them (sed -n Np), including a last line
# without a trailing newline.
N=$(awk 'END { print NR }' "${PARAMS_FILE}")
if [ "${N}" -eq 0 ]; then
    echo "Nothing to run: ${PARAMS_FILE} is empty."
    exit 0
fi

if [ "${LOCAL}" -eq 0 ]; then
    sbatch --export=ALL --array="1-${N}%${THROTTLE}" code/submit_array.sh "${PARAMS_FILE}"
    exit 0
fi

# --- local mode --------------------------------------------------------------
# participant_job.sh names its result branch after the SLURM array IDs and works
# in TMPDIR, so provide both. Each job makes its own <workdir>/<branch> dir.
mkdir -p "${WORKDIR}" logs
# Jobs cd into their clone and clean up via this path, so it must be absolute.
WORKDIR=$(cd "${WORKDIR}" && pwd)
RUN_ID="local-$(date +%Y%m%d%H%M%S)-$$"
export RUN_ID WORKDIR PARAMS_FILE dssource pushgitremote DSLOCKFILE
export INPUT_LAYOUT="${INPUT_LAYOUT:-zip}"

echo "Running ${N} job(s) from ${PARAMS_FILE} locally, ${N_CPUS} at a time."
echo "Work dir: ${WORKDIR}   Logs: logs/${RUN_ID}_<row>.{out,err}"

run_row() {
    local row="$1"
    local subses
    subses=$(sed -n "${row}p" "${PARAMS_FILE}")
    if [ -z "${subses}" ]; then
        echo "FAIL  ${row} (no entry at row ${row} of ${PARAMS_FILE})"
        return 0
    fi
    if SLURM_ARRAY_JOB_ID="${RUN_ID}" SLURM_ARRAY_TASK_ID="${row}" TMPDIR="${WORKDIR}" \
        bash code/participant_job.sh \
            "${dssource}" "${pushgitremote}" "${subses}" "${DSLOCKFILE}" "${INPUT_LAYOUT}" \
            > "logs/${RUN_ID}_${row}.out" 2> "logs/${RUN_ID}_${row}.err"; then
        echo "OK    ${row} ${subses}"
    else
        echo "FAIL  ${row} ${subses}   (see logs/${RUN_ID}_${row}.err)"
    fi
}
export -f run_row

RESULTS="logs/${RUN_ID}.results"
seq 1 "${N}" | xargs -P "${N_CPUS}" -I{} bash -c 'run_row "$1"' _ {} | tee "${RESULTS}"

n_ok=$(grep -c '^OK' "${RESULTS}" || true)
echo "${n_ok}/${N} job(s) succeeded. Results: ${RESULTS}"
if [ "${n_ok}" -ne "${N}" ]; then
    exit 1
fi
