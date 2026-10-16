#!/bin/bash
# =============================================================================
# Report which subject-sessions or BIDS subjects have NOT completed.
#
# A job pushes its result branch to the output RIA only as its final step, so a
# missing job-*-<subses> branch == a failed or incomplete job. This check needs
# no scheduler state and is safe to run repeatedly.
#
# Run from the analysis dataset directory. Writes code/done.txt + code/missing.txt.
# =============================================================================
set -e -u
# shellcheck disable=SC1091
source code/job_config.sh

# Publication units that have a result branch in the output RIA. The optional
# session group keeps the original derivative behavior and also supports BIDS
# jobs whose unit is one top-level sub-* directory.
git ls-remote "${pushgitremote}" 'refs/heads/job-*' 2>/dev/null \
    | grep -oE 'sub-[A-Za-z0-9]+(_ses-[A-Za-z0-9]+)?' | sort -u > code/done.txt

# Everything in all_subses.txt that is not yet done.
grep -vxF -f code/done.txt code/all_subses.txt > code/missing.txt || true

n_all=$(wc -l < code/all_subses.txt)
n_done=$(wc -l < code/done.txt)
n_missing=$(wc -l < code/missing.txt)
echo "total=${n_all}  done=${n_done}  missing=${n_missing}"

if [ "${n_missing}" -gt 0 ] && [ "${FIND_MISSING_QUIET:-0}" != "1" ]; then
    echo ""
    echo "Resubmit the missing jobs with:"
    echo "  bash code/submit_jobs.sh code/missing.txt   # add --local [--n-cpus 8] to run without SLURM"
fi
