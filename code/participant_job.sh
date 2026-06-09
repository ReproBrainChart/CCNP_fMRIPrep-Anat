#!/bin/bash
# =============================================================================
# Per subject-session unzip job. Invoked by submit_array.sh as one SLURM array
# task. Clones the analysis dataset, unzips one archive, pushes the individual
# files to fcp-indi (S3) and the result branch to the output RIA store.
#
#   Args: <dssource> <pushgitremote> <subses> <dslockfile>
#
# A result branch reaches the output RIA only on full success. Partial S3 uploads
# are safe because git-annex content is content-addressed and retryable; the
# branch push is the final success marker that find_missing.sh looks for.
# =============================================================================
set -e -u -x

dssource="$1"       # input RIA URL to clone the analysis dataset from
pushgitremote="$2"  # output RIA git URL to push the result branch to
subses="$3"         # e.g. sub-1234_ses-1
DSLOCKFILE="$4"     # shared lock file serializing git pushes

# Load fcp-indi credentials before the clone so the S3 remote auto-enables.
# (load_credentials.sh disables xtrace while decoding so secrets never reach
# the SLURM log files.)
# shellcheck disable=SC1091
source code/load_credentials.sh || exit 1

# Work in node-local scratch assigned by SLURM (requested via #SBATCH --tmp).
cd "${TMPDIR}"
BRANCH="job-${SLURM_ARRAY_JOB_ID}-${SLURM_ARRAY_TASK_ID}-${subses}"

cleanup() {
    set +e
    jobdir="${TMPDIR:?}/${BRANCH:?}"
    cd "${TMPDIR}" 2>/dev/null || true
    rm -rf "${jobdir}" 2>/dev/null
    if [ -e "${jobdir}" ]; then
        # git-annex protects object directories against accidental writes; make
        # only those directories removable, then retry scratch cleanup.
        annex_objects="${jobdir}/ds/.git/annex/objects"
        if [ -d "${annex_objects}" ]; then
            chmod u+rwx "${annex_objects}" 2>/dev/null || true
            find "${annex_objects}" -type d -exec chmod u+rwx {} + 2>/dev/null || true
        fi
        rm -rf "${jobdir}" 2>/dev/null || true
        if [ -e "${jobdir}" ]; then
            echo "WARNING: scratch cleanup incomplete: ${jobdir}" 1>&2
        fi
    fi
}
trap cleanup EXIT

# Print annexed files in this job clone whose content is not known to be present
# on fcp-indi.
find_missing_on_fcpindi() {
    git annex find --not --in fcp-indi || true
}

# Verify that this job's annexed outputs are present on fcp-indi. If the first
# check finds missing content, retry only those files from the local job clone,
# then verify once more before the result branch is allowed to publish.
verify_fcpindi_content() {
    fsck_status=0
    # fsck checks the remote's annex location records/availability. Capture its
    # status instead of letting set -e exit before we can retry missing files.
    git annex fsck --fast -f fcp-indi || fsck_status=$?
    missing=$(find_missing_on_fcpindi)

    if [ "${fsck_status}" -eq 0 ] && [ -z "${missing}" ]; then
        return 0
    fi

    echo "WARNING: fcp-indi verification failed for ${subses}; retrying missing content." 1>&2
    if [ -n "${missing}" ]; then
        echo "${missing}" 1>&2
        while IFS= read -r path; do
            if [ -z "${path}" ]; then
                continue
            fi
            if [ -z "$(git annex find --in here -- "${path}")" ]; then
                echo "ERROR: cannot repair ${path}; content is not present in this job clone." 1>&2
                return 1
            fi
            # Re-copy just the missing key. Existing keys already on S3 are left
            # alone, which keeps failed-job resubmission idempotent.
            git annex copy --to fcp-indi -- "${path}"
        done <<EOF
${missing}
EOF
    fi

    # The retry must leave both fsck and the missing-file query clean. If not,
    # fail before pushing the result branch so this session can be resubmitted.
    git annex fsck --fast -f fcp-indi
    missing=$(find_missing_on_fcpindi)
    if [ -n "${missing}" ]; then
        echo "ERROR: content still missing on fcp-indi after retry:" 1>&2
        echo "${missing}" 1>&2
        return 1
    fi
}

mkdir "${BRANCH}"
cd "${BRANCH}"

# Clone the analysis dataset from the INPUT store (not the push target, to
# avoid a throughput bottleneck on the output store).
datalad clone "${dssource}" ds
cd ds
git remote add outputstore "${pushgitremote}"
git checkout -b "${BRANCH}"

# Install the zipped input subdataset (git tree only), then fetch just this
# subject-session's archive.
datalad get -n inputs/data
ZIP=$(find inputs/data -name "${subses}_*.zip" | head -n 1)
if [ -z "${ZIP}" ]; then
    echo "ERROR: no zip found for ${subses} under inputs/data" 1>&2
    exit 1
fi
datalad get "${ZIP}"

# Unzip + rename + de-duplicate, recorded as provenance.
datalad run -m "unzip ${subses}" "bash code/get_files.sh ${ZIP}"

# Push file CONTENT to S3 first -- no lock needed, content writes don't conflict.
# A transient partial failure is repaired/validated below; only the final
# verification result decides whether the success branch can be pushed.
push_status=0
datalad push --to fcp-indi || push_status=$?
if [ "${push_status}" -ne 0 ]; then
    echo "WARNING: datalad push to fcp-indi exited ${push_status}; verifying and retrying missing content." 1>&2
fi
# If content is still missing, exit before the branch push so find_missing.sh
# can resubmit this session. Partial S3 uploads are safe: annex keys are
# content-addressed, so retries reuse existing keys and upload only misses.
verify_fcpindi_content

# Push the result BRANCH second -- locked, to serialize git ref updates. Once
# this succeeds, find_missing.sh considers the subject-session complete.
flock "${DSLOCKFILE}" git push outputstore "${BRANCH}"

echo "SUCCESS ${subses}"
# Scratch clone is removed by the EXIT trap.
