#!/bin/bash
# =============================================================================
# Per-unit publication job. Invoked by submit_array.sh as one SLURM array task.
# Clones the analysis dataset, prepares one zipped subject-session or one
# already-unzipped BIDS subject, pushes its files to fcp-indi (S3), and pushes
# the result branch to the output RIA store.
#
#   Args: <dssource> <pushgitremote> <subses-or-subject> <dslockfile> [layout]
#
# A result branch reaches the output RIA only on full success. Partial S3 uploads
# are safe because git-annex content is content-addressed and retryable; the
# branch push is the final success marker that find_missing.sh looks for.
# =============================================================================
set -e -u -x

dssource="$1"       # input RIA URL to clone the analysis dataset from
pushgitremote="$2"  # output RIA git URL to push the result branch to
subses="$3"         # e.g. sub-1234_ses-1 (zip) or sub-1234 (BIDS)
DSLOCKFILE="$4"     # shared lock file serializing git pushes
INPUT_LAYOUT="${5:-zip}"
FORCE_UPLOAD_PATHS="${FORCE_UPLOAD_PATHS:-}"
# The list is read after changing into the job clone, so resolve it now.
case "${FORCE_UPLOAD_PATHS}" in
    ""|/*) ;;
    *) FORCE_UPLOAD_PATHS="${PWD}/${FORCE_UPLOAD_PATHS}" ;;
esac
SKIP_RESULT_PUSH="${SKIP_RESULT_PUSH:-0}"
# Transient fcp-indi probe errors are retried this many times in total.
FCPINDI_PROBE_ATTEMPTS="${FCPINDI_PROBE_ATTEMPTS:-3}"
FCPINDI_PROBE_RETRY_SLEEP="${FCPINDI_PROBE_RETRY_SLEEP:-5}"

case "${INPUT_LAYOUT}" in
    zip|bids)
        ;;
    *)
        echo "ERROR: unsupported input layout: ${INPUT_LAYOUT}" 1>&2
        exit 1
        ;;
esac

# Load fcp-indi credentials before the clone so the S3 remote auto-enables.
# (load_credentials.sh disables xtrace while decoding so secrets never reach
# the SLURM log files.)
# shellcheck disable=SC1091
source code/load_credentials.sh || exit 1

# Public URL prefix of this dataset's objects on fcp-indi, used to compare each
# object's size with the size recorded in its annex key. Read from the remote's
# own configuration so it always matches the object names git-annex uses
# (<fileprefix><key>).
fcpindi_remote_config=$(git show git-annex:remote.log 2>/dev/null \
    | grep -E ' name=fcp-indi( |$)' | tail -n 1 || true)
fcpindi_bucket=$(printf '%s\n' "${fcpindi_remote_config}" | sed -nE 's/.* bucket=([^ ]*).*/\1/p')
fcpindi_fileprefix=$(printf '%s\n' "${fcpindi_remote_config}" | sed -nE 's/.* fileprefix=([^ ]*).*/\1/p')
if [ -z "${fcpindi_bucket}" ] || [ -z "${fcpindi_fileprefix}" ]; then
    echo "ERROR: could not read bucket/fileprefix of the fcp-indi remote from git-annex:remote.log." 1>&2
    exit 1
fi
FCPINDI_URL="https://${fcpindi_bucket}.s3.amazonaws.com/${fcpindi_fileprefix}"

# Work in shared comp_space instead of node-local scratch.
JOB_WORKDIR="${TMPDIR}"
mkdir -p "${JOB_WORKDIR}"
# Absolute, because the EXIT cleanup runs from inside the job clone.
JOB_WORKDIR=$(cd "${JOB_WORKDIR}" && pwd)
cd "${JOB_WORKDIR}"
BRANCH="job-${SLURM_ARRAY_JOB_ID}-${SLURM_ARRAY_TASK_ID}-${subses}"

cleanup() {
    set +e
    jobdir="${JOB_WORKDIR:?}/${BRANCH:?}"
    cd "${JOB_WORKDIR}" 2>/dev/null || true
    rm -rf "${jobdir}" 2>/dev/null
    if [ -e "${jobdir}" ]; then
        # git-annex protects object directories, including in subdatasets; make
        # job directories removable, then retry workdir cleanup.
        find "${jobdir}" -type d -exec chmod u+rwx {} + 2>/dev/null || true
        rm -rf "${jobdir}" 2>/dev/null || true
        if [ -e "${jobdir}" ]; then
            echo "WARNING: workdir cleanup incomplete: ${jobdir}" 1>&2
        fi
    fi
}
trap cleanup EXIT

# Return the annex UUID for fcp-indi. This UUID is needed when repairing local
# presence records after direct S3 probes.
get_fcpindi_remote_uuid() {
    local remote_uuid

    remote_uuid=$(git config --get remote.fcp-indi.annex-uuid || true)
    if [ -z "${remote_uuid}" ]; then
        echo "ERROR: could not find annex UUID for fcp-indi." 1>&2
        return 1
    fi
    printf '%s\n' "${remote_uuid}"
}

# Compare the size of a key's object on fcp-indi with the size recorded in the
# key (-s<bytes>). checkpresentkey only proves that an object exists; a
# truncated multipart upload passes it but fails `datalad get`.
#   Returns 0 if the sizes match (or the key records no size), 1 if they differ,
#   2 if the object could not be probed.
fcpindi_object_size_ok() {
    local key="$1"
    local expected headers actual

    expected=$(printf '%s\n' "${key}" | sed -nE 's/^[A-Za-z0-9]+-s([0-9]+)-.*/\1/p')
    if [ -z "${expected}" ]; then
        return 0
    fi
    if ! headers=$(curl -sfI --retry 3 "${FCPINDI_URL}${key}"); then
        echo "ERROR: could not probe size of ${key} on fcp-indi." 1>&2
        return 2
    fi
    actual=$(printf '%s\n' "${headers}" | tr -d '\r' \
        | awk 'tolower($1) == "content-length:" { print $2 }' | tail -n 1)
    if [ -z "${actual}" ]; then
        echo "ERROR: no Content-Length for ${key} on fcp-indi." 1>&2
        return 2
    fi
    if [ "${actual}" != "${expected}" ]; then
        echo "WARNING: fcp-indi object ${key} is ${actual} bytes, expected ${expected}." 1>&2
        return 1
    fi
}

# Probe fcp-indi directly for every annexed key in this job clone. Unlike
# `git annex find --not --in`, checkpresentkey asks the special remote whether
# the object is actually available, so stale presence records cannot mask a
# failed upload. Objects whose size differs from the key are treated as missing
# so they get re-uploaded.
probe_fcpindi_content() {
    local present_batch_file="$1"
    local missing_file="$2"
    local probe_errors_file="$3"
    local check_stderr_file keys_file remote_uuid key path status size_status attempt result detail

    remote_uuid=$(get_fcpindi_remote_uuid)
    : > "${present_batch_file}"
    : > "${missing_file}"
    : > "${probe_errors_file}"
    check_stderr_file="${probe_errors_file}.check"
    keys_file="${probe_errors_file}.keys"

    # List the keys first so a failed listing is an error, not an empty (passing) probe.
    # shellcheck disable=SC2016
    if ! git annex find --format='${key}\t${file}\n' > "${keys_file}"; then
        echo "ERROR: could not list annexed files to probe on fcp-indi." >> "${probe_errors_file}"
        rm -f "${keys_file}"
        return 0
    fi

    while IFS=$'\t' read -r key path; do
        if [ -z "${path}" ]; then
            continue
        fi
        if [ -z "${key}" ]; then
            printf '%s\n' "${path}" >> "${probe_errors_file}"
            continue
        fi

        # Probe errors (network resets, timeouts) are retried; a definite
        # "absent" or "wrong size" answer is not.
        attempt=1
        while :; do
            : > "${check_stderr_file}"
            result=error
            if git annex checkpresentkey "${key}" fcp-indi >/dev/null 2>"${check_stderr_file}"; then
                size_status=0
                fcpindi_object_size_ok "${key}" 2>>"${check_stderr_file}" || size_status=$?
                case "${size_status}" in
                    0) result=present ;;
                    1) result=missing ;;
                    *) detail="size probe failed" ;;
                esac
            else
                status=$?
                if [ "${status}" -eq 1 ]; then
                    result=missing
                else
                    detail="checkpresentkey exited ${status}"
                fi
            fi
            if [ "${result}" != error ] || [ "${attempt}" -ge "${FCPINDI_PROBE_ATTEMPTS}" ]; then
                break
            fi
            echo "WARNING: fcp-indi probe of ${key} failed (${detail}); retry ${attempt}/$((FCPINDI_PROBE_ATTEMPTS - 1))." 1>&2
            attempt=$((attempt + 1))
            sleep "${FCPINDI_PROBE_RETRY_SLEEP}"
        done

        case "${result}" in
            present)
                printf '%s %s 1\n' "${key}" "${remote_uuid}" >> "${present_batch_file}"
                ;;
            missing)
                cat "${check_stderr_file}" 1>&2
                printf '%s\n' "${path}" >> "${missing_file}"
                ;;
            *)
                cat "${check_stderr_file}" >> "${probe_errors_file}"
                printf '%s\t%s\t%s after %s attempt(s)\n' "${path}" "${key}" "${detail}" "${attempt}" >> "${probe_errors_file}"
                ;;
        esac
    done < "${keys_file}"
    rm -f "${check_stderr_file}" "${keys_file}"

    if [ -s "${present_batch_file}" ]; then
        git annex setpresentkey --batch < "${present_batch_file}"
    fi
}

# Upload one annexed file to fcp-indi, overwriting any existing object for its
# key, and prove the new object is complete before returning success. A plain
# `git annex copy` asks the remote first and skips keys it reports as present,
# so a truncated or corrupt object would never be replaced. Instead the key is
# recorded absent and copied with --fast, which trusts that record and uploads
# over the existing object in place: the object is never deleted, so other
# publication units sharing the key never see it go missing.
#   Args: <path> <fcp-indi uuid> [expected key; must match the job's key]
upload_fcpindi_path_with_probe() {
    local path="$1"
    local remote_uuid="$2"
    local expected_key="${3:-}"
    local key

    key=$(git annex lookupkey -- "${path}" || true)
    if [ -z "${key}" ]; then
        echo "ERROR: cannot repair ${path}; it is not an annexed file." 1>&2
        return 1
    fi
    if [ -n "${expected_key}" ] && [ "${key}" != "${expected_key}" ]; then
        echo "ERROR: cannot repair ${path}; this job produced ${key}, expected ${expected_key}." 1>&2
        return 1
    fi
    if [ -z "$(git annex find --in here -- "${path}")" ]; then
        echo "ERROR: cannot repair ${path}; content is not present in this job clone." 1>&2
        return 1
    fi
    # Never replace the remote object with local content that fails its checksum.
    if ! git annex fsck -- "${path}"; then
        echo "ERROR: cannot repair ${path}; local content does not match ${key}." 1>&2
        return 1
    fi

    git annex setpresentkey "${key}" "${remote_uuid}" 0
    if ! git annex copy --fast --to fcp-indi -- "${path}"; then
        echo "ERROR: git annex copy failed for ${path}." 1>&2
        return 1
    fi
    if ! git annex checkpresentkey "${key}" fcp-indi; then
        echo "ERROR: fcp-indi still does not provide ${path} after copy." 1>&2
        return 1
    fi
    if ! fcpindi_object_size_ok "${key}"; then
        echo "ERROR: fcp-indi object for ${path} has the wrong size after copy." 1>&2
        return 1
    fi
    # Download and checksum the new object, exactly as `datalad get` would. If it
    # is bad, fsck removes it from fcp-indi and the job fails, so the unit can be
    # resubmitted.
    if ! git annex fsck --from fcp-indi -- "${path}"; then
        echo "ERROR: fcp-indi object for ${path} failed checksum verification." 1>&2
        return 1
    fi
    git annex setpresentkey "${key}" "${remote_uuid}" 1
}

# Verify that this job's annexed outputs are present on fcp-indi. This does not
# trust local annex presence records: every key is directly probed on S3. If the
# first probe finds missing content, retry only those files from the local job
# clone, then verify once more before the result branch is allowed to publish.
verify_fcpindi_content() {
    local fsck_status verify_dir present_batch_file missing_file probe_errors_file
    local remote_uuid retry_failed path

    fsck_status=0
    # fsck checks the remote's annex location records/availability. Capture its
    # status for diagnostics, but direct probes below are the source of truth.
    git annex fsck --fast -f fcp-indi || fsck_status=$?

    verify_dir=$(mktemp -d .git/fcpindi-verify.XXXXXX)
    present_batch_file="${verify_dir}/present.batch"
    missing_file="${verify_dir}/missing.txt"
    probe_errors_file="${verify_dir}/probe-errors.txt"

    probe_fcpindi_content "${present_batch_file}" "${missing_file}" "${probe_errors_file}"
    if [ "${fsck_status}" -eq 0 ] && [ ! -s "${missing_file}" ] && [ ! -s "${probe_errors_file}" ]; then
        rm -rf "${verify_dir}"
        return 0
    fi

    echo "WARNING: fcp-indi verification failed for ${subses}; retrying missing content." 1>&2
    if [ -s "${probe_errors_file}" ]; then
        echo "ERROR: direct fcp-indi probe errors:" 1>&2
        cat "${probe_errors_file}" 1>&2
    fi

    remote_uuid=$(get_fcpindi_remote_uuid)
    retry_failed=0
    if [ -s "${missing_file}" ]; then
        cat "${missing_file}" 1>&2
        while IFS= read -r path; do
            if [ -z "${path}" ]; then
                continue
            fi
            if ! upload_fcpindi_path_with_probe "${path}" "${remote_uuid}"; then
                retry_failed=1
            fi
        done < "${missing_file}"
    fi
    if [ "${retry_failed}" -ne 0 ] || [ -s "${probe_errors_file}" ]; then
        rm -rf "${verify_dir}"
        return 1
    fi

    # The retry must leave both direct S3 probes and fsck clean. If not, fail
    # before pushing the result branch so this session can be resubmitted.
    probe_fcpindi_content "${present_batch_file}" "${missing_file}" "${probe_errors_file}"
    if [ -s "${missing_file}" ] || [ -s "${probe_errors_file}" ]; then
        echo "ERROR: content still missing or unverifiable on fcp-indi after retry:" 1>&2
        cat "${missing_file}" "${probe_errors_file}" 1>&2
        rm -rf "${verify_dir}"
        return 1
    fi
    git annex fsck --fast -f fcp-indi
    rm -rf "${verify_dir}"
}

# Optional repair mode for missing, truncated, or corrupt fcp-indi objects.
# FORCE_UPLOAD_PATHS points to a newline-delimited list of `path` or
# `path<TAB>key` lines; only paths for this job's publication unit are handled.
# Each matching object is uploaded again over the existing one; when a key is
# given, the job's re-extracted file must reproduce exactly that key.
force_upload_listed_fcpindi_content() {
    local remote_uuid matched failed path expected_key

    if [ -z "${FORCE_UPLOAD_PATHS}" ]; then
        return 0
    fi
    if [ ! -s "${FORCE_UPLOAD_PATHS}" ]; then
        echo "ERROR: FORCE_UPLOAD_PATHS is empty or unreadable: ${FORCE_UPLOAD_PATHS}" 1>&2
        return 1
    fi

    remote_uuid=$(get_fcpindi_remote_uuid)

    matched=0
    failed=0
    while IFS=$'\t' read -r path expected_key; do
        # Tolerate CRLF line endings (files written by Windows tools or Python csv).
        path="${path%$'\r'}"
        expected_key="${expected_key%$'\r'}"
        # Match the unit as a whole path component or name prefix, so sub-1
        # does not also claim sub-10 (or sub-1_ses-1 claim sub-1_ses-10).
        if [ -z "${path}" ] || ! printf '%s\n' "${path}" | grep -Eq "(^|/)${subses}([/_.]|\$)"; then
            continue
        fi

        matched=$((matched + 1))
        if ! upload_fcpindi_path_with_probe "${path}" "${remote_uuid}" "${expected_key}"; then
            failed=1
        fi
    done < "${FORCE_UPLOAD_PATHS}"

    if [ "${matched}" -eq 0 ]; then
        echo "ERROR: no FORCE_UPLOAD_PATHS entries matched ${subses}." 1>&2
        return 1
    fi
    if [ "${failed}" -ne 0 ]; then
        echo "ERROR: force-upload failed for one or more ${subses} paths." 1>&2
        return 1
    fi

    echo "Force-uploaded ${matched} ${subses} path(s) to fcp-indi."
}

# Verify that the unzip commit holds every file of the zip. `datalad run` has
# recorded exit 0 for a commit that held only part of a zip, and the fcp-indi
# checks only see what was committed. get_files.sh --expected-paths maps the
# zip listing through the same renames and deletes as the unzip; each of those
# paths must be in HEAD, and the run must leave nothing unsaved.
verify_unzip_commit() {
    local zip="$1"
    local check_dir expected_file committed_file missing_file status_file
    local n_expected failed

    check_dir=$(mktemp -d .git/unzip-check.XXXXXX)
    expected_file="${check_dir}/expected.txt"
    committed_file="${check_dir}/committed.txt"
    missing_file="${check_dir}/missing.txt"
    status_file="${check_dir}/status.txt"

    if ! bash code/get_files.sh --expected-paths "${zip}" > "${expected_file}.unsorted"; then
        echo "ERROR: code/get_files.sh --expected-paths failed for ${zip}." 1>&2
        echo "       If code/get_files.sh predates this check, copy in the current scripts/get_files.<NAME>.sh." 1>&2
        rm -rf "${check_dir}"
        return 1
    fi
    LC_ALL=C sort -u "${expected_file}.unsorted" > "${expected_file}"
    n_expected=$(($(wc -l < "${expected_file}")))

    git ls-tree -r -z --name-only HEAD | tr '\0' '\n' | LC_ALL=C sort > "${committed_file}"
    LC_ALL=C comm -23 "${expected_file}" "${committed_file}" > "${missing_file}"
    git status --porcelain > "${status_file}"

    failed=0
    if [ "${n_expected}" -eq 0 ]; then
        echo "ERROR: code/get_files.sh --expected-paths listed no files for ${zip}." 1>&2
        failed=1
    fi
    if [ -s "${missing_file}" ]; then
        echo "ERROR: unzip commit for ${subses} lacks $(($(wc -l < "${missing_file}"))) of ${n_expected} files from ${zip}:" 1>&2
        cat "${missing_file}" 1>&2
        failed=1
    fi
    if [ -s "${status_file}" ]; then
        echo "ERROR: unzip of ${subses} left changes that were not committed (git status --porcelain):" 1>&2
        cat "${status_file}" 1>&2
        failed=1
    fi
    rm -rf "${check_dir}"
    if [ "${failed}" -ne 0 ]; then
        return 1
    fi
    echo "Unzip commit for ${subses} holds all ${n_expected} files from ${zip}."
}

mkdir "${BRANCH}"
cd "${BRANCH}"

# Clone the analysis dataset from the INPUT store (not the push target, to
# avoid a throughput bottleneck on the output store).
datalad clone "${dssource}" ds
cd ds
git remote add outputstore "${pushgitremote}"
git checkout -b "${BRANCH}"

# Install the input subdataset (git tree only), then fetch and prepare only this
# job's publication unit.
datalad get -n inputs/data
case "${INPUT_LAYOUT}" in
    zip)
        ZIP=$(find inputs/data -name "${subses}_*.zip" | head -n 1)
        if [ -z "${ZIP}" ]; then
            echo "ERROR: no zip found for ${subses} under inputs/data" 1>&2
            exit 1
        fi
        datalad get "${ZIP}"

        # Unzip + rename + de-duplicate, recorded as provenance.
        datalad run -m "unzip ${subses}" "bash code/get_files.sh ${ZIP}"
        # A partial unzip commit must fail here, before any S3 upload or branch
        # push, so find_missing.sh resubmits this subject-session.
        verify_unzip_commit "${ZIP}"
        ;;
    bids)
        if ! printf '%s\n' "${subses}" | grep -Eq '^sub-[A-Za-z0-9]+$'; then
            echo "ERROR: invalid BIDS subject directory name: ${subses}" 1>&2
            exit 1
        fi
        subject_source="inputs/data/${subses}"
        if [ ! -d "${subject_source}" ]; then
            echo "ERROR: BIDS subject directory not found: ${subject_source}" 1>&2
            exit 1
        fi

        # Fetch every annexed file below this subject, dereference the source
        # annex symlinks into the output dataset, and annex the copied tree.
        # No other source path is copied.
        datalad get -r "${subject_source}"
        cp -RL -- "${subject_source}" .
        datalad save -m "Copy ${subses} BIDS data without extraction" -- "${subses}"
        ;;
esac

# Push file CONTENT to S3 first -- no lock needed, content writes don't conflict.
# A transient partial failure is repaired/validated below; only the final
# verification result decides whether the success branch can be pushed.
push_status=0
datalad push --to fcp-indi || push_status=$?
if [ "${push_status}" -ne 0 ]; then
    echo "WARNING: datalad push to fcp-indi exited ${push_status}; verifying and retrying missing content." 1>&2
fi
force_upload_listed_fcpindi_content
# If content is still missing, exit before the branch push so find_missing.sh
# can resubmit this publication unit. Partial S3 uploads are safe: annex keys are
# content-addressed, so retries reuse existing keys and upload only misses.
verify_fcpindi_content

# Repair-only runs fix S3 content for keys that are already published. Nothing in
# git changes, so there is no result branch to merge or publish.
if [ "${SKIP_RESULT_PUSH}" = "1" ]; then
    echo "SUCCESS ${subses} (repair only; result branch not pushed)"
    exit 0
fi

# Push the result BRANCH second -- locked, to serialize git ref updates. Once
# this succeeds, find_missing.sh considers the publication unit complete.
flock "${DSLOCKFILE}" git push outputstore "${BRANCH}"

echo "SUCCESS ${subses}"
# Scratch clone is removed by the EXIT trap.
