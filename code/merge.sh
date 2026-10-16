#!/bin/bash
# =============================================================================
# Merge all per-subject-session result branches and reconcile S3 availability.
#
# Run from the analysis dataset directory, after find_missing.sh reports 0
# missing. Builds a local pseudo merge, repairs scalable annex presence records,
# verifies the proposed merge with one final fast fsck, pushes only after that
# gate passes, and deletes merged job-* branches after the verified push.
# =============================================================================
set -e -u
# shellcheck disable=SC1091
source code/job_config.sh
# shellcheck disable=SC2154
annex_remote="${S3_REMOTE:-fcp-indi}"
pipeline_label="${PIPELINE:-${GET_FILES}}"
BATCH_SIZE=1000
BRANCH_DELETE_BATCH_SIZE=100
REMOTE_REF_LOCK_STALE_MINUTES="${REMOTE_REF_LOCK_STALE_MINUTES:-10}"
ANNEX_FSCK_JOBS="${ANNEX_FSCK_JOBS:-4}"
FSCK_TRANSIENT_MAX_RETRIES="${FSCK_TRANSIENT_MAX_RETRIES:-4}"
FSCK_TRANSIENT_RETRY_SLEEP_SECONDS="${FSCK_TRANSIENT_RETRY_SLEEP_SECONDS:-30}"
FSCK_RETRY_PATH_BATCH_SIZE="${FSCK_RETRY_PATH_BATCH_SIZE:-1000}"
MERGE_TMPDIR="${MERGE_TMPDIR:-/cbica/comp_space/pennlinc_rbc}"
TMPD=""
HAS_NEW_BRANCHES=0
NEEDS_PUSH=0
NEEDS_ANNEX_PUSH=0
LAST_PUSH_REF_LOCK_ERROR=0
PUSH_DELETE_SKIPPED_REFS_FILE=""
RUN_SUMMARY_FILE=""
REMOTE_REF_LOCKS_REMOVED=0
REMOTE_REF_LOCKS_LEFT=0

# Load fcp-indi credentials before the clone so the S3 remote auto-enables.
# shellcheck disable=SC1091
source code/load_credentials.sh || exit 1

# Record job completion state, but defer enforcement until merge_ds has been
# inspected. A successful merge deletes job-* branches, so a recovery rerun after
# cleanup can legitimately report every subject-session as missing.
FIND_MISSING_QUIET=1 bash code/find_missing.sh
MISSING_JOBS=0
if [ -s code/missing.txt ]; then
    MISSING_JOBS=$(wc -l < code/missing.txt)
fi

cleanup_tmpd() {
    if [ -n "${TMPD}" ]; then
        rm -rf "${TMPD}"
    fi
}

init_run_summary() {
    RUN_SUMMARY_FILE="${TMPD}/merge-summary.txt"
    : > "${RUN_SUMMARY_FILE}"
}

append_run_summary() {
    if [ -n "${RUN_SUMMARY_FILE}" ]; then
        printf '%s\n' "$@" >> "${RUN_SUMMARY_FILE}" || true
    fi
}

make_merge_tmpd() {
    if ! mkdir -p "${MERGE_TMPDIR}" 2>/dev/null; then
        echo "ERROR: could not create MERGE_TMPDIR=${MERGE_TMPDIR}." 1>&2
        echo "Set MERGE_TMPDIR to a writable filesystem with enough space and re-run merge.sh." 1>&2
        exit 1
    fi

    if [ ! -w "${MERGE_TMPDIR}" ]; then
        echo "ERROR: MERGE_TMPDIR is not writable: ${MERGE_TMPDIR}" 1>&2
        echo "Set MERGE_TMPDIR to a writable filesystem with enough space and re-run merge.sh." 1>&2
        exit 1
    fi

    mktemp -d "${MERGE_TMPDIR%/}/merge.${SLURM_JOB_ID:-$$}.XXXXXXXXXX"
}

print_run_summary() {
    exit_status="$1"
    has_summary=0

    if [ -n "${RUN_SUMMARY_FILE}" ] && [ -s "${RUN_SUMMARY_FILE}" ]; then
        has_summary=1
    fi

    if [ "${has_summary}" -eq 0 ] \
        && [ "${REMOTE_REF_LOCKS_REMOVED}" -eq 0 ] \
        && [ "${REMOTE_REF_LOCKS_LEFT}" -eq 0 ]; then
        return 0
    fi

    echo ""
    if [ "${exit_status}" -eq 0 ]; then
        echo "Merge summary:"
    else
        echo "Merge summary (exit ${exit_status}):"
    fi

    if [ "${has_summary}" -eq 1 ]; then
        sed 's/^/  /' "${RUN_SUMMARY_FILE}"
    fi

    if [ "${REMOTE_REF_LOCKS_REMOVED}" -gt 0 ] || [ "${REMOTE_REF_LOCKS_LEFT}" -gt 0 ]; then
        echo "  Remote ref lock cleanup: removed ${REMOTE_REF_LOCKS_REMOVED} stale lock(s); left ${REMOTE_REF_LOCKS_LEFT} newer lock(s)."
    fi
}

finish_run() {
    exit_status=$?
    set +e
    print_run_summary "${exit_status}"
    cleanup_tmpd
    exit "${exit_status}"
}
trap finish_run EXIT

# Fast path: synthesize batched merge commits with a temporary index instead of
# invoking Git's recursive merge machinery on every branch tree.

# Emit canonical tree entries for known shared metadata/reference files in one
# ref. Keep this narrow: subject/session outputs must still conflict loudly if
# get_files.sh missed a needed rename.
emit_root_log_auto_keep_entries_for_ref() {
    ref="$1"

    git ls-tree "${ref}" 2>/dev/null | awk -F '\t' '
        $2 == "dataset_description.json" || $2 == ".bidsignore" || $2 ~ /^desc-.*_dseg\.tsv$/ {
            split($1, meta, " ")
            printf "%s\t%s\t%s\t%s\n", $2, meta[1], meta[2], meta[3]
        }
    '

    git ls-tree -r "${ref}" -- logs 2>/dev/null | awk -F '\t' '
        $2 ~ /^logs\/CITATION\./ {
            split($1, meta, " ")
            printf "%s\t%s\t%s\t%s\n", $2, meta[1], meta[2], meta[3]
        }
    '
}

emit_fsaverage_auto_keep_entries_for_ref() {
    ref="$1"

    git ls-tree -r "${ref}" -- sourcedata/freesurfer/fsaverage 2>/dev/null | awk -F '\t' '
        $2 ~ /^sourcedata\/freesurfer\/fsaverage\// {
            split($1, meta, " ")
            printf "%s\t%s\t%s\t%s\n", $2, meta[1], meta[2], meta[3]
        }
    '
}

sort_unique_auto_keep_entries() {
    input_file="$1"
    output_file="$2"

    LC_ALL=C sort -s -t "$(printf '\t')" -k1,1 -u "${input_file}" > "${output_file}"
}

# Write canonical tree entries for auto-kept paths as TSV:
# path, mode, object type, object id. These entries are used by both merge
# strategies to keep one copy of shared metadata/reference files.
find_auto_keep_entries() {
    entries_file="$1"
    raw_entries_file="${TMPD}/auto_keep_entries.raw.tsv"
    fsaverage_entries_file="${TMPD}/auto_keep_fsaverage.raw.tsv"

    : > "${raw_entries_file}"
    : > "${fsaverage_entries_file}"

    emit_root_log_auto_keep_entries_for_ref HEAD >> "${raw_entries_file}"
    while read -r branch; do
        emit_root_log_auto_keep_entries_for_ref "${branch}" >> "${raw_entries_file}"
    done < "${TMPD}/has_results.txt"

    # fsaverage can contain many files repeated in every branch. It should be a
    # shared reference tree, so take the first available copy instead of scanning
    # thousands of identical branch copies.
    emit_fsaverage_auto_keep_entries_for_ref HEAD >> "${fsaverage_entries_file}"
    if [ ! -s "${fsaverage_entries_file}" ]; then
        while read -r branch; do
            emit_fsaverage_auto_keep_entries_for_ref "${branch}" >> "${fsaverage_entries_file}"
            if [ -s "${fsaverage_entries_file}" ]; then
                break
            fi
        done < "${TMPD}/has_results.txt"
    fi
    cat "${fsaverage_entries_file}" >> "${raw_entries_file}"

    sort_unique_auto_keep_entries "${raw_entries_file}" "${entries_file}"
}

write_index_info_from_auto_keep_entries() {
    entries_file="$1"
    index_info_file="$2"

    awk -F '\t' '{ printf "%s %s\t%s\n", $2, $4, $1 }' "${entries_file}" > "${index_info_file}"
}

apply_auto_keep_entries_to_index() {
    index_file="$1"
    entries_file="$2"
    index_info_file="${TMPD}/auto_keep.index-info"

    if [ ! -s "${entries_file}" ]; then
        return 0
    fi

    write_index_info_from_auto_keep_entries "${entries_file}" "${index_info_file}"
    GIT_INDEX_FILE="${index_file}" git update-index --index-info < "${index_info_file}"
}

split_result_branches() {
    prefix="$1"

    awk -v batch_size="${BATCH_SIZE}" -v prefix="${prefix}" '
        {
            chunk = int((NR - 1) / batch_size)
            printf "%s\n", $0 > sprintf("%s%06d", prefix, chunk)
        }
    ' "${TMPD}/has_results.txt"
}

write_seen_entries_from_index() {
    index_file="$1"
    seen_file="$2"

    GIT_INDEX_FILE="${index_file}" git ls-files -s | awk -F '\t' '
        {
            split($1, meta, " ")
            printf "%s\t%s\t%s\n", $2, meta[1], meta[2]
        }
    ' > "${seen_file}"
}

collect_branch_raw_changes() {
    chunk="$1"
    raw_file="$2"
    ownership_file="$3"

    : > "${raw_file}"
    while read -r branch; do
        branch_base=$(git merge-base HEAD "${branch}")
        git diff-tree --no-commit-id -r --raw --no-renames "${branch_base}" "${branch}" \
            | awk -v branch="${branch}" -v raw_file="${raw_file}" -v ownership_file="${ownership_file}" '
                BEGIN { FS = OFS = "\t" }

                function is_auto_keep_path(path) {
                    return path == "dataset_description.json" \
                        || path == ".bidsignore" \
                        || path ~ /^desc-.*_dseg\.tsv$/ \
                        || path ~ /^logs\/CITATION\./ \
                        || path ~ /^sourcedata\/freesurfer\/fsaverage\//
                }

                {
                    print branch, $0 >> raw_file
                    path = $2
                    split($1, meta, " ")
                    status = meta[5]
                    if (path != "" && status !~ /^D/ && !is_auto_keep_path(path)) {
                        print branch, path >> ownership_file
                    }
                }
            '
    done < "${chunk}"
}

write_index_updates_for_raw_changes() {
    seen_file="$1"
    raw_file="$2"
    updates_index_file="$3"
    updates_seen_file="$4"
    conflicts_file="$5"

    : > "${updates_index_file}"
    : > "${updates_seen_file}"
    : > "${conflicts_file}"

    awk -F '\t' \
        -v updates_index_file="${updates_index_file}" \
        -v updates_seen_file="${updates_seen_file}" \
        -v conflicts_file="${conflicts_file}" '
        FILENAME == ARGV[1] {
            seen[$1] = $2 "\t" $3
            next
        }

        function is_auto_keep_path(path) {
            return path == "dataset_description.json" \
                || path == ".bidsignore" \
                || path ~ /^desc-.*_dseg\.tsv$/ \
                || path ~ /^logs\/CITATION\./ \
                || path ~ /^sourcedata\/freesurfer\/fsaverage\//
        }

        {
            branch = $1
            meta_line = $2
            path = $3
            if (path == "" || is_auto_keep_path(path)) {
                next
            }

            split(meta_line, meta, " ")
            new_mode = meta[2]
            new_oid = meta[4]
            status = meta[5]

            if (status ~ /^D/ || new_mode == "000000" || new_oid ~ /^0+$/) {
                printf "%s\t%s\tunsupported deletion for non-auto-kept path\n", branch, path >> conflicts_file
                next
            }

            entry = new_mode "\t" new_oid
            if ((path in seen) && seen[path] != entry) {
                printf "%s\t%s\tconflicts with existing entry %s; new entry %s\n", branch, path, seen[path], entry >> conflicts_file
                next
            }

            seen[path] = entry
            updates[path] = entry
        }

        END {
            for (path in updates) {
                split(updates[path], entry_parts, "\t")
                printf "%s %s\t%s\n", entry_parts[1], entry_parts[2], path >> updates_index_file
                printf "%s\t%s\t%s\n", path, entry_parts[1], entry_parts[2] >> updates_seen_file
            }
        }
    ' "${seen_file}" "${raw_file}"
}

commit_index_batch() {
    index_file="$1"
    current_commit="$2"
    chunk="$3"
    chunk_label="$4"

    tree=$(GIT_INDEX_FILE="${index_file}" git write-tree)
    # shellcheck disable=SC2046
    git commit-tree "${tree}" -p "${current_commit}" $(awk '{ printf "-p %s ", $0 }' "${chunk}") \
        -m "Merge ${pipeline_label} data for $(wc -l < "${chunk}") subject-sessions: ${chunk_label}"
}

merge_all_by_index() {
    index_file="${TMPD}/merge.index"
    seen_file="${TMPD}/merge.seen.tsv"
    current_commit=$(git rev-parse HEAD)

    rm -f "${index_file}"
    GIT_INDEX_FILE="${index_file}" git read-tree HEAD
    apply_auto_keep_entries_to_index "${index_file}" "${TMPD}/auto_keep_entries.tsv"
    write_seen_entries_from_index "${index_file}" "${seen_file}"

    split_result_branches "${TMPD}/__batch_"
    for chunk in "${TMPD}"/__batch_*; do
        chunk_label=$(basename "${chunk}")
        raw_file="${TMPD}/index_${chunk_label}.raw"
        updates_index_file="${TMPD}/index_${chunk_label}.updates"
        updates_seen_file="${TMPD}/index_${chunk_label}.seen"
        conflicts_file="${TMPD}/index_${chunk_label}.conflicts"

        collect_branch_raw_changes "${chunk}" "${raw_file}" "${TMPD}/branch-paths.tsv"
        write_index_updates_for_raw_changes \
            "${seen_file}" \
            "${raw_file}" \
            "${updates_index_file}" \
            "${updates_seen_file}" \
            "${conflicts_file}"

        if [ -s "${conflicts_file}" ]; then
            echo "ERROR: non-auto-kept path conflicts while synthesizing ${chunk_label}:" 1>&2
            cat "${conflicts_file}" 1>&2
            echo "Fix code/get_files.sh or resolve the conflicting outputs, then rerun merge.sh." 1>&2
            exit 1
        fi

        if [ -s "${updates_index_file}" ]; then
            GIT_INDEX_FILE="${index_file}" git update-index --index-info < "${updates_index_file}"
            cat "${updates_seen_file}" >> "${seen_file}"
        fi

        current_commit=$(commit_index_batch "${index_file}" "${current_commit}" "${chunk}" "${chunk_label}")
    done

    git reset --hard "${current_commit}"
}

get_output_main_ref() {
    upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null || true)
    if [ -n "${upstream}" ]; then
        echo "${upstream}"
        return 0
    fi

    for candidate in origin/main origin/master; do
        if git rev-parse -q --verify "${candidate}" >/dev/null; then
            echo "${candidate}"
            return 0
        fi
    done

    return 1
}

align_with_output_main() {
    output_main_ref=$(get_output_main_ref || true)
    if [ -z "${output_main_ref}" ]; then
        echo "ERROR: could not determine the output RIA main branch for merge_ds." 1>&2
        echo "Check the origin remote and re-run merge.sh." 1>&2
        exit 1
    fi

    if git merge-base --is-ancestor HEAD "${output_main_ref}"; then
        if [ "$(git rev-parse HEAD)" != "$(git rev-parse "${output_main_ref}")" ]; then
            echo "Fast-forwarding merge_ds to ${output_main_ref}"
            git reset --hard "${output_main_ref}"
        fi
        NEEDS_PUSH=0
        return 0
    fi

    if git merge-base --is-ancestor "${output_main_ref}" HEAD; then
        echo "merge_ds already contains ${output_main_ref}"
        if [ "$(git rev-parse HEAD)" != "$(git rev-parse "${output_main_ref}")" ]; then
            NEEDS_PUSH=1
        fi
        return 0
    fi

    echo "ERROR: merge_ds history has diverged from ${output_main_ref}." 1>&2
    echo "Inspect merge_ds manually, or delete merge_ds and re-run merge.sh to clone the output RIA again." 1>&2
    exit 1
}

collect_new_result_branches() {
    : > "${TMPD}/has_results.txt"

    git for-each-ref \
        --format='%(refname:short)' \
        --no-merged HEAD \
        'refs/remotes/origin/job-*' > "${TMPD}/has_results.txt"

    n_results=$(wc -l < "${TMPD}/has_results.txt")
    if [ "${n_results}" -gt 0 ]; then
        HAS_NEW_BRANCHES=1
        echo "Merging ${n_results} new result branches"
        append_run_summary "Result branches: ${n_results} new branch(es) queued for merge."
    else
        HAS_NEW_BRANCHES=0
        echo "No new result branches to merge; rechecking annex availability in existing history."
        append_run_summary "Result branches: no new branches; rechecking existing merge history."
    fi
}

get_output_branch_name() {
    upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null || true)
    case "${upstream}" in
        origin/*)
            echo "${upstream#origin/}"
            return 0
            ;;
    esac

    for candidate in main master; do
        if git rev-parse -q --verify "refs/remotes/origin/${candidate}" >/dev/null; then
            echo "${candidate}"
            return 0
        fi
    done

    current=$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)
    if [ -n "${current}" ] && git rev-parse -q --verify "refs/remotes/origin/${current}" >/dev/null; then
        echo "${current}"
        return 0
    fi

    return 1
}

fetch_output_refs() {
    output_branch=$(get_output_branch_name || true)
    if [ -n "${output_branch}" ]; then
        git fetch --prune origin \
            "+refs/heads/${output_branch}:refs/remotes/origin/${output_branch}" \
            '+refs/heads/job-*:refs/remotes/origin/job-*'
    else
        git fetch --prune origin '+refs/heads/*:refs/remotes/origin/*'
    fi

    git fetch origin '+refs/heads/git-annex:refs/remotes/origin/git-annex' 2>/dev/null || true
}

sync_annex_tracking_branch() {
    if ! git rev-parse -q --verify git-annex >/dev/null; then
        if git rev-parse -q --verify refs/remotes/origin/git-annex >/dev/null; then
            git update-ref refs/heads/git-annex refs/remotes/origin/git-annex
        fi
        return 0
    fi

    if ! git rev-parse -q --verify refs/remotes/origin/git-annex >/dev/null; then
        NEEDS_ANNEX_PUSH=1
        return 0
    fi

    if git merge-base --is-ancestor git-annex refs/remotes/origin/git-annex; then
        git update-ref refs/heads/git-annex refs/remotes/origin/git-annex
        return 0
    fi

    if git merge-base --is-ancestor refs/remotes/origin/git-annex git-annex; then
        NEEDS_ANNEX_PUSH=1
        return 0
    fi

    backup_ref="refs/merge-local-git-annex/$(date +%s)"
    echo "WARNING: local git-annex branch is unrelated to origin/git-annex; backing it up at ${backup_ref} and using origin/git-annex." 1>&2
    git update-ref "${backup_ref}" git-annex
    git update-ref refs/heads/git-annex refs/remotes/origin/git-annex
}

ensure_merge_clone_ready() {
    if git rev-parse -q --verify MERGE_HEAD >/dev/null; then
        echo "ERROR: merge_ds has an unfinished Git merge. Resolve or remove merge_ds, then re-run merge.sh." 1>&2
        exit 1
    fi
    fetch_output_refs
    align_with_output_main
    sync_annex_tracking_branch
    git annex enableremote "${annex_remote}" 2>/dev/null || true
    TMPD=$(make_merge_tmpd)
    echo "Merge temporary directory: ${TMPD}"
    init_run_summary
    : > "${TMPD}/branch-paths.tsv"
    collect_new_result_branches
}

prepare_full_merge_clone() {
    # Clone the output RIA. If output main already contains previous merge work,
    # the branch collector below skips those ancestors and keeps only new results.
    rm -rf merge_ds
    # shellcheck disable=SC2154
    datalad clone "${output_clone_source}" merge_ds
    cd merge_ds
    ensure_merge_clone_ready
}

prepare_existing_merge_clone() {
    cd merge_ds
    ensure_merge_clone_ready
}

write_affected_subses() {
    input_file="$1"
    output_file="$2"

    grep -oE 'sub-[A-Za-z0-9]+(_ses-[A-Za-z0-9]+)?' "${input_file}" | sort -u > "${output_file}" || true
}

strip_origin_branch() {
    branch="$1"

    case "${branch}" in
        origin/*)
            echo "${branch#origin/}"
            ;;
        refs/remotes/origin/*)
            echo "${branch#refs/remotes/origin/}"
            ;;
        refs/heads/*)
            echo "${branch#refs/heads/}"
            ;;
        *)
            echo "${branch}"
            ;;
    esac
}

get_local_origin_git_dir() {
    remote_url=$(git remote get-url --push origin 2>/dev/null || git remote get-url origin 2>/dev/null || true)
    remote_path=""

    if [ -z "${remote_url}" ]; then
        return 1
    fi

    remote_url="${remote_url%%#*}"
    case "${remote_url}" in
        file://*)
            remote_path="${remote_url#file://}"
            ;;
        /*|./*|../*)
            remote_path="${remote_url}"
            ;;
        *)
            return 1
            ;;
    esac

    if [ -d "${remote_path}/refs" ] && [ -d "${remote_path}/objects" ]; then
        echo "${remote_path}"
        return 0
    fi

    if [ -d "${remote_path}/.git/refs" ] && [ -d "${remote_path}/.git/objects" ]; then
        echo "${remote_path}/.git"
        return 0
    fi

    return 1
}

push_origin_with_lock() {
    if [ -n "${DSLOCKFILE:-}" ]; then
        {
            flock 9
            git push origin "$@"
        } 9>"${DSLOCKFILE}"
    else
        git push origin "$@"
    fi
}

push_log_has_ref_lock_error() {
    push_log="$1"

    grep -Eq "Unable to create '[^']*\.lock': File exists|cannot lock ref 'refs/heads/job-[^']*'.*File exists|refs/heads/job-[^']*\.lock': File exists" "${push_log}"
}

remote_ref_lock_is_stale() {
    lock_file="$1"

    case "${REMOTE_REF_LOCK_STALE_MINUTES}" in
        ''|*[!0-9]*)
            echo "WARNING: REMOTE_REF_LOCK_STALE_MINUTES must be a non-negative integer; using 10." 1>&2
            REMOTE_REF_LOCK_STALE_MINUTES=10
            ;;
    esac

    if [ "${REMOTE_REF_LOCK_STALE_MINUTES}" -eq 0 ]; then
        return 0
    fi

    find "${lock_file}" -type f -mmin "+${REMOTE_REF_LOCK_STALE_MINUTES}" -print -quit | grep -q .
}

cleanup_stale_remote_job_ref_locks() {
    push_log="$1"
    shift
    remote_git_dir=$(get_local_origin_git_dir || true)
    remote_git_dir_real=""
    lock_candidates_file="${TMPD:-/tmp}/remote-lock-candidates-${$}-${RANDOM}.txt"
    sorted_lock_candidates_file="${lock_candidates_file}.sorted"

    if [ -z "${remote_git_dir}" ]; then
        echo "WARNING: origin is not a local git directory; cannot clean stale remote ref locks automatically." 1>&2
        return 0
    fi
    remote_git_dir_real=$(cd "${remote_git_dir}" 2>/dev/null && pwd -P || true)

    : > "${lock_candidates_file}"
    for refspec in "$@"; do
        case "${refspec}" in
            :refs/heads/job-*)
                ref="${refspec#:}"
                printf '%s\n' "${remote_git_dir}/${ref}.lock" >> "${lock_candidates_file}"
                ;;
            *)
                continue
                ;;
        esac
    done

    if [ -f "${push_log}" ]; then
        sed -n "s/.*Unable to create '\\([^']*\\.lock\\)': File exists.*/\\1/p" "${push_log}" >> "${lock_candidates_file}" || true
    fi

    if [ ! -s "${lock_candidates_file}" ]; then
        rm -f "${lock_candidates_file}" "${sorted_lock_candidates_file}"
        return 0
    fi

    LC_ALL=C sort -u "${lock_candidates_file}" > "${sorted_lock_candidates_file}"
    while read -r lock_file; do
        lock_file_clean=$(printf '%s\n' "${lock_file}" | sed 's#/\./#/#g')
        if [ -z "${lock_file}" ]; then
            continue
        fi

        case "${lock_file_clean}" in
            "${remote_git_dir}"/*)
                ;;
            "${remote_git_dir_real}"/*)
                ;;
            *)
                echo "WARNING: remote ref lock ${lock_file_clean} is outside ${remote_git_dir}; skipping cleanup." 1>&2
                continue
                ;;
        esac

        if [ ! -e "${lock_file_clean}" ]; then
            continue
        fi

        if remote_ref_lock_is_stale "${lock_file_clean}"; then
            echo "Removing stale remote ref lock ${lock_file_clean}" 1>&2
            rm -f "${lock_file_clean}"
            REMOTE_REF_LOCKS_REMOVED=$((REMOTE_REF_LOCKS_REMOVED + 1))
        else
            echo "WARNING: remote ref lock ${lock_file_clean} is newer than ${REMOTE_REF_LOCK_STALE_MINUTES} minute(s); leaving it in place." 1>&2
            REMOTE_REF_LOCKS_LEFT=$((REMOTE_REF_LOCKS_LEFT + 1))
        fi
    done < "${sorted_lock_candidates_file}"
    rm -f "${lock_candidates_file}" "${sorted_lock_candidates_file}"
}

push_origin_with_stale_ref_lock_retry_unlocked() {
    push_log="${TMPD}/git-push-origin-${$}-${RANDOM}.log"
    retry_log="${TMPD}/git-push-origin-retry-${$}-${RANDOM}.log"
    LAST_PUSH_REF_LOCK_ERROR=0

    if git push origin "$@" > "${push_log}" 2>&1; then
        cat "${push_log}"
        rm -f "${push_log}" "${retry_log}"
        return 0
    else
        push_status=$?
    fi
    cat "${push_log}" 1>&2

    if ! push_log_has_ref_lock_error "${push_log}"; then
        rm -f "${push_log}" "${retry_log}"
        return "${push_status}"
    fi

    LAST_PUSH_REF_LOCK_ERROR=1
    cleanup_stale_remote_job_ref_locks "${push_log}" "$@"
    echo "Retrying git push after remote ref lock cleanup." 1>&2
    if git push origin "$@" > "${retry_log}" 2>&1; then
        cat "${retry_log}"
        rm -f "${push_log}" "${retry_log}"
        return 0
    else
        push_status=$?
    fi

    cat "${retry_log}" 1>&2
    if ! push_log_has_ref_lock_error "${retry_log}"; then
        LAST_PUSH_REF_LOCK_ERROR=0
    fi
    rm -f "${push_log}" "${retry_log}"
    return "${push_status}"
}

push_origin_with_stale_ref_lock_retry() {
    if [ -n "${DSLOCKFILE:-}" ]; then
        push_status=0
        {
            flock 9
            push_origin_with_stale_ref_lock_retry_unlocked "$@" || push_status=$?
        } 9>"${DSLOCKFILE}"
        return "${push_status}"
    fi

    push_origin_with_stale_ref_lock_retry_unlocked "$@"
}

record_skipped_delete_refspec() {
    refspec="$1"

    if [ -n "${PUSH_DELETE_SKIPPED_REFS_FILE}" ]; then
        printf '%s\n' "${refspec}" >> "${PUSH_DELETE_SKIPPED_REFS_FILE}"
    fi
}

push_delete_refspec_batch() {
    if push_origin_with_stale_ref_lock_retry "$@"; then
        return 0
    else
        push_status=$?
    fi

    if [ "${LAST_PUSH_REF_LOCK_ERROR}" != "1" ]; then
        return "${push_status}"
    fi

    echo "WARNING: batch branch deletion hit remote ref locks; retrying deletions one branch at a time." 1>&2
    for refspec in "$@"; do
        if push_origin_with_stale_ref_lock_retry "${refspec}"; then
            continue
        else
            push_status=$?
        fi

        if [ "${LAST_PUSH_REF_LOCK_ERROR}" = "1" ]; then
            record_skipped_delete_refspec "${refspec}"
            echo "WARNING: leaving ${refspec#:} on origin because its remote ref lock could not be cleared." 1>&2
            continue
        fi

        return "${push_status}"
    done

    return 0
}

delete_remote_result_branches() {
    branches_file="$1"
    reason="$2"
    refspecs_file="${TMPD}/delete-refspecs.txt"
    skipped_refspecs_file="${TMPD}/delete-skipped-refspecs.txt"
    skipped_local_refs_file="${TMPD}/delete-skipped-local-refs.txt"
    batch=()

    if [ ! -s "${branches_file}" ]; then
        append_run_summary "Branch deletion (${reason}): no candidate result branches."
        return 0
    fi

    : > "${refspecs_file}"
    : > "${skipped_refspecs_file}"
    : > "${skipped_local_refs_file}"
    PUSH_DELETE_SKIPPED_REFS_FILE="${skipped_refspecs_file}"
    while read -r branch; do
        if [ -z "${branch}" ]; then
            continue
        fi
        remote_branch=$(strip_origin_branch "${branch}")
        case "${remote_branch}" in
            job-*)
                printf ':refs/heads/%s\n' "${remote_branch}" >> "${refspecs_file}"
                ;;
            *)
                echo "WARNING: refusing to delete non-job branch '${branch}' while ${reason}." 1>&2
                ;;
        esac
    done < "${branches_file}"

    if [ ! -s "${refspecs_file}" ]; then
        PUSH_DELETE_SKIPPED_REFS_FILE=""
        append_run_summary "Branch deletion (${reason}): no valid job-* branch refspecs to delete."
        return 0
    fi

    delete_requested_count=$(wc -l < "${refspecs_file}")
    echo "Deleting ${delete_requested_count} ${reason} result branch(es) from origin."
    while read -r refspec; do
        batch+=("${refspec}")
        if [ "${#batch[@]}" -ge "${BRANCH_DELETE_BATCH_SIZE}" ]; then
            push_delete_refspec_batch "${batch[@]}"
            batch=()
        fi
    done < "${refspecs_file}"
    if [ "${#batch[@]}" -gt 0 ]; then
        push_delete_refspec_batch "${batch[@]}"
    fi
    PUSH_DELETE_SKIPPED_REFS_FILE=""

    while read -r refspec; do
        if [ -z "${refspec}" ]; then
            continue
        fi
        ref="${refspec#:}"
        remote_branch="${ref#refs/heads/}"
        printf 'refs/remotes/origin/%s\n' "${remote_branch}" >> "${skipped_local_refs_file}"
    done < "${skipped_refspecs_file}"

    skipped_count=$(wc -l < "${skipped_refspecs_file}")
    completed_count=$((delete_requested_count - skipped_count))
    skipped_report=""
    if [ -s "${skipped_refspecs_file}" ]; then
        branch_report_dir="../code/merge-branch-deletion"
        mkdir -p "${branch_report_dir}"
        skipped_report="${branch_report_dir}/skipped-${reason}-refspecs.txt"
        cp "${skipped_refspecs_file}" "${skipped_report}"
        echo "WARNING: $(wc -l < "${skipped_refspecs_file}") ${reason} result branch deletion(s) were skipped because remote ref locks remained." 1>&2
        echo "Re-run merge.sh later to retry deleting the remaining merged result branch(es)." 1>&2
    fi

    update_refs_file="${TMPD}/delete-local-remote-refs.stdin"
    : > "${update_refs_file}"
    while read -r branch; do
        if [ -z "${branch}" ]; then
            continue
        fi

        remote_branch=$(strip_origin_branch "${branch}")
        local_ref="refs/remotes/origin/${remote_branch}"
        if grep -Fxq "${local_ref}" "${skipped_local_refs_file}"; then
            continue
        fi

        printf 'delete %s\n' "${local_ref}" >> "${update_refs_file}"
    done < "${branches_file}"
    local_refs_removed_count=$(wc -l < "${update_refs_file}")
    if [ -s "${update_refs_file}" ]; then
        git update-ref --stdin < "${update_refs_file}" 2>/dev/null || true
    fi

    if [ -n "${skipped_report}" ]; then
        append_run_summary "Branch deletion (${reason}): requested ${delete_requested_count}; completed ${completed_count}; skipped ${skipped_count} due remote ref locks; local tracking refs cleaned ${local_refs_removed_count}; skipped refs: ${skipped_report}."
    else
        append_run_summary "Branch deletion (${reason}): requested ${delete_requested_count}; completed ${completed_count}; skipped 0; local tracking refs cleaned ${local_refs_removed_count}."
    fi
}

rollback_local_merge_to_output_main() {
    output_main_ref=$(get_output_main_ref || true)
    if [ -z "${output_main_ref}" ]; then
        echo "WARNING: could not determine output main ref; leaving local pseudo merge in place." 1>&2
        append_run_summary "Rollback: could not determine output main ref; local pseudo merge left in place."
        return 0
    fi

    echo "Rolling local pseudo merge back to ${output_main_ref}; it was not pushed."
    git reset --hard "${output_main_ref}"
    NEEDS_PUSH=0
    append_run_summary "Rollback: local pseudo merge reset to ${output_main_ref}; merged Git history was not pushed."
}

write_bad_result_branches() {
    absent_paths_file="$1"
    bad_branches_file="$2"
    resubmit_file="$3"

    : > "${bad_branches_file}"
    write_affected_subses "${absent_paths_file}" "${resubmit_file}"

    if [ -s "${TMPD}/branch-paths.tsv" ]; then
        awk -F '\t' '
            NR == FNR {
                if ($0 != "") {
                    absent[$0] = 1
                }
                next
            }
            $2 in absent {
                print $1
            }
        ' "${absent_paths_file}" "${TMPD}/branch-paths.tsv" | LC_ALL=C sort -u > "${bad_branches_file}"
    fi

    if [ ! -s "${bad_branches_file}" ] && [ -s "${resubmit_file}" ] && [ -s "${TMPD}/has_results.txt" ]; then
        awk '
            NR == FNR {
                if ($0 != "") {
                    subses[$0] = 1
                }
                next
            }
            {
                for (s in subses) {
                    if (index($0, s) > 0) {
                        print $0
                        next
                    }
                }
            }
        ' "${resubmit_file}" "${TMPD}/has_results.txt" | LC_ALL=C sort -u > "${bad_branches_file}"
    fi
}

handle_absent_content_for_resubmission() {
    absent_paths_file="$1"
    report_dir="$2"
    bad_branches_file="${report_dir}/bad-result-branches.txt"
    resubmit_file="${report_dir}/resubmit-subses.txt"

    write_bad_result_branches "${absent_paths_file}" "${bad_branches_file}" "${resubmit_file}"

    if [ -s "${bad_branches_file}" ]; then
        echo "Content is absent on ${annex_remote}; affected result branches are listed in ${bad_branches_file}." 1>&2
        delete_remote_result_branches "${bad_branches_file}" "bad"
        rollback_local_merge_to_output_main
    else
        echo "WARNING: content is absent on ${annex_remote}, but no owning job branch could be identified." 1>&2
    fi
}

classify_failed_fsck_paths() {
    failures_file="$1"
    failed_paths_file="$2"
    transient_paths_file="$3"

    : > "${failed_paths_file}"
    : > "${transient_paths_file}"
    if [ ! -s "${failures_file}" ]; then
        return 0
    fi

    python3 - "${failures_file}" "${failed_paths_file}" "${transient_paths_file}" <<'PY'
import json
import re
import sys

failures_file, failed_paths_file, transient_paths_file = sys.argv[1:4]

transient_patterns = (
    re.compile(r"\bstatusCode\s*=\s*(429|500|502|503|504)\b"),
    re.compile(r"\b(Service Unavailable|Internal Server Error|Bad Gateway|Gateway Timeout|Too Many Requests)\b", re.I),
    re.compile(r"\b(HttpExceptionRequest|timeout|timed out|temporar|connection reset|connection aborted|ConnectionFailure|ConnectionTimeout|TLS|network)\b", re.I),
)
missing_patterns = (
    re.compile(r"\bstatusCode\s*=\s*404\b"),
    re.compile(r"\b(NoSuchKey|Not Found)\b", re.I),
)

def get_path(record):
    path = record.get("file")
    if isinstance(path, str) and path:
        return path
    inputs = record.get("input")
    if isinstance(inputs, list):
        for candidate in inputs:
            if isinstance(candidate, str) and candidate:
                return candidate
    return ""

failed_paths = set()
transient_paths = set()

with open(failures_file, "r", encoding="utf-8", errors="replace") as f:
    for raw_line in f:
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        path = get_path(record)
        if not path:
            continue

        note = str(record.get("note", ""))
        is_missing = any(p.search(note) for p in missing_patterns)
        is_transient = (not is_missing) and any(p.search(note) for p in transient_patterns)
        if is_transient:
            transient_paths.add(path)
        else:
            failed_paths.add(path)

# Any path with one non-transient failure should stay in failed_paths.
transient_paths.difference_update(failed_paths)

with open(failed_paths_file, "w", encoding="utf-8") as out_failed:
    for path in sorted(failed_paths):
        out_failed.write(path + "\n")

with open(transient_paths_file, "w", encoding="utf-8") as out_transient:
    for path in sorted(transient_paths):
        out_transient.write(path + "\n")
PY
}

ensure_valid_fsck_retry_config() {
    case "${FSCK_TRANSIENT_MAX_RETRIES}" in
        ''|*[!0-9]*)
            echo "WARNING: FSCK_TRANSIENT_MAX_RETRIES must be a non-negative integer; using 2." 1>&2
            FSCK_TRANSIENT_MAX_RETRIES=2
            ;;
    esac

    case "${FSCK_TRANSIENT_RETRY_SLEEP_SECONDS}" in
        ''|*[!0-9]*)
            echo "WARNING: FSCK_TRANSIENT_RETRY_SLEEP_SECONDS must be a non-negative integer; using 30." 1>&2
            FSCK_TRANSIENT_RETRY_SLEEP_SECONDS=30
            ;;
    esac
}

print_fcpindi_resubmit_guidance() {
    report_dir="$1"
    resubmit_file="${report_dir}/resubmit-subses.txt"

    if [ -s "${resubmit_file}" ]; then
        echo "Re-run affected jobs with:" 1>&2
        echo "  bash code/submit_jobs.sh code/merge-fsck-repair/resubmit-subses.txt   # add --local [--n-cpus 8] to run without SLURM" 1>&2
    else
        echo "No subject-session IDs could be extracted for automatic resubmission." 1>&2
        echo "Inspect ${report_dir}/ and repair or resubmit the affected data manually." 1>&2
    fi
    echo "Then re-run merge.sh; it will reuse merge_ds and merge only new result branches." 1>&2
}

handle_failed_fsck_for_resubmission() {
    fsck_label="$1"
    report_dir="../code/merge-fsck-repair"
    failures_file="${report_dir}/fsck-${fsck_label}-failures.jsonl"
    failed_paths_file="${report_dir}/content-absent-from-fsck-${fsck_label}.txt"
    transient_paths_file="${report_dir}/transient-fsck-${fsck_label}.txt"

    classify_failed_fsck_paths "${failures_file}" "${failed_paths_file}" "${transient_paths_file}"
    if [ -s "${transient_paths_file}" ]; then
        append_run_summary "fsck failure classification (${fsck_label}): transient/transport failures $(wc -l < "${transient_paths_file}"); report ${transient_paths_file}."
    fi

    if [ -s "${transient_paths_file}" ] && [ ! -s "${failed_paths_file}" ]; then
        echo "WARNING: fsck ${fsck_label} failed only due to transient ${annex_remote} probe errors." 1>&2
        echo "Transient failure paths: ${transient_paths_file}" 1>&2
        echo "No job branches will be deleted unless fsck shows non-transient failures." 1>&2
        return 2
    fi

    if [ ! -s "${failed_paths_file}" ]; then
        return 0
    fi

    echo "ERROR: final fsck found content that ${annex_remote} is recorded as having, but cannot provide." 1>&2
    echo "Failed content paths: ${failed_paths_file}" 1>&2
    handle_absent_content_for_resubmission "${failed_paths_file}" "${report_dir}"
    print_fcpindi_resubmit_guidance "${report_dir}"
    return 1
}

retry_transient_fsck_paths() {
    fsck_label="$1"
    initial_transient_paths_file="$2"
    current_paths_file="${initial_transient_paths_file}"
    attempt=0
    max_retries="${FSCK_TRANSIENT_MAX_RETRIES}"

    ensure_valid_fsck_retry_config
    if [ ! -s "${current_paths_file}" ]; then
        return 0
    fi

    case "${FSCK_RETRY_PATH_BATCH_SIZE}" in
        ''|*[!0-9]*)
            echo "WARNING: FSCK_RETRY_PATH_BATCH_SIZE must be a positive integer; using 1000." 1>&2
            FSCK_RETRY_PATH_BATCH_SIZE=1000
            ;;
        0)
            echo "WARNING: FSCK_RETRY_PATH_BATCH_SIZE must be greater than 0; using 1000." 1>&2
            FSCK_RETRY_PATH_BATCH_SIZE=1000
            ;;
    esac

    if [ "${max_retries}" -eq 0 ]; then
        echo "ERROR: fsck ${fsck_label} has transient-only failures, but FSCK_TRANSIENT_MAX_RETRIES=0." 1>&2
        rollback_local_merge_to_output_main
        return 1
    fi

    while [ "${attempt}" -lt "${max_retries}" ]; do
        retry_count=$(wc -l < "${current_paths_file}")
        attempt=$((attempt + 1))
        retry_label="${fsck_label}-transient-retry-${attempt}"
        echo "WARNING: retrying fsck ${fsck_label} for ${retry_count} transient path(s) (${attempt}/${max_retries})." 1>&2
        append_run_summary "fsck retry (${fsck_label}): retrying ${retry_count} transient path(s) (${attempt}/${max_retries})."

        fsck_status=0
        run_annex_fast_fsck_with_summary_for_paths "${retry_label}" "${current_paths_file}" || fsck_status=$?
        if [ "${fsck_status}" -eq 0 ]; then
            append_run_summary "fsck retry (${fsck_label}): transient path retries passed on attempt ${attempt}/${max_retries}."
            return 0
        fi

        handle_status=0
        handle_failed_fsck_for_resubmission "${retry_label}" || handle_status=$?
        if [ "${handle_status}" -eq 1 ]; then
            # Non-transient failures were handled (delete + rollback) already.
            return 1
        fi
        if [ "${handle_status}" -ne 2 ]; then
            echo "ERROR: fsck ${retry_label} failed, but failure records could not be classified safely." 1>&2
            rollback_local_merge_to_output_main
            return 1
        fi

        if [ "${attempt}" -ge "${max_retries}" ]; then
            break
        fi

        current_paths_file="../code/merge-fsck-repair/transient-fsck-${retry_label}.txt"
        echo "WARNING: transient fsck failures remain after attempt ${attempt}/${max_retries}; waiting ${FSCK_TRANSIENT_RETRY_SLEEP_SECONDS}s before retry." 1>&2
        append_run_summary "fsck retry (${fsck_label}): transient failures remain after attempt ${attempt}/${max_retries}; waiting ${FSCK_TRANSIENT_RETRY_SLEEP_SECONDS}s."
        sleep "${FSCK_TRANSIENT_RETRY_SLEEP_SECONDS}"
    done

    echo "ERROR: fsck ${fsck_label} transient retries exhausted after ${max_retries} attempt(s)." 1>&2
    echo "No result branches were deleted; rolling back local pseudo merge for a safe rerun." 1>&2
    append_run_summary "fsck retry (${fsck_label}): transient retries exhausted after ${max_retries} attempt(s); rollback applied."
    rollback_local_merge_to_output_main
    return 1
}

delete_merged_result_branches() {
    merged_branches_file="${TMPD}/merged-result-branches.txt"

    if [ -s "${TMPD}/has_results.txt" ]; then
        cp "${TMPD}/has_results.txt" "${merged_branches_file}"
    else
        git for-each-ref \
            --format='%(refname:short)' \
            --merged HEAD \
            'refs/remotes/origin/job-*' > "${merged_branches_file}"
    fi

    delete_remote_result_branches "${merged_branches_file}" "merged"
}

print_fcpindi_repair_summary() {
    repair_mode="$1"
    initial_missing_file="$2"
    repaired_batch_file="$3"
    truly_missing_file="$4"
    probe_errors_file="$5"
    still_missing_file="$6"
    resubmit_file="$7"

    append_run_summary "Annex reconciliation (${repair_mode}): initial missing records $(wc -l < "${initial_missing_file}"); records repaired $(wc -l < "${repaired_batch_file}"); content absent on S3 $(wc -l < "${truly_missing_file}"); probe errors $(wc -l < "${probe_errors_file}"); still missing $(wc -l < "${still_missing_file}"); affected subject-sessions $(wc -l < "${resubmit_file}")."
}

# Jobs push file content directly to S3, but their git-annex location logs do
# not necessarily arrive in this merged clone. The scalable repair pass trusts
# the success marker on each job branch: participant_job.sh verifies S3 before
# that branch can be pushed. If the final fsck still fails, the script probes
# only the missing records to decide which branches need replacement jobs.
repair_missing_fcpindi_records() {
    repair_mode="$1"
    report_dir="../code/merge-fsck-repair"
    missing_file="${report_dir}/missing-on-${annex_remote}.txt"
    missing_keys_file="${report_dir}/missing-keys-on-${annex_remote}.tsv"
    repaired_batch_file="${report_dir}/repaired-${annex_remote}.batch"
    truly_missing_file="${report_dir}/content-absent-on-${annex_remote}.txt"
    probe_errors_file="${report_dir}/probe-errors-${annex_remote}.txt"
    still_missing_file="${report_dir}/still-missing-on-${annex_remote}.txt"
    resubmit_file="${report_dir}/resubmit-subses.txt"
    remote_uuid=$(git config --get "remote.${annex_remote}.annex-uuid" || true)

    mkdir -p "${report_dir}"
    : > "${missing_file}"
    : > "${missing_keys_file}"
    : > "${repaired_batch_file}"
    : > "${truly_missing_file}"
    : > "${probe_errors_file}"
    : > "${still_missing_file}"
    : > "${resubmit_file}"

    # shellcheck disable=SC2016
    git annex find --not --in "${annex_remote}" --format='${key}\t${file}\n' > "${missing_keys_file}" || true
    cut -f2- "${missing_keys_file}" > "${missing_file}"
    if [ ! -s "${missing_file}" ]; then
        append_run_summary "Annex reconciliation (${repair_mode}): no missing ${annex_remote} presence records."
        return 0
    fi

    if [ -z "${remote_uuid}" ]; then
        echo "ERROR: could not find annex UUID for remote '${annex_remote}'." 1>&2
        echo "Run 'git annex enableremote ${annex_remote}' in merge_ds and re-run merge.sh." 1>&2
        return 1
    fi

    case "${repair_mode}" in
        trust)
            echo "WARNING: annex has missing ${annex_remote} presence records; recording them from successful job branches." 1>&2
            awk -F '\t' -v uuid="${remote_uuid}" '
                $1 != "" {
                    printf "%s %s 1\n", $1, uuid
                }
            ' "${missing_keys_file}" | LC_ALL=C sort -u > "${repaired_batch_file}"

            awk -F '\t' '$1 == "" { print }' "${missing_keys_file}" >> "${probe_errors_file}"
            ;;
        probe)
            echo "WARNING: annex has missing ${annex_remote} presence records; probing S3 before repairing." 1>&2
            while IFS=$'\t' read -r key path; do
                if [ -z "${path}" ]; then
                    continue
                fi
                if [ -z "${key}" ]; then
                    printf '%s\n' "${path}" >> "${probe_errors_file}"
                    continue
                fi

                if git annex checkpresentkey "${key}" "${annex_remote}" >/dev/null 2>>"${probe_errors_file}"; then
                    printf '%s %s 1\n' "${key}" "${remote_uuid}" >> "${repaired_batch_file}"
                else
                    status=$?
                    if [ "${status}" -eq 1 ]; then
                        printf '%s\n' "${path}" >> "${truly_missing_file}"
                    else
                        printf '%s\t%s\tcheckpresentkey exited %s\n' "${path}" "${key}" "${status}" >> "${probe_errors_file}"
                    fi
                fi
            done < "${missing_keys_file}"
            ;;
        *)
            echo "ERROR: unsupported repair mode '${repair_mode}'." 1>&2
            return 1
            ;;
    esac

    if [ -s "${repaired_batch_file}" ]; then
        git annex setpresentkey --batch < "${repaired_batch_file}"
        NEEDS_ANNEX_PUSH=1
    fi

    git annex find --not --in "${annex_remote}" > "${still_missing_file}" || true
    if [ -s "${truly_missing_file}" ]; then
        write_affected_subses "${truly_missing_file}" "${resubmit_file}"
    else
        write_affected_subses "${still_missing_file}" "${resubmit_file}"
    fi
    print_fcpindi_repair_summary \
        "${repair_mode}" \
        "${missing_file}" \
        "${repaired_batch_file}" \
        "${truly_missing_file}" \
        "${probe_errors_file}" \
        "${still_missing_file}" \
        "${resubmit_file}"

    if [ -s "${still_missing_file}" ]; then
        echo "ERROR: some annexed contents are still not available on ${annex_remote}." 1>&2
        echo "Reports are in ${report_dir}/" 1>&2
        if [ -s "${truly_missing_file}" ]; then
            handle_absent_content_for_resubmission "${truly_missing_file}" "${report_dir}"
        fi
        print_fcpindi_resubmit_guidance "${report_dir}"
        return 1
    fi

    return 0
}

reconcile_annex_remote() {
    if ! repair_missing_fcpindi_records trust; then
        exit 1
    fi
}

print_annex_fast_fsck_result() {
    fsck_label="$1"
    fsck_status="$2"
    fsck_log="$3"
    checked_count=$(grep -Ec '"command"[[:space:]]*:[[:space:]]*"fsck"' "${fsck_log}" || true)
    fail_count=$(grep -Ec '"success"[[:space:]]*:[[:space:]]*false' "${fsck_log}" || true)

    if [ "${fsck_status}" -eq 0 ]; then
        append_run_summary "fsck result (${fsck_label}): PASS; checked ${checked_count} file(s), ${fail_count} failed."
    else
        fsck_summary="fsck result (${fsck_label}): FAIL (exit ${fsck_status}); checked ${checked_count} file(s), ${fail_count} failed."
        report_dir="../code/merge-fsck-repair"
        mkdir -p "${report_dir}"
        fsck_report="${report_dir}/fsck-${fsck_label}.jsonl"
        cp "${fsck_log}" "${fsck_report}"
        fsck_summary="${fsck_summary} Full log: ${fsck_report}."
        if [ "${fail_count}" -gt 0 ]; then
            failures_file="${report_dir}/fsck-${fsck_label}-failures.jsonl"
            grep -E '"success"[[:space:]]*:[[:space:]]*false' "${fsck_log}" > "${failures_file}" || true
            fsck_summary="${fsck_summary} Failed records: ${failures_file}."
            echo "Failed fsck records: ${failures_file}" 1>&2
        fi
        append_run_summary "${fsck_summary}"
    fi
}

run_annex_fast_fsck_with_summary_for_paths() {
    fsck_label="$1"
    paths_file="${2:-}"
    fsck_status=0
    fsck_log="${TMPD}/fsck-${fsck_label}.jsonl"

    : > "${fsck_log}"
    if [ -n "${paths_file}" ] && [ -s "${paths_file}" ]; then
        chunk_prefix="${TMPD}/__fsck_${fsck_label}_"
        awk -v batch_size="${FSCK_RETRY_PATH_BATCH_SIZE}" -v prefix="${chunk_prefix}" '
            $0 != "" {
                chunk = int((NR - 1) / batch_size)
                print > sprintf("%s%06d", prefix, chunk)
            }
        ' "${paths_file}"

        fsck_batch_status=0
        for chunk in "${chunk_prefix}"*; do
            if [ ! -s "${chunk}" ]; then
                continue
            fi

            mapfile -t fsck_paths < "${chunk}"
            if [ "${#fsck_paths[@]}" -eq 0 ]; then
                continue
            fi

            fsck_batch_status=0
            git annex fsck --fast -J "${ANNEX_FSCK_JOBS}" -f "${annex_remote}" \
                --json --json-error-messages -- "${fsck_paths[@]}" >> "${fsck_log}" 2>&1 || fsck_batch_status=$?
            if [ "${fsck_batch_status}" -ne 0 ]; then
                fsck_status="${fsck_batch_status}"
            fi
        done
        rm -f "${chunk_prefix}"*
    else
        git annex fsck --fast -J "${ANNEX_FSCK_JOBS}" -f "${annex_remote}" \
            --json --json-error-messages > "${fsck_log}" 2>&1 || fsck_status=$?
    fi

    print_annex_fast_fsck_result "${fsck_label}" "${fsck_status}" "${fsck_log}"
    return "${fsck_status}"
}

run_annex_fast_fsck_with_summary() {
    fsck_label="$1"
    run_annex_fast_fsck_with_summary_for_paths "${fsck_label}" ""
}

run_final_fast_fsck() {
    report_dir="../code/merge-fsck-repair"
    transient_before_file="${report_dir}/transient-fsck-before-repair.txt"

    echo "Running final fast fsck against ${annex_remote} before pushing merged history."
    if run_annex_fast_fsck_with_summary "before-repair"; then
        return 0
    fi

    handle_status=0
    handle_failed_fsck_for_resubmission "before-repair" || handle_status=$?
    if [ "${handle_status}" -eq 1 ]; then
        # Non-transient failures were handled (delete + rollback).
        exit 1
    fi
    if [ "${handle_status}" -ne 2 ]; then
        echo "ERROR: final fsck failed, but failure records were not classified safely." 1>&2
        rollback_local_merge_to_output_main
        exit 1
    fi

    if ! retry_transient_fsck_paths "before-repair" "${transient_before_file}"; then
        exit 1
    fi
}

push_reconciled_history() {
    if [ "${NEEDS_ANNEX_PUSH}" = "1" ]; then
        push_origin_with_stale_ref_lock_retry git-annex
        append_run_summary "History push: git-annex pushed to origin."
    else
        append_run_summary "History push: git-annex push not needed."
    fi

    if [ "${NEEDS_PUSH}" = "1" ]; then
        output_branch=$(get_output_branch_name || true)
        if [ -z "${output_branch}" ]; then
            echo "ERROR: could not determine the output RIA branch to push." 1>&2
            echo "Check the origin remote and re-run merge.sh." 1>&2
            exit 1
        fi
        push_origin_with_stale_ref_lock_retry "HEAD:refs/heads/${output_branch}"
        append_run_summary "History push: HEAD pushed to origin/${output_branch}."
    else
        append_run_summary "History push: output branch push not needed."
    fi
}

HAD_MERGE_DS=0
if [ -d merge_ds/.git ]; then
    HAD_MERGE_DS=1
    prepare_existing_merge_clone
else
    prepare_full_merge_clone
fi

if [ "${MISSING_JOBS}" -gt 0 ]; then
    if [ "${HAS_NEW_BRANCHES}" = "1" ] || [ "${HAD_MERGE_DS}" != "1" ]; then
        append_run_summary "Missing jobs: ${MISSING_JOBS} job(s) still missing; resubmission required before merging."
        echo "ERROR: ${MISSING_JOBS} job(s) still missing. Resubmit before merging." 1>&2
        echo "Run:" 1>&2
        echo "  bash code/submit_jobs.sh code/missing.txt   # add --local [--n-cpus 8] to run without SLURM" 1>&2
        exit 1
    fi

    echo "WARNING: ${MISSING_JOBS} job branch(es) are absent, but merge_ds has no new result branches to merge." 1>&2
    echo "Continuing as a recovery rerun after merged result branches were already deleted." 1>&2
    append_run_summary "Missing jobs: ${MISSING_JOBS} job branch(es) absent; continuing recovery rerun because no new result branches remain."
fi

if [ "${HAS_NEW_BRANCHES}" = "1" ]; then
    find_auto_keep_entries "${TMPD}/auto_keep_entries.tsv"
    merge_all_by_index
    NEEDS_PUSH=1
    append_run_summary "Merge: synthesized $(wc -l < "${TMPD}/has_results.txt") result branch(es) into local merge history."
fi

# Reconcile S3 content availability. Each job pushed content directly to S3, so
# the merged clone may need to rebuild annex presence records before fsck can
# validate the remote cleanly.
reconcile_annex_remote

# This path avoids repeated full remote checks, but the final gate does one fast
# fsck before any merged history is pushed.
run_final_fast_fsck

# Publish the merged Git and git-annex histories back to the output RIA only
# after annex repair/fsck succeeds, then delete result branches that are now
# reachable from the verified merged history.
push_reconciled_history
delete_merged_result_branches

append_run_summary "Final status: SUCCESS; merged dataset $(pwd); next step code/publish.sh."
echo "SUCCESS: merge complete; all content recorded present on ${annex_remote}."
echo "Merged dataset: $(pwd)"
echo "Run code/publish.sh next."
