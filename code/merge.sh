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
TMPD=""
HAS_NEW_BRANCHES=0
NEEDS_PUSH=0
NEEDS_ANNEX_PUSH=0

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
trap cleanup_tmpd EXIT

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

    git for-each-ref --format='%(refname:short)' 'refs/remotes/origin/job-*' | while read -r branch; do
        if [ -z "${branch}" ]; then
            continue
        fi

        if git merge-base --is-ancestor "${branch}" HEAD; then
            continue
        fi

        echo "${branch}"
    done > "${TMPD}/has_results.txt"

    n_results=$(wc -l < "${TMPD}/has_results.txt")
    if [ "${n_results}" -gt 0 ]; then
        HAS_NEW_BRANCHES=1
        echo "Merging ${n_results} new result branches"
    else
        HAS_NEW_BRANCHES=0
        echo "No new result branches to merge; rechecking annex availability in existing history."
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
    TMPD=$(mktemp -d)
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

    grep -oE 'sub-[A-Za-z0-9]+_ses-[A-Za-z0-9]+' "${input_file}" | sort -u > "${output_file}" || true
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

delete_remote_result_branches() {
    branches_file="$1"
    reason="$2"
    refspecs_file="${TMPD}/delete-refspecs.txt"
    batch=()

    if [ ! -s "${branches_file}" ]; then
        return 0
    fi

    : > "${refspecs_file}"
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
        return 0
    fi

    echo "Deleting $(wc -l < "${refspecs_file}") ${reason} result branch(es) from origin."
    while read -r refspec; do
        batch+=("${refspec}")
        if [ "${#batch[@]}" -ge "${BRANCH_DELETE_BATCH_SIZE}" ]; then
            git push origin "${batch[@]}"
            batch=()
        fi
    done < "${refspecs_file}"
    if [ "${#batch[@]}" -gt 0 ]; then
        git push origin "${batch[@]}"
    fi

    while read -r branch; do
        if [ -n "${branch}" ]; then
            git update-ref -d "refs/remotes/${branch}" 2>/dev/null || true
        fi
    done < "${branches_file}"
}

rollback_local_merge_to_output_main() {
    output_main_ref=$(get_output_main_ref || true)
    if [ -z "${output_main_ref}" ]; then
        echo "WARNING: could not determine output main ref; leaving local pseudo merge in place." 1>&2
        return 0
    fi

    echo "Rolling local pseudo merge back to ${output_main_ref}; it was not pushed."
    git reset --hard "${output_main_ref}"
    NEEDS_PUSH=0
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

delete_merged_result_branches() {
    merged_branches_file="${TMPD}/merged-result-branches.txt"
    candidate_branches_file="${TMPD}/cleanup-candidate-result-branches.txt"

    if [ -s "${TMPD}/has_results.txt" ]; then
        cp "${TMPD}/has_results.txt" "${candidate_branches_file}"
    else
        git for-each-ref --format='%(refname:short)' 'refs/remotes/origin/job-*' > "${candidate_branches_file}"
    fi

    : > "${merged_branches_file}"
    while read -r branch; do
        if [ -n "${branch}" ] && git merge-base --is-ancestor "${branch}" HEAD; then
            echo "${branch}" >> "${merged_branches_file}"
        fi
    done < "${candidate_branches_file}"

    delete_remote_result_branches "${merged_branches_file}" "merged"
}

print_fcpindi_repair_summary() {
    initial_missing_file="$1"
    repaired_batch_file="$2"
    truly_missing_file="$3"
    probe_errors_file="$4"
    still_missing_file="$5"
    resubmit_file="$6"

    echo "fcp-indi reconciliation report:"
    echo "  initial missing records: $(wc -l < "${initial_missing_file}")"
    echo "  records repaired:         $(wc -l < "${repaired_batch_file}")"
    echo "  content absent on S3:     $(wc -l < "${truly_missing_file}")"
    echo "  probe errors:             $(wc -l < "${probe_errors_file}")"
    echo "  still missing:            $(wc -l < "${still_missing_file}")"
    echo "  affected subject-sessions: $(wc -l < "${resubmit_file}")"
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
        if [ -s "${resubmit_file}" ]; then
            echo "Re-run affected jobs with:" 1>&2
            echo "  sbatch --array=1-\$(wc -l < code/merge-fsck-repair/resubmit-subses.txt)%${THROTTLE} code/submit_array.sh code/merge-fsck-repair/resubmit-subses.txt" 1>&2
        else
            echo "No subject-session IDs could be extracted for automatic resubmission." 1>&2
            echo "Inspect ${still_missing_file} and repair or resubmit the affected data manually." 1>&2
        fi
        echo "Then re-run merge.sh; it will reuse merge_ds and merge only new result branches." 1>&2
        return 1
    fi

    return 0
}

reconcile_annex_remote() {
    if ! repair_missing_fcpindi_records trust; then
        exit 1
    fi
}

run_final_fast_fsck() {
    echo "Running final fast fsck against ${annex_remote} before pushing merged history."
    fsck_status=0
    git annex fsck --fast -f "${annex_remote}" || fsck_status=$?
    if [ "${fsck_status}" -eq 0 ]; then
        return 0
    fi

    echo "WARNING: final fast fsck failed; probing missing records once before deciding whether jobs must be rerun." 1>&2
    if ! repair_missing_fcpindi_records probe; then
        exit 1
    fi

    fsck_status=0
    git annex fsck --fast -f "${annex_remote}" || fsck_status=$?
    if [ "${fsck_status}" -ne 0 ]; then
        echo "ERROR: final git annex fsck --fast -f ${annex_remote} failed after repair." 1>&2
        echo "No merged Git or git-annex history was pushed. Inspect the fsck output above and reports under code/merge-fsck-repair/." 1>&2
        exit "${fsck_status}"
    fi

}

push_reconciled_history() {
    if [ "${NEEDS_ANNEX_PUSH}" = "1" ]; then
        git push origin git-annex
    fi

    if [ "${NEEDS_PUSH}" = "1" ]; then
        output_branch=$(get_output_branch_name || true)
        if [ -z "${output_branch}" ]; then
            echo "ERROR: could not determine the output RIA branch to push." 1>&2
            echo "Check the origin remote and re-run merge.sh." 1>&2
            exit 1
        fi
        git push origin "HEAD:refs/heads/${output_branch}"
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
        echo "ERROR: ${MISSING_JOBS} job(s) still missing. Resubmit before merging." 1>&2
        echo "Run:" 1>&2
        echo "  sbatch --array=1-${MISSING_JOBS}%${THROTTLE} code/submit_array.sh code/missing.txt" 1>&2
        exit 1
    fi

    echo "WARNING: ${MISSING_JOBS} job branch(es) are absent, but merge_ds has no new result branches to merge." 1>&2
    echo "Continuing as a recovery rerun after merged result branches were already deleted." 1>&2
fi

if [ "${HAS_NEW_BRANCHES}" = "1" ]; then
    find_auto_keep_entries "${TMPD}/auto_keep_entries.tsv"
    merge_all_by_index
    NEEDS_PUSH=1
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

echo "SUCCESS: merge complete; all content recorded present on ${annex_remote}."
echo "Merged dataset: $(pwd)"
echo "Run code/publish.sh next."

