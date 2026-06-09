#!/bin/bash
# =============================================================================
# Merge all per-subject-session result branches and reconcile S3 availability.
#
# Run from the analysis dataset directory, AFTER find_missing.sh reports 0
# missing. Produces ./merge_ds with the merged history; run code/publish.sh next.
# =============================================================================
set -e -u -x
# shellcheck disable=SC1091
source code/job_config.sh

# Load fcp-indi credentials before the clone so the S3 remote auto-enables.
# shellcheck disable=SC1091
source code/load_credentials.sh || exit 1

# Refuse to merge while jobs are still missing.
bash code/find_missing.sh
if [ -s code/missing.txt ]; then
    echo "ERROR: $(wc -l < code/missing.txt) job(s) still missing. Resubmit before merging." 1>&2
    exit 1
fi

# Clone the output RIA -- this brings every job-* result branch.
rm -rf merge_ds
datalad clone "${output_clone_source}" merge_ds
cd merge_ds
git annex enableremote fcp-indi 2>/dev/null || true

base_ref=$(git rev-parse HEAD)

TMPD=$(mktemp -d)
trap 'rm -rf "${TMPD}"' EXIT

# Collect job branches that carry results (tip differs from the base commit).
git branch -r | tr -d ' ' | grep -E '^origin/job-' | while read -r b; do
    if [ "$(git rev-parse "${b}")" != "${base_ref}" ]; then
        echo "${b}"
    fi
done > "${TMPD}/has_results.txt"

n_results=$(wc -l < "${TMPD}/has_results.txt")
if [ "${n_results}" -eq 0 ]; then
    echo "ERROR: no result branches found in the output RIA." 1>&2
    exit 1
fi
echo "Merging ${n_results} result branches"

pipeline_label="${MERGE_PIPELINE_LABEL:-${PIPELINE:-${GET_FILES}}}"

# Print the subject-session token encoded in a result branch name.
# Falls back to a placeholder if a branch name does not follow the job pattern.
subses_from_branch() {
    branch="$1"
    subses=$(printf '%s\n' "${branch}" | grep -oE 'sub-[A-Za-z0-9]+_ses-[A-Za-z0-9]+' | tail -n 1 || true)
    if [ -z "${subses}" ]; then
        echo "unknown subject-session"
    else
        echo "${subses}"
    fi
}

# Build the subject-level extraction message used for single-branch merge
# commits and synthetic fast-path parent commits.
merge_commit_message() {
    branch="$1"
    echo "Merge ${pipeline_label} data for $(subses_from_branch "${branch}")"
}

# Create a synthetic commit with the provided tree and the result branch as its
# parent, preserving branch provenance while allowing fast batched merges.
make_extract_commit() {
    tree="$1"
    branch="$2"
    git commit-tree "${tree}" -p "${branch}" -m "$(merge_commit_message "${branch}")"
}

# Fast path: make synthetic local commits where known repeated metadata already
# matches the final "ours" copy, then octopus-merge those normalized commits in
# large batches. This avoids one Git merge per subject-session while preserving
# the original result branches as parents of the temporary commits. If a batch
# still has any other conflict, fall back to the slower per-branch merge below
# so the exact conflicting paths are reported.
MERGE_BATCH_SIZE="${MERGE_BATCH_SIZE:-2000}"

# Return success when a conflicted path is allowed to be resolved by keeping the
# already-merged copy.
is_auto_keep_path() {
    path="$1"
    case "${path}" in
        dataset_description.json|.bidsignore|desc-*_dseg.tsv|logs/CITATION.*|sourcedata/freesurfer/fsaverage/*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# List only known shared metadata/reference files for one tree. Keep this narrow:
# subject/session outputs must still conflict loudly if get_files.sh missed a
# needed rename.
list_auto_keep_paths_for_ref() {
    ref="$1"

    git ls-tree --name-only "${ref}" 2>/dev/null | while read -r path; do
        case "${path}" in
            dataset_description.json|.bidsignore|desc-*_dseg.tsv)
                printf '%s\n' "${path}"
                ;;
        esac
    done

    git ls-tree -r --name-only "${ref}" -- logs 2>/dev/null | while read -r path; do
        case "${path}" in
            logs/CITATION.*)
                printf '%s\n' "${path}"
                ;;
        esac
    done

    git ls-tree -r --name-only "${ref}" -- sourcedata/freesurfer/fsaverage 2>/dev/null || true
}

# Write canonical tree entries for auto-kept paths as TSV:
# path, mode, object type, object id. These entries are used to normalize every
# result branch before the fast octopus merge.
find_auto_keep_entries() {
    entries_file="$1"
    paths_file="${TMPD}/auto_keep_paths.txt"
    : > "${entries_file}"
    : > "${paths_file}"

    list_auto_keep_paths_for_ref HEAD >> "${paths_file}"
    while read -r branch; do
        list_auto_keep_paths_for_ref "${branch}" >> "${paths_file}"
    done < "${TMPD}/has_results.txt"
    sort -u "${paths_file}" -o "${paths_file}"

    while read -r path; do
        if [ -z "${path}" ]; then
            continue
        fi
        source_ref=""
        if git cat-file -e "HEAD:${path}" 2>/dev/null; then
            source_ref="HEAD"
        else
            while read -r branch; do
                if git cat-file -e "${branch}:${path}" 2>/dev/null; then
                    source_ref="${branch}"
                    break
                fi
            done < "${TMPD}/has_results.txt"
        fi

        if [ -n "${source_ref}" ]; then
            entry=$(git ls-tree "${source_ref}" -- "${path}")
            mode=$(printf '%s\n' "${entry}" | awk '{print $1}')
            type=$(printf '%s\n' "${entry}" | awk '{print $2}')
            oid=$(printf '%s\n' "${entry}" | awk '{print $3}')
            printf '%s\t%s\t%s\t%s\n' "${path}" "${mode}" "${type}" "${oid}" >> "${entries_file}"
        fi
    done < "${paths_file}"
}

# Create a synthetic per-branch extraction commit whose tree has auto-kept
# metadata normalized to the canonical entries. Uses a cheap root-tree rewrite
# when all auto-kept paths are at the repository root.
normalize_branch_for_octopus() {
    branch="$1"
    entries_file="$2"
    root_entries="${TMPD}/normalize.root"
    mktree_entries="${TMPD}/normalize.mktree"

    if [ ! -s "${entries_file}" ]; then
        make_extract_commit "$(git rev-parse "${branch}^{tree}")" "${branch}"
        return 0
    fi

    if cut -f1 "${entries_file}" | grep -q '/'; then
        normalize_branch_with_index "${branch}" "${entries_file}"
        return 0
    fi

    git ls-tree "${branch}^{tree}" > "${root_entries}"
    awk -F '\t' 'NR == FNR { replace[$1] = 1; next } !($2 in replace)' \
        "${entries_file}" "${root_entries}" > "${mktree_entries}"
    awk -F '\t' '{ printf "%s %s %s\t%s\n", $2, $3, $4, $1 }' \
        "${entries_file}" >> "${mktree_entries}"

    normalized_tree=$(git mktree < "${mktree_entries}")
    make_extract_commit "${normalized_tree}" "${branch}"
}

# Fallback normalizer for auto-kept paths below subdirectories. It uses a
# temporary index because rewriting nested trees by hand is more error-prone.
normalize_branch_with_index() {
    branch="$1"
    entries_file="$2"
    index_file="${TMPD}/normalize.index"

    rm -f "${index_file}"
    GIT_INDEX_FILE="${index_file}" git read-tree "${branch}"
    while IFS=$'\t' read -r path mode _type oid; do
        GIT_INDEX_FILE="${index_file}" git update-index --add --cacheinfo "${mode}" "${oid}" "${path}"
    done < "${entries_file}"

    normalized_tree=$(GIT_INDEX_FILE="${index_file}" git write-tree)
    make_extract_commit "${normalized_tree}" "${branch}"
}

# Merge one result branch directly, auto-resolving only allowlisted metadata
# conflicts and failing loudly for any other conflicted path.
merge_result_branch() {
    branch="$1"

    if git merge -m "$(merge_commit_message "${branch}")" "${branch}"; then
        return 0
    fi

    if ! git rev-parse -q --verify MERGE_HEAD >/dev/null; then
        echo "ERROR: merge failed before Git left a resolvable merge state for ${branch}." 1>&2
        exit 1
    fi

    conflicted=$(git diff --name-only --diff-filter=U)
    if [ -z "${conflicted}" ]; then
        echo "ERROR: merge failed without reporting conflicted paths for ${branch}." 1>&2
        exit 1
    fi

    unhandled=""
    while IFS= read -r path; do
        if ! is_auto_keep_path "${path}"; then
            unhandled="${unhandled}
${path}"
        fi
    done <<EOF
${conflicted}
EOF

    if [ -n "${unhandled}" ]; then
        echo "ERROR: merge conflict while merging ${branch}; not auto-resolving:" 1>&2
        printf '%s\n' "${unhandled}" | sed '/^$/d' 1>&2
        echo "Resolve this in merge_ds/ or fix code/get_files.sh, then rerun merge.sh." 1>&2
        exit 1
    fi

    while IFS= read -r path; do
        if [ -n "${path}" ]; then
            echo "Keeping existing ${path} while merging ${branch}"
            git checkout --ours -- "${path}"
            git add -- "${path}"
        fi
    done <<EOF
${conflicted}
EOF

    if [ -n "$(git diff --name-only --diff-filter=U)" ]; then
        echo "ERROR: unresolved conflicts remain after auto-resolution for ${branch}." 1>&2
        git diff --name-only --diff-filter=U 1>&2
        exit 1
    fi

    git commit --no-edit
}

# Normalize every branch in a chunk, octopus-merge the synthetic commits, and
# fall back to direct per-branch merges if the batch still has a real conflict.
merge_chunk_fast() {
    chunk="$1"
    chunk_label=$(basename "${chunk}")
    normalized_chunk="${TMPD}/normalized_${chunk_label}"

    : > "${normalized_chunk}"
    while read -r branch; do
        normalize_branch_for_octopus "${branch}" "${TMPD}/auto_keep_entries.tsv" >> "${normalized_chunk}"
    done < "${chunk}"

    # shellcheck disable=SC2046
    if git merge -m "Merge ${pipeline_label} data for $(wc -l < "${chunk}") subject-sessions: ${chunk_label}" $(cat "${normalized_chunk}"); then
        return 0
    fi

    echo "WARNING: fast batch merge failed for ${chunk_label}; retrying one branch at a time." 1>&2
    if git rev-parse -q --verify MERGE_HEAD >/dev/null; then
        git merge --abort || true
    fi

    while read -r branch; do
        merge_result_branch "${branch}"
    done < "${chunk}"
}

find_auto_keep_entries "${TMPD}/auto_keep_entries.tsv"

split -l "${MERGE_BATCH_SIZE}" --numeric-suffixes "${TMPD}/has_results.txt" "${TMPD}/__batch_"
for chunk in "${TMPD}"/__batch_*; do
    merge_chunk_fast "${chunk}"
done

# Publish the merged history back to the output RIA.
git push origin HEAD

# Reconcile fcp-indi content availability. Each job pushed content directly to
# S3, so the location log must be rebuilt here.
# NOTE: `git annex fsck -f` issues one S3 request per key and does NOT scale to
# the large datasets. Time it on the CCNP pilot; for production, replace with a
# single `aws s3 ls --recursive` of the prefix + `git annex setpresentkey`.
fsck_status=0
git annex fsck --fast -f fcp-indi || fsck_status=$?

missing=$(git annex find --not --in fcp-indi || true)
if [ -n "${missing}" ]; then
    echo "ERROR: the following file contents are not on fcp-indi:" 1>&2
    echo "${missing}" 1>&2
    echo "Re-run the relevant jobs or copy the content, then re-run merge.sh." 1>&2
    exit 1
fi

if [ "${fsck_status}" -ne 0 ]; then
    echo "ERROR: git annex fsck --fast -f fcp-indi failed, but no missing content was reported by git annex find." 1>&2
    echo "Inspect the fsck output above, fix the underlying annex/S3 error, then re-run merge.sh." 1>&2
    exit "${fsck_status}"
fi

echo "SUCCESS: merge complete; all content present on fcp-indi."
echo "Merged dataset: $(pwd)"
echo "Run code/publish.sh next."
