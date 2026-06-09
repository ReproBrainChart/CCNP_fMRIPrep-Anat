#!/bin/bash
# =============================================================================
# get_files variant: fmriprep_anat
#
# Unzip ONE BABS-produced fMRIPrep anat-only archive into the dataset root,
# renaming outputs that are subject-level in fMRIPrep but session-specific in
# this release.
#
# Invoked by participant_job.sh inside `datalad run`. CWD is the dataset root.
#   Argument $1: path to the .zip file (under inputs/data/).
# =============================================================================
set -e -u -x

ZIP_FILE="$1"
zipbase=$(basename "${ZIP_FILE}")
subid=$(echo "${zipbase}" | grep -oE 'sub-[A-Za-z0-9]+' | head -n 1)
sesid=$(echo "${zipbase}" | grep -oE 'ses-[A-Za-z0-9]+' | head -n 1)

if [ -z "${subid}" ] || [ -z "${sesid}" ]; then
    echo "ERROR: expected both sub-* and ses-* in fmriprep_anat zip name: ${zipbase}" 1>&2
    exit 1
fi

LEADING_DIR="fmriprep_anat"

# --- extract -----------------------------------------------------------------
move_tree_no_overwrite() {
    local src_dir="$1"
    local dst_dir="$2"
    local src name dst

    shopt -s nullglob dotglob
    mkdir -p "${dst_dir}"
    for src in "${src_dir}"/*; do
        name=$(basename "${src}")
        dst="${dst_dir}/${name}"

        if [ -d "${src}" ] && [ ! -L "${src}" ] && [ -d "${dst}" ] && [ ! -L "${dst}" ]; then
            move_tree_no_overwrite "${src}" "${dst}"
            rmdir "${src}"
        elif [ -e "${dst}" ]; then
            echo "ERROR: refusing to overwrite existing path: ${dst}" 1>&2
            return 1
        else
            mv "${src}" "${dst}"
        fi
    done
}

extract_dir=$(mktemp -d "${PWD}/.get_files_extract.XXXXXX")
cleanup_extract() {
    rm -rf "${extract_dir}"
}
trap cleanup_extract EXIT

unzip -q "${ZIP_FILE}" -d "${extract_dir}"
if [ ! -d "${extract_dir}/${LEADING_DIR}" ]; then
    echo "ERROR: leading dir '${LEADING_DIR}/' not found in ${zipbase}." 1>&2
    echo "       Run '7z l ${ZIP_FILE}' and fix LEADING_DIR in code/get_files.sh." 1>&2
    exit 1
fi

# Lift the contents up to the dataset root without replacing any existing file.
move_tree_no_overwrite "${extract_dir}/${LEADING_DIR}" "."

# --- rename files that lack session info -------------------------------------
# fMRIPrep writes the subject report as <sub>.html at the dataset root. Inject
# the session so multi-session subjects do not collide at merge.
if [ -f "${subid}.html" ]; then
    mv "${subid}.html" "${subid}_${sesid}.html"
fi

# FreeSurfer anat-only outputs are stored under sourcedata/freesurfer/<sub>,
# even though this pipeline runs per subject-session. Inject the session entity
# into that directory name so multiple sessions for one subject can coexist.
fs_subject_dir="sourcedata/freesurfer/${subid}"
fs_session_dir="sourcedata/freesurfer/${subid}_${sesid}"
if [ -d "${fs_subject_dir}" ]; then
    if [ -e "${fs_session_dir}" ]; then
        echo "ERROR: refusing to overwrite existing FreeSurfer session dir: ${fs_session_dir}" 1>&2
        exit 1
    fi
    mv "${fs_subject_dir}" "${fs_session_dir}"
fi

echo "get_files.fmriprep_anat: unpacked ${zipbase}"
