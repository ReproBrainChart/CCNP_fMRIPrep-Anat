#!/bin/bash
# =============================================================================
# Decode the obfuscated fcp-indi (AWS) + GitHub credentials into environment
# variables. SOURCE this file -- do not execute it:
#       source code/load_credentials.sh || exit 1
#
# Credentials live base64-encoded in ~/.cache/.p1 / .p2 / .p3, readable only by
# the owner. base64 is OBFUSCATION, not encryption -- the real protection is the
# file permissions (chmod 600). Create the files once with setup_credentials.sh.
# =============================================================================

# Disable xtrace while touching secrets so they never reach SLURM log files;
# remember whether it was on so it can be restored afterwards.
__lc_xtrace=0
case "$-" in *x*) __lc_xtrace=1 ;; esac
set +x

__lc_dir="${HOME}/.cache"

if [ ! -r "${__lc_dir}/.p1" ] || [ ! -r "${__lc_dir}/.p2" ]; then
    echo "ERROR: fcp-indi credentials not found (${__lc_dir}/.p1, .p2)." 1>&2
    echo "       Run setup_credentials.sh once to create them." 1>&2
    if [ "${__lc_xtrace}" = 1 ]; then set -x; fi
    unset __lc_xtrace __lc_dir
    return 1 2>/dev/null || exit 1
fi

export AWS_ACCESS_KEY_ID="$(base64 -d "${__lc_dir}/.p1")"
export AWS_SECRET_ACCESS_KEY="$(base64 -d "${__lc_dir}/.p2")"

# GitHub token is optional -- only publish.sh needs it.
if [ -r "${__lc_dir}/.p3" ]; then
    export DATALAD_CREDENTIAL_GH_TOKEN="$(base64 -d "${__lc_dir}/.p3")"
fi

if [ "${__lc_xtrace}" = 1 ]; then set -x; fi
unset __lc_xtrace __lc_dir
