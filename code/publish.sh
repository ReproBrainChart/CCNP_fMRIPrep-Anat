#!/bin/bash
# =============================================================================
# Publish the merged dataset to GitHub.
#
# Run from the analysis dataset directory, AFTER code/merge.sh has produced
# ./merge_ds. Creates the GitHub sibling and pushes git history + the git-annex
# branch (which carries the fcp-indi content-location records).
# =============================================================================
set -e -u -x
# shellcheck disable=SC1091
source code/job_config.sh

if [ ! -d merge_ds ]; then
    echo "ERROR: merge_ds/ not found. Run code/merge.sh first." 1>&2
    exit 1
fi

# Credentials: AWS for fcp-indi, GitHub token for create-sibling-github and
# direct git pushes. Disable xtrace while handling the token so it never lands
# in logs.
__pub_xtrace=0
case "$-" in
    *x*)
        __pub_xtrace=1
        set +x
        ;;
esac
# shellcheck disable=SC1091
source code/load_credentials.sh || exit 1
if [ -z "${DATALAD_CREDENTIAL_GH_TOKEN:-}" ]; then
    echo "ERROR: GitHub token not loaded. Re-run setup_credentials.sh and provide it." 1>&2
    exit 1
fi
export DATALAD_CREDENTIAL_GH_TOKEN
if [ "${__pub_xtrace}" -eq 1 ]; then
    set -x
fi
unset __pub_xtrace

cd merge_ds

# GitHub displays the default branch; make sure it is `main`.
current=$(git rev-parse --abbrev-ref HEAD)
if [ "${current}" != "main" ]; then
    git branch -m "${current}" main
fi

# Create the GitHub repo + sibling. --publish-depends fcp-indi ensures content
# is on S3 before any future datalad push.
datalad create-sibling-github "${GITHUB_ORG_REPO}" \
    -s github --private --credential GH \
    --existing reconfigure \
    --publish-depends fcp-indi \
    --description "RBC ${PIPELINE} derivatives (AI2D release)"

# datalad push is unreliable on very large repos -- push the git refs directly.
TMPD=$(mktemp -d)
trap 'rm -rf "${TMPD}"' EXIT
GIT_ASKPASS_HELPER="${TMPD}/git-askpass"
cat > "${GIT_ASKPASS_HELPER}" <<'EOF'
#!/bin/sh
case "$1" in
    *Username*) printf '%s\n' "x-access-token" ;;
    *Password*) printf '%s\n' "${DATALAD_CREDENTIAL_GH_TOKEN}" ;;
    *) printf '\n' ;;
esac
EOF
chmod 700 "${GIT_ASKPASS_HELPER}"

GIT_ASKPASS="${GIT_ASKPASS_HELPER}" GIT_TERMINAL_PROMPT=0 git push github main
GIT_ASKPASS="${GIT_ASKPASS_HELPER}" GIT_TERMINAL_PROMPT=0 git push github git-annex

echo "SUCCESS: published to https://github.com/${GITHUB_ORG_REPO}"
echo ""
echo "Remaining manual steps:"
echo "  * verify a fresh clone: datalad clone https://github.com/${GITHUB_ORG_REPO} test && \\"
echo "      cd test && datalad get <a-file>"
echo "  * add a deprecation notice + disclosure pointer to the old C-PAC repo"
echo "  * update AI2D/docs/get_data.md to the unzipped-file access pattern"
