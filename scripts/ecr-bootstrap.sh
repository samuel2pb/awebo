#!/usr/bin/env bash
# Create ECR repo (idempotent). Build/push happens in CI via
# .github/workflows/publish-ecr.yml — this script only ensures the repo
# exists so the first workflow run has somewhere to push to.
#
# Usage:
#   scripts/ecr-bootstrap.sh --repo awebo [--region us-east-1] [--profile dodpit]

set -euo pipefail

PROFILE="dodpit"
REGION="us-east-1"
REPO=""
SCAN_ON_PUSH="true"
IMMUTABLE="false"

usage() {
    cat <<USAGE >&2
usage: $(basename "$0") --repo NAME [--region REGION] [--profile PROFILE]

  --repo NAME        ECR repository name (required)
  --region REGION    AWS region (default: us-east-1)
  --profile PROFILE  AWS CLI profile (default: dodpit)
  --immutable        Create with IMMUTABLE tag policy (default MUTABLE)
  --no-scan          Disable scan-on-push (default enabled)
USAGE
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo) REPO="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --immutable) IMMUTABLE="true"; shift ;;
        --no-scan) SCAN_ON_PUSH="false"; shift ;;
        -h|--help) usage ;;
        *) echo "unknown arg: $1" >&2; usage ;;
    esac
done

[[ -z "$REPO" ]] && { echo "error: --repo is required" >&2; usage; }

aws_cli() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

ACCOUNT_ID="$(aws_cli sts get-caller-identity --query Account --output text)"
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
REPO_URI="${REGISTRY}/${REPO}"

echo ">> account=${ACCOUNT_ID} region=${REGION} repo=${REPO}"

if aws_cli ecr describe-repositories --repository-names "$REPO" >/dev/null 2>&1; then
    echo ">> ecr repo ${REPO} already exists"
else
    echo ">> creating ecr repo ${REPO}"
    MUTABILITY="MUTABLE"
    [[ "$IMMUTABLE" == "true" ]] && MUTABILITY="IMMUTABLE"
    # AES256 server-side encryption is the ECR default; flag omitted for
    # compatibility with older aws-cli v2 builds.
    aws_cli ecr create-repository \
        --repository-name "$REPO" \
        --image-tag-mutability "$MUTABILITY" \
        --image-scanning-configuration "scanOnPush=${SCAN_ON_PUSH}" \
        >/dev/null
    echo ">> created ${REPO_URI}"
fi

echo ">> AWS_ECR_REPO=${REPO_URI}"
