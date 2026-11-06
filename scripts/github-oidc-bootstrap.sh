#!/usr/bin/env bash
# Set up GitHub Actions OIDC -> AWS for the ECR publish workflow (idempotent).
# Creates the IAM OIDC provider for token.actions.githubusercontent.com (if
# missing), a role assumable only from the given repo's main branch, and an
# inline policy allowing image push to one ECR repository. After running,
# set vars.AWS_OIDC_ROLE_ARN and delete the static AWS key secrets (the
# exact commands are printed at the end).
#
# Usage:
#   scripts/github-oidc-bootstrap.sh --repo-name awebo --github-repo samuel2pb/awebo \
#       [--role-name github-awebo-ecr-push] [--region us-east-1] [--profile NAME]
#
# Profile resolution: --profile flag, then $AWS_PROFILE env var, then the
# AWS CLI default profile (no --profile passed to aws).

set -euo pipefail

PROFILE="${AWS_PROFILE:-}"
REGION="${AWS_REGION:-us-east-1}"
REPO_NAME=""
GITHUB_REPO=""
ROLE_NAME="github-awebo-ecr-push"
POLICY_NAME="ecr-push"
OIDC_HOST="token.actions.githubusercontent.com"
OIDC_URL="https://${OIDC_HOST}"
AUDIENCE="sts.amazonaws.com"
# AWS validates GitHub's certificate chain itself; the API still requires a
# thumbprint, so a placeholder is passed.
THUMBPRINT="ffffffffffffffffffffffffffffffffffffffff"

usage() {
    cat <<USAGE >&2
usage: $(basename "$0") --repo-name NAME --github-repo OWNER/REPO [options]

  --repo-name NAME       ECR repository name (required)
  --github-repo O/R      GitHub repository allowed to assume the role (required)
  --role-name NAME       IAM role name (default: github-awebo-ecr-push)
  --region REGION        AWS region (default: \$AWS_REGION or us-east-1)
  --profile PROFILE      AWS CLI profile (default: \$AWS_PROFILE, else CLI default)
USAGE
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-name) REPO_NAME="$2"; shift 2 ;;
        --github-repo) GITHUB_REPO="$2"; shift 2 ;;
        --role-name) ROLE_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --profile) PROFILE="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "unknown arg: $1" >&2; usage ;;
    esac
done

[[ -z "$REPO_NAME" ]] && { echo "error: --repo-name is required" >&2; usage; }
[[ -z "$GITHUB_REPO" ]] && { echo "error: --github-repo is required" >&2; usage; }

aws_cli() {
    if [[ -n "$PROFILE" ]]; then
        aws --profile "$PROFILE" --region "$REGION" "$@"
    else
        aws --region "$REGION" "$@"
    fi
}

ACCOUNT_ID="$(aws_cli sts get-caller-identity --query Account --output text)"
PROVIDER_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/${OIDC_HOST}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
REPO_ARN="arn:aws:ecr:${REGION}:${ACCOUNT_ID}:repository/${REPO_NAME}"

echo ">> account=${ACCOUNT_ID} region=${REGION} ecr_repo=${REPO_NAME} github_repo=${GITHUB_REPO} role=${ROLE_NAME}"

# 1. OIDC provider
if aws_cli iam list-open-id-connect-providers \
        --query 'OpenIDConnectProviderList[].Arn' --output text \
        | tr '\t' '\n' | grep -qx "$PROVIDER_ARN"; then
    echo ">> oidc provider ${OIDC_HOST} already exists"
else
    echo ">> creating oidc provider ${OIDC_HOST}"
    aws_cli iam create-open-id-connect-provider \
        --url "$OIDC_URL" \
        --client-id-list "$AUDIENCE" \
        --thumbprint-list "$THUMBPRINT" \
        >/dev/null
fi

# 2. Role + trust policy (main branch of GITHUB_REPO only)
TRUST_POLICY="$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Federated": "${PROVIDER_ARN}" },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": { "${OIDC_HOST}:aud": "${AUDIENCE}" },
        "StringLike": { "${OIDC_HOST}:sub": "repo:${GITHUB_REPO}:ref:refs/heads/main" }
      }
    }
  ]
}
JSON
)"

if aws_cli iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    echo ">> role ${ROLE_NAME} exists; updating trust policy"
    aws_cli iam update-assume-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-document "$TRUST_POLICY"
else
    echo ">> creating role ${ROLE_NAME}"
    aws_cli iam create-role \
        --role-name "$ROLE_NAME" \
        --description "GitHub Actions (${GITHUB_REPO} main) push to ECR ${REPO_NAME}" \
        --assume-role-policy-document "$TRUST_POLICY" \
        >/dev/null
fi

# 3. Inline ECR push policy (put-role-policy overwrites, so re-runs converge)
PUSH_POLICY="$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EcrAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "EcrPush",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:CompleteLayerUpload",
        "ecr:InitiateLayerUpload",
        "ecr:PutImage",
        "ecr:UploadLayerPart",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:DescribeRepositories"
      ],
      "Resource": "${REPO_ARN}"
    }
  ]
}
JSON
)"

echo ">> putting inline policy ${POLICY_NAME} on ${ROLE_NAME}"
aws_cli iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "$POLICY_NAME" \
    --policy-document "$PUSH_POLICY"

echo ">> done. role arn: ${ROLE_ARN}"
echo
echo "Next steps:"
echo "  gh variable set AWS_OIDC_ROLE_ARN --body \"${ROLE_ARN}\" -R ${GITHUB_REPO}"
echo "  # after a green publish-ecr run via OIDC:"
echo "  gh secret delete AWS_ACCESS_KEY -R ${GITHUB_REPO}"
echo "  gh secret delete AWS_SECRET_ACCESS_KEY -R ${GITHUB_REPO}"
