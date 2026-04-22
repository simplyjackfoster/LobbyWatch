#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <s3-bucket-name> <cloudfront-distribution-id>"
  exit 1
fi

BUCKET="$1"
DISTRIBUTION_ID="$2"
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "${ROOT_DIR}/frontend"
npm ci
npm run build

aws s3 sync dist/ "s3://${BUCKET}" --delete
aws cloudfront create-invalidation --distribution-id "${DISTRIBUTION_ID}" --paths "/*" >/dev/null

echo "Frontend deployed to s3://${BUCKET} and CloudFront invalidation requested."
