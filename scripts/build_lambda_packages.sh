#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/dist/lambda"
API_DIR="${BUILD_DIR}/api"
WORKER_DIR="${BUILD_DIR}/worker"
EXPORT_DIR="${BUILD_DIR}/export"

rm -rf "${BUILD_DIR}"
mkdir -p "${API_DIR}" "${WORKER_DIR}" "${EXPORT_DIR}" "${ROOT_DIR}/dist"

python3 -m pip install --upgrade pip >/dev/null
python3 -m pip install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  -r "${ROOT_DIR}/backend/requirements.txt" \
  -t "${API_DIR}" >/dev/null
python3 -m pip install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  -r "${ROOT_DIR}/backend/requirements.txt" \
  -t "${WORKER_DIR}" >/dev/null
python3 -m pip install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  -r "${ROOT_DIR}/backend/requirements.txt" \
  -t "${EXPORT_DIR}" >/dev/null

cp "${ROOT_DIR}/backend"/*.py "${API_DIR}/"
cp "${ROOT_DIR}/backend"/*.py "${WORKER_DIR}/"
cp "${ROOT_DIR}/backend"/*.py "${EXPORT_DIR}/"

(
  cd "${API_DIR}"
  zip -qr "${ROOT_DIR}/dist/lambda_api.zip" .
)

(
  cd "${WORKER_DIR}"
  zip -qr "${ROOT_DIR}/dist/lambda_worker.zip" .
)

(
  cd "${EXPORT_DIR}"
  zip -qr "${ROOT_DIR}/dist/lambda_export.zip" .
)

echo "Built:"
echo "  ${ROOT_DIR}/dist/lambda_api.zip"
echo "  ${ROOT_DIR}/dist/lambda_worker.zip"
echo "  ${ROOT_DIR}/dist/lambda_export.zip"
