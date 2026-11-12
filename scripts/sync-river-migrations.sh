#!/usr/bin/env bash
# Copies River's migrations into the package, unmodified, from the Go module
# cache. Needs Go. Run from the repository root.
set -euo pipefail

VERSION="${1:-v0.51.2}"
MODULE="github.com/riverqueue/river/riverdriver/riverpgxv5"
DEST="src/eddy_pg/river_migrations"

dir="$(cd tests/support/gohelper && go mod download -json "${MODULE}@${VERSION}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Dir"])')"

rm -f "${DEST}"/*.up.sql
cp "${dir}"/migration/main/*.up.sql "${DEST}/"
cp "${dir}/LICENSE" "${DEST}/LICENSE"
chmod 644 "${DEST}"/*
echo "Copied River ${VERSION} migrations into ${DEST}."
echo "Now follow the rest of \"River version\" in README.md: the job SQL may need changes too,"
echo "and the Go eddy library must be on the same River version."
