#!/usr/bin/env bash
# Download latest Apache Arrow C++ + Flight prebuilt (headers + libs) into
# cpp/package/arrow/{include,lib}.
#
# Source: pyarrow's manylinux wheel from PyPI — bundles upstream Arrow C++
# shared libs (gRPC/protobuf statically linked) plus full public C++ headers.
#
# If PyPI download is slow/blocked, the script prints the wheel URL and the
# local path it expects; download manually (e.g. via browser/proxy/mirror),
# drop the file at that path, and re-run — the script will detect & reuse it.
#
# Requires: curl, python3, unzip.

set -euo pipefail
cd "$(dirname "$0")"

ARCH="$(uname -m)"
META_FILE="$(mktemp)"
WORK="$(mktemp -d)"
trap 'rm -rf "$META_FILE" "$WORK"' EXIT

echo "[vendor] querying PyPI for latest pyarrow manylinux $ARCH wheel ..."
curl -fsSL --connect-timeout 10 --max-time 30 https://pypi.org/pypi/pyarrow/json -o "$META_FILE"

read -r VER WHEEL_URL WHEEL_NAME < <(python3 - "$META_FILE" "$ARCH" <<'PY'
import json, re, sys
meta, arch = sys.argv[1], sys.argv[2]
with open(meta) as f: d = json.load(f)
ver = d["info"]["version"]
pat = re.compile(rf"manylinux.*_{re.escape(arch)}\.whl$")
cands = [x for x in d["releases"][ver] if pat.search(x["filename"])]
assert cands, f"No manylinux {arch} wheel for pyarrow {ver}"
w = cands[0]
print(ver, w["url"], w["filename"])
PY
)

WHEEL_PATH="./$WHEEL_NAME"

if [[ -f "$WHEEL_PATH" ]]; then
    echo "[vendor] reusing cached $WHEEL_PATH"
else
    cat <<EOF
[vendor] downloading pyarrow $VER
    url:  $WHEEL_URL
    dest: $(pwd)/$WHEEL_NAME

If PyPI is unreachable, download the URL above manually (browser / proxy /
mirror), save it to the dest path, and re-run this script.

EOF
    curl -fL --progress-bar --connect-timeout 10 -o "$WHEEL_PATH" "$WHEEL_URL"
fi

unzip -q "$WHEEL_PATH" -d "$WORK"

PYA="$WORK/pyarrow"
shopt -s nullglob
ARROW_SOS=(  "$PYA"/libarrow.so.* )
FLIGHT_SOS=( "$PYA"/libarrow_flight.so.* )
PARQUET_SOS=("$PYA"/libparquet.so.* )
shopt -u nullglob
(( ${#ARROW_SOS[@]}   == 1 )) || { echo "ERROR: libarrow.so.* count = ${#ARROW_SOS[@]}";    exit 1; }
(( ${#FLIGHT_SOS[@]}  == 1 )) || { echo "ERROR: libarrow_flight.so.* count = ${#FLIGHT_SOS[@]}";  exit 1; }
(( ${#PARQUET_SOS[@]} == 1 )) || { echo "ERROR: libparquet.so.* count = ${#PARQUET_SOS[@]}"; exit 1; }

ARROW_SONAME="$(basename   "${ARROW_SOS[0]}")"
FLIGHT_SONAME="$(basename  "${FLIGHT_SOS[0]}")"
PARQUET_SONAME="$(basename "${PARQUET_SOS[0]}")"

rm -rf include lib
mkdir -p include lib
cp -r "$PYA/include/arrow"   include/
cp -r "$PYA/include/parquet" include/
cp "${ARROW_SOS[0]}"   lib/
cp "${FLIGHT_SOS[0]}"  lib/
cp "${PARQUET_SOS[0]}" lib/
( cd lib && ln -sf "$ARROW_SONAME"   libarrow.so )
( cd lib && ln -sf "$FLIGHT_SONAME"  libarrow_flight.so )
( cd lib && ln -sf "$PARQUET_SONAME" libparquet.so )

echo "[vendor] headers: arrow=$(du -sh include/arrow | cut -f1)  parquet=$(du -sh include/parquet | cut -f1)"
echo "[vendor] $ARROW_SONAME   ($(du -h lib/$ARROW_SONAME   | cut -f1))"
echo "[vendor] $FLIGHT_SONAME  ($(du -h lib/$FLIGHT_SONAME  | cut -f1))"
echo "[vendor] $PARQUET_SONAME ($(du -h lib/$PARQUET_SONAME | cut -f1))"
echo "[vendor] done — Apache Arrow $VER ready"
