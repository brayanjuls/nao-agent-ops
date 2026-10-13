#!/usr/bin/env bash
# Downloads a few small sample Hudi tables from the hudi-rs repo (Apache-2.0) for local testing.
set -euo pipefail
DEST="${1:-./sample_tables}"
BASE="https://raw.githubusercontent.com/apache/hudi-rs/main/crates/test/data"
mkdir -p "$DEST"
for f in sample_table/cow/v9_txns_simple_meta sample_table/cow/v8_simplekeygen_nonhivestyle quickstart_trips_table/mor/avro/v6_trips_8i3d; do
  name=$(basename "$f")
  curl -fsSL "$BASE/$f.zip" -o "/tmp/$name.zip"
  unzip -qo "/tmp/$name.zip" -d "$DEST/$name.tmp"
  # each zip holds <name>/ with the table; keep only that folder
  rm -rf "$DEST/$name"; mv "$DEST/$name.tmp/$name" "$DEST/$name"; rm -rf "$DEST/$name.tmp" "/tmp/$name.zip"
done
find "$DEST" -name __MACOSX -prune -exec rm -rf {} +
echo "Sample tables in $DEST:"; ls "$DEST"
