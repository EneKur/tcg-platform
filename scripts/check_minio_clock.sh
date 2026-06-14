#!/usr/bin/env bash
# MinIO clock-skew check.
#
# Hits MinIO's health endpoint, reads the `Date` response header, and
# compares it to the local UTC clock. Exits non-zero only on hard FAIL
# (skew >= 5 min). WARN (60s..5min) is visible but not a hard fail;
# SKIP (MinIO unreachable) is non-fatal so CI without MinIO still passes.
#
# Override endpoint: MINIO_ENDPOINT=http://host:port  (default localhost:9000)

set -euo pipefail

ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
URL="${ENDPOINT%/}/minio/health/live"

# 1. Hit the health endpoint. If unreachable, SKIP (exit 0).
HEADERS="$(curl -sS --max-time 5 -D - -o /dev/null "$URL" 2>/dev/null || true)"
if [ -z "$HEADERS" ]; then
    echo "SKIP  minio_unreachable endpoint=$URL"
    exit 0
fi

# 2. Pull the Date header. Strip weekday prefix with sed for cross-platform parsing.
SERVER_DATE_RAW="$(printf '%s' "$HEADERS" | awk -F': ' 'tolower($1)=="date" {sub(/\r$/,"",$2); print $2; exit}')"
if [ -z "$SERVER_DATE_RAW" ]; then
    echo "SKIP  minio_unreachable endpoint=$URL"
    exit 0
fi

# 3. Convert both to epoch seconds (UTC). Use Python for portable RFC-2822 parsing.
SKEW_SECS="$(SERVER_DATE_RAW="$SERVER_DATE_RAW" python3 - <<'PY'
import email.utils, os, time
parsed = email.utils.parsedate_to_datetime(os.environ["SERVER_DATE_RAW"])
server_epoch = parsed.timestamp()
local_epoch = time.time()
print(int(abs(server_epoch - local_epoch)))
PY
)"

# 4. Bucket the skew.
if [ "$SKEW_SECS" -ge 300 ]; then
    echo "FAIL  skew=${SKEW_SECS}s endpoint=$URL"
    exit 1
elif [ "$SKEW_SECS" -ge 60 ]; then
    echo "WARN  skew=${SKEW_SECS}s endpoint=$URL"
    exit 0
else
    echo "OK    skew=${SKEW_SECS}s endpoint=$URL"
    exit 0
fi
