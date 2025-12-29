#!/bin/sh
set -e

# Check user and /data writability
UID=$(id -u 2>/dev/null || echo '?')
GID=$(id -g 2>/dev/null || echo '?')
USER=$(id -u -n 2>/dev/null || echo '?')
echo "[startup] Running as: user=$USER uid=$UID gid=$GID"

# Check /data is writable (fail immediately if not)
if ! sh -c "printf '' > /data/.llm_mount_test" 2>/dev/null; then
  echo "[startup] ERROR: /data is not writable. Aborting."
  exit 1
fi
rm -f /data/.llm_mount_test 2>/dev/null || true

# Standard single-server mode (FastAPI only with background worker)
if [ "$#" -eq 0 ]; then
  echo "[startup] ERROR: No command provided. entrypoint.sh requires CMD to be set in Dockerfile."
  exit 1
fi

echo "[startup] Launching: $@"
exec "$@"


