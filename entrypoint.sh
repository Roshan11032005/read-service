#!/bin/sh
# Fix /data/hotcache ownership at runtime so BadgerDB can write its LOCK file
# even when the host volume was created by root.
# This script runs as root, fixes the dir, then drops to appuser.
if [ -d "/data/hotcache" ]; then
    chown -R appuser:appgroup /data/hotcache 2>/dev/null || true
fi
exec su-exec appuser /app/read-orchestrator "$@"