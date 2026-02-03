#!/bin/bash
set -euo pipefail

WORKSPACE="/workspace/PersonalML"
DATA_DIR="$WORKSPACE/data"

TMP_DIR="$WORKSPACE/tmp"
TMP_TAR="$TMP_DIR/data.tar.gz"

GDRIVE_DEST="gdrive:/PersonalML_backups"
LOG_FILE="$WORKSPACE/backup.log"

mkdir -p "$TMP_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

perform_backup() {
  log "========== STARTING BACKUP =========="

  if [ ! -d "$DATA_DIR" ]; then
    log "ERROR: $DATA_DIR not found."
    return 1
  fi

  # Skip if no changes in last hour
  if ! find "$DATA_DIR" -type f -mmin -60 | head -n 1 | grep -q .; then
    log "No changes in last hour; skipping."
    return 0
  fi

  log "Action: Tarring data → $TMP_TAR"
  ionice -c2 -n7 nice -n 19 \
    tar -czf "$TMP_TAR" -C "$WORKSPACE" "data"

  log "Action: Uploading to GDrive..."
  rclone copy "$TMP_TAR" "$GDRIVE_DEST" \
    --transfers 1 --checkers 2 --bwlimit 8M

  rm -f "$TMP_TAR"
  log "SUCCESS: Backup pushed."
  log "========== BACKUP FINISHED =========="
}

log "Backup service started. Running every 1 hour..."
while true; do
  perform_backup || true
  log "Waiting for 1 hour..."
  sleep 3600
done
