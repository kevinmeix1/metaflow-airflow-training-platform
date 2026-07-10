#!/usr/bin/env bash
set -euo pipefail

SCREENSHOT_DIR="${1:-docs/screenshots}"
AUDIO="${2:-.local/demo/training-judge-demo.mp3}"
OUTPUT="${3:-docs/demo/training-judge-demo.mp4}"

command -v ffmpeg >/dev/null || { echo "ffmpeg is required" >&2; exit 1; }
test -f "$AUDIO" || { echo "Missing narration: run make demo-voice" >&2; exit 1; }
test -f "$SCREENSHOT_DIR/dashboard.png" || { echo "Missing dashboard screenshot" >&2; exit 1; }
test -f "$SCREENSHOT_DIR/dashboard-evidence.png" || { echo "Missing evidence screenshot" >&2; exit 1; }
test -f "$SCREENSHOT_DIR/dashboard-mobile.png" || { echo "Missing mobile screenshot" >&2; exit 1; }

mkdir -p "$(dirname "$OUTPUT")"
ffmpeg -y \
  -loop 1 -t 50 -i "$SCREENSHOT_DIR/dashboard.png" \
  -loop 1 -t 65 -i "$SCREENSHOT_DIR/dashboard-evidence.png" \
  -loop 1 -t 50 -i "$SCREENSHOT_DIR/dashboard-mobile.png" \
  -i "$AUDIO" \
  -filter_complex \
    "[0:v]scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=#f5f7fa,format=yuv420p[v0]; \
     [1:v]scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=#f5f7fa,format=yuv420p[v1]; \
     [2:v]scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=#f5f7fa,format=yuv420p[v2]; \
     [v0][v1][v2]concat=n=3:v=1:a=0[video]" \
  -map "[video]" -map 3:a \
  -c:v libx264 -profile:v high -crf 20 -c:a aac -b:a 160k -shortest "$OUTPUT"
echo "$OUTPUT"
