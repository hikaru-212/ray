#!/usr/bin/env bash
set -u
phase2_root=/tmp/ray-pr67424-phase2-lgVIc0KN
phase2_label=$1
shift
if [[ -e "$phase2_root/logs/$phase2_label.command" ]]; then
  echo 'Refusing to overwrite existing evidence' >&2
  exit 90
fi
{ printf 'cwd=%q\n' "$PWD"; printf '%q ' "$@"; printf '\n'; } > "$phase2_root/logs/$phase2_label.command"
date -u +%FT%TZ > "$phase2_root/logs/$phase2_label.start"
"$@" > "$phase2_root/logs/$phase2_label.output" 2>&1
phase2_status=$?
printf '%s\n' "$phase2_status" > "$phase2_root/logs/$phase2_label.exit"
date -u +%FT%TZ > "$phase2_root/logs/$phase2_label.end"
cat "$phase2_root/logs/$phase2_label.output"
printf '\nRecorded exit status: %s\n' "$phase2_status"
exit "$phase2_status"
