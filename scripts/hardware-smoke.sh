#!/usr/bin/env bash
set -euo pipefail

bt_daemon=${1:-bt-daemon}

call() {
  local id=$1 method=$2 params=${3:-'{}'}
  printf '{"op":"call","id":"%s","method":"%s","params":%s}\n{"op":"shutdown","id":"shutdown"}\n' \
    "$id" "$method" "$params" \
    | "$bt_daemon" client \
    | jq -cer --arg id "$id" 'select(.kind == "response" and .id == $id) | .response'
}

snapshot=$(call snapshot bluetooth.snapshot)
jq -e '
  .protocol == "bt-api" and .version == 1 and .ok == true and
  (.data.snapshot.adapters | type == "array") and
  (.data.snapshot.devices | type == "array") and
  all(.data.snapshot.adapters[]; (.key | startswith("adapter-")) and (.key | contains(":") | not)) and
  all(.data.snapshot.devices[]; (.key | startswith("device-")) and (.key | contains(":") | not)) and
  (([.data.snapshot.adapters[].key] | length) == ([.data.snapshot.adapters[].key] | unique | length)) and
  (([.data.snapshot.devices[].key] | length) == ([.data.snapshot.devices[].key] | unique | length))
' >/dev/null <<<"$snapshot"

obex=$(call obex bluetooth.obex.snapshot)
jq -e '.protocol == "bt-api" and .version == 1 and .ok == true and (.data.obex.available | type == "boolean")' >/dev/null <<<"$obex"

audio=$(call audio bluetooth.audio.snapshot)
jq -e '.protocol == "bt-api" and .version == 1 and .ok == true and (.data.audio_devices | type == "array")' >/dev/null <<<"$audio"

jq -n \
  --argjson snapshot "$snapshot" \
  --argjson obex "$obex" \
  --argjson audio "$audio" \
  '{
    adapters: ($snapshot.data.snapshot.adapters | length),
    devices: ($snapshot.data.snapshot.devices | length),
    connected_devices: ([$snapshot.data.snapshot.devices[] | select(.connected)] | length),
    battery_reports: ([$snapshot.data.snapshot.devices[].battery[]] | length),
    audio_devices: ($audio.data.audio_devices | length),
    obex: $obex.data.obex
  }'
