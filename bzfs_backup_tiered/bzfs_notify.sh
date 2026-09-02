#!/usr/bin/env bash
#
# Copyright 2024 Wolfgang Hoschek AT mac DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notification transport used by bzfs_alert.sh / bzfs_alert.py. Takes the subject as its only argument and the body on
# stdin. Out of the box it only writes to the system log and to a local file, which is deliberately boring but always
# works; enable one of the commented delivery mechanisms below to actually reach a human.
# Usage: bzfs_notify.sh "<subject>" < body

set -euo pipefail

SUBJECT="$1"
BODY="$(cat)"
LOG_FILE="$HOME/bzfs-job-logs/bzfs-alerts.log" # EDIT ME (optional)

mkdir -p "$(dirname "$LOG_FILE")"
printf '%s %s\n%s\n\n' "$(date -Is)" "$SUBJECT" "$BODY" >> "$LOG_FILE"
logger -t bzfs-alert -- "$SUBJECT"

# EDIT ME: uncomment and adapt exactly one of the following delivery mechanisms.

# Local or relayed email:
# printf '%s\n' "$BODY" | mail -s "$SUBJECT" you@example.com

# ntfy.sh (self-hosted or public); the topic name is the shared secret, so keep it unguessable:
# curl --silent --show-error --fail --max-time 20 --data-binary "$BODY" \
#   --header "Title: $SUBJECT" --header "Priority: high" https://ntfy.example.com/my-unguessable-topic

# Gotify:
# curl --silent --show-error --fail --max-time 20 --form "title=$SUBJECT" --form "message=$BODY" \
#   "https://gotify.example.com/message?token=$(cat "$HOME/.config/bzfs-gotify-token")"

# Pushover:
# curl --silent --show-error --fail --max-time 20 --form-string "token=$(cat "$HOME/.config/bzfs-pushover-token")" \
#   --form-string "user=$(cat "$HOME/.config/bzfs-pushover-user")" --form-string "title=$SUBJECT" \
#   --form-string "message=$BODY" https://api.pushover.net/1/messages.json
