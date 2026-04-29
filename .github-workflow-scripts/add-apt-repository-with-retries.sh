#!/usr/bin/env bash

set -e
sudo add-apt-repository -y --no-update ${1+"$@"}
script_dir="$(dirname "$(realpath "$0")")"
"${script_dir}/apt-get-update-with-retries.sh"
