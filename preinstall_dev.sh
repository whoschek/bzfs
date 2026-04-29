#!/usr/bin/env bash

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

# Prepare for installation of optional development dependencies

COOLDOWN_DAYS=7  # for background see https://nesbitt.io/2026/03/04/package-managers-need-to-cool-down.html
#export PIP_UPLOADED_PRIOR_TO="P${COOLDOWN_DAYS}D"  # requires pip >= 26.1
PIP_UPLOADED_PRIOR_TO="$(python3 -c "from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=$COOLDOWN_DAYS)).strftime('%Y-%m-%dT%H:%M:%SZ'))")"
export PIP_UPLOADED_PRIOR_TO
export UV_EXCLUDE_NEWER="$COOLDOWN_DAYS days"
export NPM_CONFIG_MIN_RELEASE_AGE=$COOLDOWN_DAYS

export NPM_CONFIG_IGNORE_SCRIPTS=true
export PIP_ONLY_BINARY=:all:
export PIP_NO_BINARY=argparse-manpage  # not yet available as wheels

python3 -m pip install --upgrade pip
python3 -m pip --version
