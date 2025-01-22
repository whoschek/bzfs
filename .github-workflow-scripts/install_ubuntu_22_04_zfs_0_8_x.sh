#!/usr/bin/env sh
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

set -e # Exit immediately if a cmd returns a non-zero status

echo "=== Adding focal repositories for zfs-0.8.x ==="
cat << EOF | sudo tee /etc/apt/sources.list.d/focal-zfs.list
deb http://archive.ubuntu.com/ubuntu focal main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu focal-updates main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu focal-security main restricted universe multiverse
EOF

echo "=== Pinning ZFS packages to focal ==="
cat << EOF | sudo tee /etc/apt/preferences.d/focal-zfs-pin
Package: *
Pin: release n=focal
Pin-Priority: 100

Package: zfs*
Pin: release n=focal
Pin-Priority: 700
EOF

sudo apt-get update --quiet

echo "=== Installing zfs-0.8.x packages ==="
sudo apt-get install -y --no-install-recommends zfs-dkms=0.8.3-1ubuntu12.18 zfsutils-linux=0.8.3-1ubuntu12.18
