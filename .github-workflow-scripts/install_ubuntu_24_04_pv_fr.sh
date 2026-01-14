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

# build & install pv (Pipe Viewer) with French localization
set -euo pipefail

sudo apt-get -y install gettext locales language-pack-fr

#PV_VERSION="git-main"
PV_VERSION="1.9.31"
#PV_VERSION="1.9.25"
#PV_VERSION="1.9.15"
#PV_VERSION="1.9.7"
#PV_VERSION="1.8.14"
#PV_VERSION="1.8.5"
#PV_VERSION="-"
if [ "$PV_VERSION" != "-" ]; then
    echo "pv ${PV_VERSION} is being installed with French localization ..."
    sudo apt-get -y install build-essential autopoint
    WORKDIR=$(mktemp -d)
    cd "$WORKDIR"
    if [ "$PV_VERSION" == "git-main" ]; then
        git clone https://codeberg.org/ivarch/pv.git
        cd pv
    else
        PV_URL="https://www.ivarch.com/programs/sources/pv-${PV_VERSION}.tar.gz"
        curl -fsSL --retry 999 --retry-delay 1 --retry-max-time 60 --retry-all-errors -O "${PV_URL}"
        tar xzf "pv-${PV_VERSION}.tar.gz"
        cd "pv-${PV_VERSION}"
    fi
    autoreconf -fi
    ./configure --enable-nls --quiet
    make --quiet
    sudo make install --quiet
    echo "✓ pv ${PV_VERSION} installed with French localization."
    pv --version

    sudo locale-gen fr_FR.UTF-8
    locale
    dd if=/dev/zero bs=1k count=8 | pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
    export LC_ALL=fr_FR.UTF-8
    dd if=/dev/zero bs=1k count=16 | pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
    dd if=/dev/zero bs=1k count=32 | LC_ALL=C pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
    export LC_MESSAGES=fr_FR.UTF-8
    dd if=/dev/zero bs=1k count=64 | LC_ALL=C pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
    export LANG=fr_FR.UTF-8
    dd if=/dev/zero bs=1k count=128 | LC_ALL=C pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
    export LANGUAGE=fr_FR:fr
    dd if=/dev/zero bs=1k count=256 | LC_ALL=C pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
    dd if=/dev/zero bs=1k count=512 | LC_ALL=C pv --progress --timer --eta --fineta --rate --average-rate --bytes --interval=1 --width=120 --buffer-size=2M --force >/dev/null
else
    sudo locale-gen fr_FR.UTF-8
    locale
fi
