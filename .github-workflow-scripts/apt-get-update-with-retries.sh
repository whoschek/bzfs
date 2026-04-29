#!/usr/bin/env bash

set -e
sudo apt-get \
    -o Acquire::Retries=3 \
    -o Acquire::http::Timeout=30 \
    -o Acquire::https::Timeout=30 \
    -y update ${1+"$@"}
