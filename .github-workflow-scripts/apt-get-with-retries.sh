#!/usr/bin/env bash

set -e
sudo apt-get -y -o Acquire::Retries=12 ${1+"$@"}
