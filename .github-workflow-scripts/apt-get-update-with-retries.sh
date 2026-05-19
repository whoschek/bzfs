#!/usr/bin/env bash

set -e
sudo apt-get -y update ${1+"$@"}
