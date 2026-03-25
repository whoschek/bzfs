<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.-->

# Docker Testbed Helpers

This directory contains helper files for running `bzfs` inside a privileged Docker or nerdctl container on the local
testbed. The intended environment is VMs created via [`../lima_testbed.sh`](../lima_testbed.sh) where ZFS and VM-to-VM
SSH connectivity is already available.

## Files

- [`Dockerfile`](Dockerfile): Builds an image with `bzfs`, `bzfs_jobrunner`, ZFS userland, cron, OpenSSH, and optional
  hpnssh.
- [`docker_image.sh`](docker_image.sh): Builds a local image from the latest stable `v*` git tag by default, and can
  optionally push multi-arch images to a registry.
- [`docker_run_example.sh`](docker_run_example.sh): Starts or removes the container and runs the example
  `bzfs_job_testbed.py` job inside it.
- [`cronjob_example`](cronjob_example): Example cron file for periodic job execution inside the container.

## Prerequisites

- A Linux host or VM with ZFS installed.
- A rootful container runtime reachable as `docker` or `nerdctl` CLI.
- SSH keys already present in `~/.ssh`, because that directory is bind-mounted into the container.
- The example [`../bzfs_job_testbed.py`](../bzfs_job_testbed.py) script because `runjob` executes that job config.

## Build an Image

Run this on each testbed VM that should host a container:

```bash
cd bzfs_testbed/docker
sudo ./docker_image.sh
```

By default the script:

- Determines the latest stable `bzfs` git tag from `https://github.com/whoschek/bzfs.git`.
- Builds an image for the local host architecture.
- Tags it as `<git-tag>-<os>`, for example `v1.19.0-ubuntu-24.04`.
- Runs a sanity check that `bzfs` and `bzfs_jobrunner` start successfully.

Useful overrides:

```bash
BZFS_GIT_TAG=v1.19.0 sudo ./docker_image.sh
```

## Run the Example Container

The example runner uses the local image tag from `BZFS_DOCKER_IMAGE`, defaulting to `v1.19.0-ubuntu-24.04`. Override it
if you use a different tag or registry.

Bring the container up on each VM:

```bash
cd bzfs_testbed/docker
BZFS_DOCKER_IMAGE=v1.19.0-ubuntu-24.04 ./docker_run_example.sh up
```

The script forwards the host port `2222` to container port `2222`, which is where OpenSSH listens inside of the
container. To use `hpnsshd` instead, recreate the container with:

```bash
cd bzfs_testbed/docker
./docker_run_example.sh down
BZFS_DOCKER_INSTALL_HPNSSH=true BZFS_DOCKER_IMAGE=v1.19.0-ubuntu-24.04 ./docker_run_example.sh up
```

`up` performs the following:

- Prepares `~/bzfs-config/etc/ssh` and `~/bzfs-config/etc/hpnssh` on the host and configures whether OpenSSH or hpnsshd
  listens on port `2222` inside of the container.
- Creates host directories `~/bzfs-config`, `~/bzfs-job-logs`, and `~/bzfs-logs`, if they do not already exist.
- Starts a privileged container named `bzfs`.
- Bind-mounts host SSH config, user SSH keys, config files and log directories.
- Reloads managed cron files from `~/bzfs-config/cron.d`.

Run the example job after all peer containers are up:

```bash
./docker_run_example.sh runjob
```

This executes `bzfs_testbed/bzfs_job_testbed.py` inside the container and uses SSH port `2222` for both source and
destination hosts.

If there are problems consider entering an interactive shell inside the running container for debugging:

```bash
./docker_run_example.sh shell
```

Remove the container:

```bash
./docker_run_example.sh down
```

## Cron Jobs

If `~/bzfs-config/cron.d/` exists, `up` copies its files into `/etc/cron.d/` inside the container. To install the
included [`cronjob_example.sh`](cronjob_example), edit `USER_NAME` and `USER_HOME` in that file, then reload cron jobs:

```bash
mkdir -p ~/bzfs-config/cron.d
cp cronjob_example ~/bzfs-config/cron.d/
./docker_run_example.sh up
```

The cron job writes its output below `~/bzfs-job-logs/cron/`.
