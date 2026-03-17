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

# End-to-End Testing on the Testbed

For reproducible local testing, consider using [`lima_vm.sh`](lima_vm.sh). On macOS or Linux, this script uses Lima to
locally create a guest Ubuntu or AlmaLinux VM, then installs ZFS and runs the bzfs test suite inside of that VM.

Also consider running the [`lima_testbed.sh`](lima_testbed.sh) script that uses this to create/delete a local Lima
testbed with N source VMs and M destination VMs for testing, with ZFS and VM-to-VM SSH connectivity working out of the
box. All default settings work out of the box.

Then consider running the example replication jobconfig script [`bzfs_job_testbed.py`](bzfs_job_testbed.py) on the
testbed, which also works out of the box.

Or instead, simply tell Codex, Claude, or any comparable agent something like "Run the bzfs_job_testbed.py example
replication script on the testbed." It will figure out the rest and do it.

For a small single-container Ubuntu environment, consider using [`docker_create.sh`](docker_create.sh). It builds a
minimal docker image with a checkout of the latest stable `v*` git tag of `bzfs`. If you want to publish the image to a
registry such as Docker Hub after `docker login`, set `BZFS_DOCKER_PUSH=true` and `BZFS_DOCKER_REGISTRY_PREFIX=...`. The
default pushed image reference is `${BZFS_DOCKER_REGISTRY_PREFIX}/bzfs:${BZFS_GIT_TAG}-${BZFS_DOCKER_OS}`, for example
`docker.io/mydockerhubuser/bzfs:v1.19.0-ubuntu-24.04`. Published images include both `linux/amd64` and `linux/arm64`.
