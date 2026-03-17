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

For a smaller single-container Ubuntu 24.04 environment, use [`docker_create.sh`](docker_create.sh). It rebuilds the
image from [`Dockerfile.ubuntu-24.04`](Dockerfile.ubuntu-24.04), replaces any existing `bzfs-ubuntu-24.04` container,
and checks out `bzfs` into `/bzfs`. By default it selects the latest stable `v*` git tag via generalized numeric sort
(`sort -V`), and you can override that with `BZFS_GIT_TAG=...`. If you want to publish the image to Docker Hub after
`docker login`, set `BZFS_DOCKER_PUSH=true` and `BZFS_DOCKERHUB_USER=...`. The default Docker Hub tag is
`$BZFS_DOCKER_OS-$BZFS_GIT_TAG`, which for this script means `ubuntu-24.04-vX.Y.Z`.
