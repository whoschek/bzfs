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

# Testing on the Testbed

For reproducible local testing on Ubuntu, consider using [`lima_ubuntu_24_04.sh`](lima_ubuntu_24_04.sh). On macOS or
Linux, this script uses Lima to locally create a guest Ubuntu server VM, then runs the bzfs test suite inside of that
VM.

Also consider running the [`lima_testbed.sh`](lima_testbed.sh) script that uses this to create/delete a local Lima
testbed with N source VMs and M destination VMs for testing, with VM-to-VM SSH networking enabled out of the box. All
default settings work out of the box.

Then consider running the example replication jobconfig script [`bzfs_job_testbed.py`](bzfs_job_testbed.py) on the
testbed, which also works out of the box.
