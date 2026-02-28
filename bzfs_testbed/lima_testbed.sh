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

# Creates/deletes a local Lima testbed with N source VMs and M destination VMs for bzfs testing.
# After running this script, consider running the example bzfs_testbed/bzfs_job_testbed.py script to replicate across the VMs.
set -eo pipefail
TESTBED_HOSTNAME_PREFIX="${TESTBED_HOSTNAME_PREFIX:-test}"
export LIMA_MESH_VMS="^${TESTBED_HOSTNAME_PREFIX}.*"
export LIMA_NO_RUN_TESTS="${LIMA_NO_RUN_TESTS:-true}"
TESTBED_NUM_SRC_VMS="${TESTBED_NUM_SRC_VMS:-1}"
TESTBED_NUM_DST_VMS="${TESTBED_NUM_DST_VMS:-1}"
TESTBED_ZPOOL_CAPACITY_MB="${TESTBED_ZPOOL_CAPACITY_MB:-1024}"  # 1GB by default
mydir="$(dirname "$(realpath "$0")")"

usage() {
    prog_name="$(basename "$0")"
    cat <<EOF
Usage: ${prog_name} --create|--delete

Modes:
  --create  Create/start source and destination VMs as configured by TESTBED_NUM_SRC_VMS/TESTBED_NUM_DST_VMS.
  --delete  Stop and delete all Lima VMs whose name starts with TESTBED_HOSTNAME_PREFIX.
EOF
}

create_vm_group() {
    local group="$1"
    local count="$2"
    local i
    local padded_i
    for ((i = 1; i <= count; i++)); do
        printf -v padded_i "%02d" "$i"
        export LIMA_VM_NAME="${TESTBED_HOSTNAME_PREFIX}${group}${padded_i}"
        export LIMA_SSH_PORT=0
        "$mydir/lima_ubuntu_24_04.sh"
        limactl shell --tty=false --workdir=/ "$LIMA_VM_NAME" -- env \
            pool="$group" \
            zpool_capacity_mb="$TESTBED_ZPOOL_CAPACITY_MB" \
            bash -s <<'EOF'
set -eo pipefail
if ! zpool list -H "$pool" >/dev/null 2>&1; then
    dd if=/dev/zero of=~/test_pool_"$pool" bs=1M count="$zpool_capacity_mb" status=none  # create empty test file
    sudo zpool create "$pool" ~/test_pool_"$pool"
fi
if ! sudo zfs list -H "$pool/foo/bar" >/dev/null 2>&1; then
    sudo zfs create -p "$pool/foo/bar"
fi
EOF
    done
}

delete_matching_vms() {
    local lima_vm_names
    local matching_vm_names=()
    local vm
    lima_vm_names="$(limactl list --tty=false --format='{{.Name}}')"
    while IFS= read -r vm; do
        matching_vm_names+=("$vm")
    done < <(grep -E -- "^${TESTBED_HOSTNAME_PREFIX}.*" <<<"$lima_vm_names" || [[ "$?" -eq 1 ]])  # 1 means "no match"
    for vm in "${matching_vm_names[@]}"; do
        echo "Stopping Lima VM: $vm"
        limactl stop --tty=false --force "$vm" || true
    done
    for vm in "${matching_vm_names[@]}"; do
        echo "Deleting Lima VM: $vm"
        limactl delete --tty=false --force "$vm"
    done
}

if [[ "$#" -ne 1 ]]; then
    usage >&2
    exit 2
fi

case "$1" in
    --create)
        echo "Creating Lima testbed with $TESTBED_NUM_SRC_VMS source VMs and $TESTBED_NUM_DST_VMS destination VMs ..."
        create_vm_group "src" "$TESTBED_NUM_SRC_VMS"
        create_vm_group "dst" "$TESTBED_NUM_DST_VMS"
        success_msg="Success! Recommended next manual step is login: limactl shell --workdir=/bzfs ${TESTBED_HOSTNAME_PREFIX}dst01"
        ;;
    --delete)
        echo "Deleting Lima testbed with hostname prefix '$TESTBED_HOSTNAME_PREFIX' ..."
        delete_matching_vms
        success_msg="Success!"
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo "Unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
esac

limactl list --tty=false
echo
echo "$success_msg"
