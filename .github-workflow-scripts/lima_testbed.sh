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
# After running this script, consider running the example bzfs_tests/bzfs_job_testbed.py script to replicate across the VMs.
set -eo pipefail
LIMA_HOSTNAME_PREFIX="${LIMA_HOSTNAME_PREFIX:-test}"
export LIMA_MESH_VMS="^${LIMA_HOSTNAME_PREFIX}.*"
export LIMA_NO_RUN_TESTS="${LIMA_NO_RUN_TESTS:-true}"
NUM_SRC="${NUM_SRC:-1}"
NUM_DST="${NUM_DST:-1}"
mydir="$(dirname "$(realpath "$0")")"

usage() {
    prog_name="$(basename "$0")"
    cat <<EOF
Usage: ${prog_name} --create|--delete

Modes:
  --create  Create/start source and destination VMs as configured by NUM_SRC/NUM_DST.
  --delete  Stop and delete all Lima VMs whose name starts with LIMA_HOSTNAME_PREFIX.
EOF
}

create_vm_group() {
    local group="$1"
    local count="$2"
    local i
    local padded_i
    for ((i = 1; i <= count; i++)); do
        printf -v padded_i "%02d" "$i"
        export LIMA_VM_NAME="${LIMA_HOSTNAME_PREFIX}${group}${padded_i}"
        export LIMA_SSH_PORT=0
        "$mydir/test_ubuntu_24_04_lima.sh"
        limactl shell --tty=false --workdir=/ "$LIMA_VM_NAME" -- env \
            pool="$group" \
            bash -s <<'EOF'
set -eo pipefail
if ! zpool list -H "$pool" >/dev/null 2>&1; then
    dd if=/dev/zero of=~/test_pool_"$pool" bs=1M count=1000 status=none  # create empty 1GB test file
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
    done < <(grep -E -- "^${LIMA_HOSTNAME_PREFIX}.*" <<<"$lima_vm_names" || [[ "$?" -eq 1 ]])  # 1 means "no match"
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
        echo "Creating Lima testbed with $NUM_SRC source VMs and $NUM_DST destination VMs ..."
        create_vm_group "src" "$NUM_SRC"
        create_vm_group "dst" "$NUM_DST"
        ;;
    --delete)
        echo "Deleting Lima testbed with hostname prefix '$LIMA_HOSTNAME_PREFIX' ..."
        delete_matching_vms
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
echo "Success! Recommended next manual step is login: limactl shell --workdir=/bzfs ${LIMA_HOSTNAME_PREFIX}dst01"
