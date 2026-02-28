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

# Configures a basic mesh network, i.e. mutual SSH trust and hostname aliases for matching Lima VMs.
# This enables each matching VM to talk via ssh to each other matching VM, by appending the public key of
# each matching VM to the ~/.ssh/authorized_keys file of each other matching VM.
# Matching applies a `grep -E` regex to all known Lima VM names. The default is zero matching VMs.
# Also makes it so that, on each matching VM, `ssh -p N <vm_name>` connects to that other VM, via ~/.ssh/config aliases.
set -eo pipefail
LIMA_MESH_VMS="${LIMA_MESH_VMS:-}"  # optional `grep -E` regex to select VMs for mutual SSH trust in a mesh network
LIMA_WORKDIR="/"

# find matching Lima VMs
lima_mesh_vm_names=()
if [[ "$LIMA_MESH_VMS" != "" ]]; then
    lima_vm_names="$(limactl list --tty=false --format='{{.Name}}')"
    lima_mesh_matches="$(grep -E -- "$LIMA_MESH_VMS" <<<"$lima_vm_names")" || [[ "$?" -eq 1 ]]  # 1 means "no match"
    if [[ "$lima_mesh_matches" != "" ]]; then
        while IFS= read -r vm; do
            lima_mesh_vm_names+=("$vm")
        done <<<"$lima_mesh_matches"
    fi
fi
echo "LIMA_MESH_VMS matches: ${lima_mesh_vm_names[*]}"

if [[ ${#lima_mesh_vm_names[@]} -gt 1 ]]; then
    # fetch public keys of matching VMs
    lima_mesh_public_keys=()
    for lima_mesh_vm_name in "${lima_mesh_vm_names[@]}"; do
        if [[ "$(limactl list --tty=false --format='{{.Status}}' "$lima_mesh_vm_name")" != "Running" ]]; then
            echo "LIMA_MESH_VMS matched a VM that is not running: $lima_mesh_vm_name" >&2
            exit 1
        fi
        lima_mesh_public_key="$(limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$lima_mesh_vm_name" -- bash -lc 'cat ~/.ssh/id_rsa.pub' | tr -d '\r\n')"
        lima_mesh_public_keys+=("$lima_mesh_public_key")
    done

    # append the public key of each matching VM to the ~/.ssh/authorized_keys file of each other matching VM
    for ((j = 0; j < ${#lima_mesh_vm_names[@]}; j++)); do
        source_vm_name="${lima_mesh_vm_names[$j]}"
        source_public_key="${lima_mesh_public_keys[$j]}"
        source_ssh_alias_args=()
        for ((i = 0; i < ${#lima_mesh_vm_names[@]}; i++)); do
            target_vm_name="${lima_mesh_vm_names[$i]}"
            if [[ "$source_vm_name" == "$target_vm_name" ]]; then
                continue
            fi
            source_ssh_alias_args+=("$target_vm_name" "lima-${target_vm_name}.internal")  # lima:user-v2 network enables VM-to-VM connectivity
            limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$target_vm_name" -- bash -s -- "$source_public_key" <<'EOF'
set -eo pipefail
source_public_key="$1"
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"
if ! grep -Fqx -- "$source_public_key" "$HOME/.ssh/authorized_keys"; then  # append public key if it isn't already present
    printf '%s\n' "$source_public_key" >> "$HOME/.ssh/authorized_keys"
fi
EOF
        done

        # make it so that, on each matching VM, `ssh -p N <vm_name>` connects to that other VM, via ~/.ssh/config aliases.
        limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$source_vm_name" -- bash -s -- "${source_ssh_alias_args[@]}" <<'EOF'
set -eo pipefail
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
touch "$HOME/.ssh/config"
chmod 600 "$HOME/.ssh/config"
mesh_config_path="$HOME/.ssh/config.bzfs-lima-mesh"
include_line="Include ~/.ssh/config.bzfs-lima-mesh"
sed -i '\|^Include ~/.ssh/config.bzfs-lima-mesh$|d' "$HOME/.ssh/config"  # remove existing Include line before reinserting
if [[ ! -s "$HOME/.ssh/config" ]]; then
    printf '%s\n' "$include_line" > "$HOME/.ssh/config"  # empty file case
else
    sed -i "1i$include_line" "$HOME/.ssh/config"  # prepend Include line to ensure global scope (a no-op on empty file)
fi
rm -f "$mesh_config_path"
touch "$mesh_config_path"
chmod 600 "$mesh_config_path"
while [[ "$#" -gt 0 ]]; do
    target_vm_name="$1"
    target_hostname="$2"
    shift 2
    cat >> "$mesh_config_path" <<CONFIG_EOF
Host $target_vm_name
    HostName $target_hostname
    StrictHostKeyChecking no      # ok for ssh client on test VM A to connect to test VM B
    UserKnownHostsFile /dev/null  # ok for ssh client on test VM A to connect to test VM B
    ConnectTimeout 5

CONFIG_EOF
done
EOF
    done
fi

# verify mesh connectivity
for source_vm_name in "${lima_mesh_vm_names[@]}"; do
    for target_vm_name in "${lima_mesh_vm_names[@]}"; do
        if [[ "$source_vm_name" == "$target_vm_name" ]]; then
            continue
        fi
        limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$source_vm_name" -- ssh -n -oBatchMode=yes "$target_vm_name" true
        target_hostname="$(limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$source_vm_name" -- ssh -n -oBatchMode=yes "$target_vm_name" hostname)"
        if [[ "$target_hostname" != "$target_vm_name" ]]; then
            echo "Mesh hostname verification failed: source=$source_vm_name target=$target_vm_name got=$target_hostname" >&2
            exit 1
        fi
    done
done
