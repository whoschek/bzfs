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

# This script can run on macOS on Apple Silicon, or on Linux on any arch.
# The script uses Lima to locally create a guest Ubuntu server VM, then runs the bzfs test suite inside of that VM.
# Currently uses ubuntu-24.04, python-3.12, and zfs-2.4 or zfs-2.2 depending on the value of $LIMA_ZFS_VERSION.
# Cold start of the guest VM takes ~30 seconds with defaults; warm start takes ~1.5 seconds.

# shellcheck disable=SC2154
set -eo pipefail
LIMA_VM_TEMPLATE="${LIMA_VM_TEMPLATE:-ubuntu-24.04}"  # see `limactl create --list-templates` for available options
LIMA_VM_NAME="${LIMA_VM_NAME:-mylimavm}"
LIMA_VM_DISK="${LIMA_VM_DISK:-10}"  # GiB
LIMA_VM_CPUS="${LIMA_VM_CPUS:-0}"  # 0 uses Lima default which currently is min(4, #cores)
LIMA_VM_MEMORY="${LIMA_VM_MEMORY:-4}"  # GiB
LIMA_VM_RECREATE="${LIMA_VM_RECREATE:-false}"  # to init VM from scratch
LIMA_SSH_PORT="${LIMA_SSH_PORT:-0}"  # 0 picks random unused port;
                                     # host box: ssh 127.0.0.1:$LIMA_SSH_PORT --> guest VM
                                     # guest VM: ssh 127.0.0.1:$LIMA_SSH_PORT --> guest VM loopback
LIMA_COPY_BASHRC="${LIMA_COPY_BASHRC:-false}"  # opt-in: copy host ~/.bashrc into guest ~/.bashrc
LIMA_NO_RUN_TESTS="${LIMA_NO_RUN_TESTS:-false}"  # to skip running tests
LIMA_HOST_WORKDIR="$(dirname "$(dirname "$(realpath "$0")")")"  # aka git repo root dir
LIMA_WORKDIR=/bzfs  # this is also the dir where $LIMA_HOST_WORKDIR is mounted within the guest VM
LIMA_WORKDIR_WRITABLE="${LIMA_WORKDIR_WRITABLE:-false}"  # false=read-only, true=read-write shared with host
  # 'false' is for running tests only. 'true' enables repo file changes, e.g. for limited sharing with a sandboxed AI agent.
LIMA_ZFS_VERSION="${LIMA_ZFS_VERSION:-}"  # can be empty or "zfs-2.4"

# Install Lima if it isn't already installed
if ! command -v limactl >/dev/null 2>&1; then
    if [[ "$(uname -s)" == "Darwin" ]]; then
        HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 brew install lima
    else
        echo "Please install Lima manually before running this script. See https://lima-vm.io/docs/installation/"
        exit 1
    fi
fi

# Delete prior state; init VM from scratch
if [[ "$LIMA_VM_RECREATE" == "true" ]]; then
    limactl stop --tty=false --force "$LIMA_VM_NAME" || true
    limactl delete --tty=false --force "$LIMA_VM_NAME"
fi

# Create VM if it doesn't already exist
lima_vm_names="$(limactl list --tty=false --format='{{.Name}}')"
if ! grep -Fqx -- "$LIMA_VM_NAME" <<<"$lima_vm_names"; then
    limactl create --tty=false \
        --name="$LIMA_VM_NAME" \
        --disk="$LIMA_VM_DISK" \
        --cpus="$LIMA_VM_CPUS" \
        --memory="$LIMA_VM_MEMORY" \
        --set=".ssh.loadDotSSHPubKeys=true" \
        --set=".mounts = []" \
        --set=".mounts += [{\"location\":\"$LIMA_HOST_WORKDIR\",\"mountPoint\":\"$LIMA_WORKDIR\",\"writable\":$LIMA_WORKDIR_WRITABLE}]" \
        --containerd=none \
        template:"$LIMA_VM_TEMPLATE"
        # Note: ".ssh.loadDotSSHPubKeys=true" imports ~/.ssh/*.pub from host into the guest VM ~/.ssh/authorized_keys
fi

# Start VM if it isn't already running
lima_vm_statuses="$(limactl list --tty=false --format='{{.Name}} {{.Status}}')"
if ! grep -Fqx -- "${LIMA_VM_NAME} Running" <<<"$lima_vm_statuses"; then
    limactl start --tty=false --name="$LIMA_VM_NAME" --ssh-port="$LIMA_SSH_PORT" --timeout="${LIMA_START_TIMEOUT:-2m}" --progress="${LIMA_START_PROGRESS:-false}"
fi
LIMA_SSH_PORT="$(limactl list --tty=false --format="{{if eq .Name \"$LIMA_VM_NAME\"}}{{.SSHLocalPort}}{{end}}")"
LIMA_SSH_PORT="$(tr -d '[:space:]' <<<"$LIMA_SSH_PORT")"
mkdir -p "$LIMA_HOST_WORKDIR/venv"  # makes `mount` later succeed in guest VM even in read-only mode

# Prepare VM
limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- env \
    LIMA_SSH_PORT="$LIMA_SSH_PORT" \
    LIMA_ZFS_VERSION="$LIMA_ZFS_VERSION" \
    bash -s <<'EOF'

set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
if [[ ! -f ~/.bzfs_apt_update_done ]]; then
    echo "Now running 'apt-get update' ..."
    sudo apt-get -y -qq update
    # sudo apt-get -y full-upgrade
    touch ~/.bzfs_apt_update_done
fi

# Upgrade zfs kernel + userland to specific upstream zfs version
if [[ "$LIMA_ZFS_VERSION" == "zfs-2.4" ]]; then
    # see https://launchpad.net/~patrickdk/+archive/ubuntu/zfs/+packages
    sudo add-apt-repository ppa:patrickdk/zfs; sudo apt-get -y update
    sudo apt-get -y install zfs-dkms
    # Ensure the just-installed DKMS module is actually the loaded kernel module, and userland has same ZFS version as kernel
    sudo systemctl stop zfs-zed.service || true
    sudo modprobe --remove zfs
    sudo modprobe zfs
else
    sudo apt-get -y install zfsutils-linux
fi
zfs --version

# Run common preparation steps
sudo apt-get -y install python3 zstd mbuffer pv rsync ripgrep python3-venv
# sudo apt-get -y install pandoc git nano mosh curl wget rclone jq net-tools tree

mkdir -p "$HOME/.ssh"
rm -f "$HOME/.ssh/id_rsa" "$HOME/.ssh/id_rsa.pub"
ssh-keygen -t rsa -f "$HOME/.ssh/id_rsa" -q -N ""  # create private key and public key
cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"

# Keep default SSH port 22 semantics untouched, and add LIMA_SSH_PORT to socket-activated SSH service
if [[ "$LIMA_SSH_PORT" == "22" ]]; then
    printf 'Port 22\n' | sudo tee /etc/ssh/sshd_config.d/99-bzfs-extra-ports.conf > /dev/null
else
    printf 'Port 22\nPort %s\n' "$LIMA_SSH_PORT" | sudo tee /etc/ssh/sshd_config.d/99-bzfs-extra-ports.conf > /dev/null
fi
sudo systemctl daemon-reload
sudo systemctl restart ssh.socket
ssh -n -oBatchMode=yes -oStrictHostKeyChecking=accept-new -oConnectTimeout=5 -p "$LIMA_SSH_PORT" 127.0.0.1 echo hello1  # verify
ssh -n -oBatchMode=yes -oStrictHostKeyChecking=accept-new -oConnectTimeout=5 -p "$LIMA_SSH_PORT" 127.0.0.2 echo hello2  # verify

setup_bind_mount() {  # also ensures mount persists across reboot and restart of guest VM
    local source_dir="$1"
    local target_dir="$2"
    local fstab_entry="$source_dir $target_dir none bind,nofail 0 0"
    mkdir -p "$source_dir" "$target_dir"
    if ! grep -Fqx -- "$fstab_entry" /etc/fstab; then
        printf '%s\n' "$fstab_entry" | sudo tee -a /etc/fstab > /dev/null
    fi
    if ! mountpoint -q "$target_dir"; then
        sudo mount "$target_dir"
    fi
}
# Enable guest VM to have its own venv and git hooks
setup_bind_mount "$HOME/.bzfs-lima/venv" "$(pwd)/venv"
setup_bind_mount "$HOME/.bzfs-lima/git-hooks" "$(pwd)/.git/hooks"

# Display ZFS version and Python version
id -u -n
uname -a
zfs --version
python3 --version
ssh -V
zstd --version
pv --version | head -n 1
mbuffer --version |& head -n 1
command -v sh | xargs ls -l
EOF

# Optionally copy host ~/.bashrc into guest ~/.bashrc
if [[ "$LIMA_COPY_BASHRC" == "true" && -f "$HOME/.bashrc" ]]; then
    limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- bash -lc 'cat > ~/.bashrc' < "$HOME/.bashrc"
fi
echo "LIMA_SSH_PORT: $LIMA_SSH_PORT"

# Run test suite inside VM
if [[ "$LIMA_NO_RUN_TESTS" == "false" ]]; then
    limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- env \
        bzfs_test_mode="$bzfs_test_mode" \
        bzfs_test_no_run_quietly="$bzfs_test_no_run_quietly" \
        bash -lc './test.sh'
fi

# Alternatively (or subsequently), you can now run tests in a more flexible way by setting env vars for test.sh like so:
if false; then
    export bzfs_test_remote_userhost=127.0.0.1  # 127.0.0.1:$LIMA_SSH_PORT is the Ubuntu guest VM
    export bzfs_test_remote_path=mybzfs
    export bzfs_test_ssh_port="$LIMA_SSH_PORT"  # 127.0.0.1:$LIMA_SSH_PORT is the Ubuntu guest VM
    export bzfs_test_remote_private_key=~/.ssh/id_rsa_mylimavm  # change this to your preferred key
    export bzfs_test_mode=smoke
    export bzfs_test_no_run_quietly=false  # for AI agents; print test output only for failed tests; prevent polluting the context window with token noise
    # export bzfs_test_no_run_quietly=true # for humans; print all test output even during successful tests
    ./test.sh
fi
