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
# The script uses Lima to locally create a guest Ubuntu or AlmaLinux VM, then installs ZFS and runs the bzfs test suite
# inside of that VM.
# By default uses ubuntu-24.04 with zfs-2.4 or default ZFS depending on the value of $LIMA_ZFS_VERSION.
# Cold start of the guest VM takes ~30 seconds with defaults; warm start takes ~1.5 seconds.
# To create an AlmaLinux-10 guest VM use: export LIMA_VM_TEMPLATE=template:almalinux-10
# To create an AlmaLinux-9 guest VM use: export LIMA_VM_TEMPLATE=template:almalinux-9

# shellcheck disable=SC2154
set -eo pipefail
LIMA_VM_TEMPLATE="${LIMA_VM_TEMPLATE:-template:ubuntu-24.04}"  # see `limactl create --list-templates` for available options
LIMA_VM_NAME="${LIMA_VM_NAME:-mylimavm}"
LIMA_VM_DISK="${LIMA_VM_DISK:-15}"  # GiB
LIMA_VM_CPUS="${LIMA_VM_CPUS:-0}"  # 0 uses Lima default which currently is min(4, #cores)
LIMA_VM_MEMORY="${LIMA_VM_MEMORY:-4}"  # GiB
LIMA_VM_RECREATE="${LIMA_VM_RECREATE:-false}"  # to init VM from scratch
LIMA_VM_PROTECT="${LIMA_VM_PROTECT:-false}"  # 'true' prohibits accidental deletion of this VM
LIMA_VM_NETWORK="${LIMA_VM_NETWORK:-lima:user-v2}"  # lima:user-v2 network enables VM-to-VM connectivity
LIMA_VM_MOUNT_TYPE="${LIMA_VM_MOUNT_TYPE:-}"  # empty (the default) lets `limactl` pick a good mount type
LIMA_SSH_PORT="${LIMA_SSH_PORT:-0}"  # 0 picks random unused port;
                                     # host box: ssh 127.0.0.1:$LIMA_SSH_PORT --> guest VM
                                     # guest VM: ssh 127.0.0.1:$LIMA_SSH_PORT --> guest VM loopback
LIMA_COPY_BASHRC="${LIMA_COPY_BASHRC:-false}"  # opt-in: copy host ~/.bashrc into guest ~/.bashrc
LIMA_NO_RUN_TESTS="${LIMA_NO_RUN_TESTS:-false}"  # to skip running the bzfs test suite
LIMA_MESH_VMS="${LIMA_MESH_VMS:-}"  # optional `grep -E` regex to select VMs for mutual SSH trust in a mesh network
LIMA_HOST_WORKDIR="$(dirname "$(dirname "$(realpath "${BASH_SOURCE[0]}")")")"  # aka git repo root dir
LIMA_WORKDIR=/bzfs  # this is also the dir where $LIMA_HOST_WORKDIR is mounted within the guest VM
LIMA_WORKDIR_WRITABLE="${LIMA_WORKDIR_WRITABLE:-false}"  # false=read-only, true=read-write shared with host
  # 'false' is for running tests only. 'true' enables repo file changes, e.g. for limited sharing with a sandboxed AI agent.
LIMA_ZFS_VERSION="${LIMA_ZFS_VERSION:-}"  # can be empty (use the default ZFS version) or "zfs-2.4"

# Install Lima if it isn't already installed
if ! command -v limactl > /dev/null 2>&1; then
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
    limactl delete --tty=false --force "$LIMA_VM_NAME"  # by design fails with non-zero exit code if the VM is "protected"
fi

# Create VM if it doesn't already exist
lima_vm_names="$(limactl list --tty=false --format='{{.Name}}')"
if ! grep -Fqx -- "$LIMA_VM_NAME" <<< "$lima_vm_names"; then
    limactl_mount_type_arg=()
    if [[ "$LIMA_VM_MOUNT_TYPE" != "" ]]; then
        limactl_mount_type_arg=(--mount-type="$LIMA_VM_MOUNT_TYPE")
    fi
    limactl create --tty=false \
        --name="$LIMA_VM_NAME" \
        --disk="$LIMA_VM_DISK" \
        --cpus="$LIMA_VM_CPUS" \
        --memory="$LIMA_VM_MEMORY" \
        --network="$LIMA_VM_NETWORK" \
        --set=".ssh.loadDotSSHPubKeys=true" \
        --set=".mounts = ${LIMA_VM_MOUNTS:-[]}" \
        --set=".mounts += [{\"location\":\"$LIMA_HOST_WORKDIR\",\"mountPoint\":\"$LIMA_WORKDIR\",\"writable\":$LIMA_WORKDIR_WRITABLE}]" \
        --containerd="${LIMA_VM_CONTAINERD:-none}" \
        "${limactl_mount_type_arg[@]}" \
        "$LIMA_VM_TEMPLATE"
        # Note: ".ssh.loadDotSSHPubKeys=true" imports ~/.ssh/*.pub from host into the guest VM ~/.ssh/authorized_keys
        # Note: To retain mounts defined in $LIMA_VM_TEMPLATE use export LIMA_VM_MOUNTS='.mounts'
fi

# Optionally, prohibit accidental deletion of this VM
if [[ "$LIMA_VM_PROTECT" == "true" ]]; then
    limactl protect --tty=false "$LIMA_VM_NAME"
    limactl list --tty=false --format='{{.Name}} {{.Protected}}' "$LIMA_VM_NAME"
fi

# Start VM if it isn't already running
if [[ "$(limactl list --tty=false --format='{{.Status}}' "$LIMA_VM_NAME")" != "Running" ]]; then
    # limactl edit --tty=false "$LIMA_VM_NAME" --set=".mounts += [{\"location\":\"$HOME/repos\",\"mountPoint\":\"/repos\",\"writable\":false}]"
    limactl start --tty=false "$LIMA_VM_NAME" --ssh-port="$LIMA_SSH_PORT" --timeout="${LIMA_START_TIMEOUT:-1m}" \
        --progress="${LIMA_START_PROGRESS:-false}" --log-level="${LIMA_START_LOGLEVEL:-info}"
fi
LIMA_SSH_PORT="$(limactl list --tty=false --format="{{.SSHLocalPort}}" "$LIMA_VM_NAME")"
mkdir -p "$LIMA_HOST_WORKDIR/.venv"  # makes `mount` later succeed in guest VM even in read-only mode

# Workaround for https://github.com/lima-vm/lima/issues/678
limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- bash -s << 'EOF'
set -eo pipefail
sudo mkdir -p /etc/cloud/cloud.cfg.d
sudo sh -c 'printf "ssh_deletekeys: false\n" > /etc/cloud/cloud.cfg.d/99-lima-preserve-ssh-hostkeys.cfg'
EOF

# Prepare VM
limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- env \
    LIMA_VM_NAME="$LIMA_VM_NAME" \
    LIMA_SSH_PORT="$LIMA_SSH_PORT" \
    LIMA_ZFS_VERSION="$LIMA_ZFS_VERSION" \
    LIMA_SSH_PROGRAM="${LIMA_SSH_PROGRAM:-ssh}" \
    bash -s << 'EOF'

set -eo pipefail
if [[ -f /etc/redhat-release ]]; then  # RHEL/EL family
    .github-workflow-scripts/install_almalinux_9.sh "${LIMA_ZFS_VERSION:-zfs-2.4}" "$LIMA_SSH_PROGRAM"
    sudo dnf -y install rsync ripgrep
    # sudo dnf -y install pandoc git gh nano mosh curl wget rclone jq tree bash-completion tmux fio net-tools traceroute sysstat ifstat iperf3 iotop iftop
    # sudo dnf -y install npm && sudo npm install -g @openai/codex  # codex --yolo -c model_reasoning_effort=high
elif command -v apt-get > /dev/null 2>&1; then  # Ubuntu
    export DEBIAN_FRONTEND=noninteractive
    if [[ ! -f ~/.bzfs_apt_update_done ]]; then
        echo "Now running 'apt-get update' ..."
        sudo apt-get -y -qq update
        # sudo apt-get -y full-upgrade
        touch ~/.bzfs_apt_update_done
    fi

    if [[ "$LIMA_ZFS_VERSION" == "zfs-2.4" ]]; then  # Ubuntu
        # Upgrade zfs kernel + userland to specific upstream zfs version
        # see https://launchpad.net/~patrickdk/+archive/ubuntu/zfs/+packages
        sudo add-apt-repository ppa:patrickdk/zfs; sudo apt-get -y update
        sudo apt-get -y install zfs-dkms
        # Ensure the just-installed DKMS module is actually the loaded kernel module, and userland has same ZFS version as kernel
        sudo systemctl stop zfs-zed.service || true
        sudo modprobe --remove zfs || true
        sudo modprobe zfs
        sudo systemctl start zfs-zed.service
    elif [[ "$LIMA_ZFS_VERSION" == tag:zfs-* ]]; then  # EXPERIMENTAL e.g. 'tag:zfs-2.4.1', 'tag:zfs-2.3.6', 'tag:zfs-2.2.9'
        sudo apt-get -y install alien autoconf automake build-essential debhelper-compat dh-dkms dh-python dkms fakeroot gawk libaio-dev libattr1-dev libblkid-dev libcurl4-openssl-dev libelf-dev libffi-dev libpam0g-dev libssl-dev libtirpc-dev libtool libudev-dev linux-headers-generic po-debconf python3 python3-all-dev python3-cffi python3-dev python3-packaging python3-setuptools python3-sphinx uuid-dev zlib1g-dev
        sudo apt-get -y install git
        upstream_zfs_git_tag="${LIMA_ZFS_VERSION#tag:}"
        build_dir="$(mktemp -d /var/tmp/zfs-src.XXXXXX)"
        trap 'rm -rf "$build_dir"' EXIT
        git clone --branch "$upstream_zfs_git_tag" --single-branch https://github.com/openzfs/zfs.git "$build_dir/zfs"
        (
            cd "$build_dir/zfs"
            ./autogen.sh
            ./configure
            make native-deb-utils
            rm ../openzfs-zfs-dracut_*.deb  # instead use initramfs
            sudo apt-get -y install --fix-missing ../*.deb
            printf 'zfs\n' | sudo tee /etc/modules-load.d/zfs.conf > /dev/null  # autoload zfs module on reboot
        )
    else  # Ubuntu
        sudo apt-get -y install zfsutils-linux
    fi
    zfs --version

    # Run common preparation steps
    sudo apt-get -y install python3 zstd mbuffer pv rsync ripgrep python3-venv
    # sudo apt-get -y install pandoc git gh nano mosh curl wget rclone jq tree bash-completion tmux fio net-tools traceroute sysstat ifstat iperf3 iotop iftop
    # sudo apt-get -y install npm && sudo npm install -g @openai/codex  # codex --yolo -c model_reasoning_effort=high

    mkdir -p "$HOME/.ssh"
    if [[ ! -f ~/.bzfs_keys_done ]]; then
        rm -f "$HOME/.ssh/id_rsa" "$HOME/.ssh/id_rsa.pub"
        ssh-keygen -t rsa -f "$HOME/.ssh/id_rsa" -q -N ""  # create private key and public key
        cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
        touch ~/.bzfs_keys_done
    fi
else
    echo "ERROR: Unsupported guest OS: expected an Ubuntu or RHEL/EL-family guest." >&2
    exit 1
fi

# Keep default SSH port 22 semantics untouched, and add LIMA_SSH_PORT to socket-activated SSH service
if [[ "$LIMA_SSH_PORT" == "22" ]]; then
    printf 'Port 22\n' | sudo tee /etc/ssh/sshd_config.d/99-bzfs-extra-ports.conf > /dev/null
else
    printf 'Port 22\nPort %s\n' "$LIMA_SSH_PORT" | sudo tee /etc/ssh/sshd_config.d/99-bzfs-extra-ports.conf > /dev/null
    if [[ -f /etc/redhat-release ]]; then
        # workaround for SELinux
        if ! command -v semanage > /dev/null 2>&1; then
            sudo dnf -y install policycoreutils-python-utils
        fi
        sudo semanage port -a -t ssh_port_t -p tcp "$LIMA_SSH_PORT" || \
            sudo semanage port -m -t ssh_port_t -p tcp "$LIMA_SSH_PORT"
    fi
fi
sudo systemctl daemon-reload
if [[ -f /etc/redhat-release ]]; then  # RHEL/EL family
    sudo systemctl restart sshd.service
else  # Ubuntu
    sudo systemctl restart ssh.socket
fi
ssh -n -oBatchMode=yes -oStrictHostKeyChecking=accept-new -oConnectTimeout=5 -p "$LIMA_SSH_PORT" 127.0.0.1 echo hello1  # verify
ssh -n -oBatchMode=yes -oStrictHostKeyChecking=accept-new -oConnectTimeout=5 -p "$LIMA_SSH_PORT" 127.0.0.2 echo hello2  # verify
sudo hostnamectl set-hostname "$LIMA_VM_NAME"  # use hostname without synthetic "lima-" prefix for ease-of-use
ssh -n -oBatchMode=yes -oStrictHostKeyChecking=accept-new -oConnectTimeout=5 "$LIMA_VM_NAME" echo hello3  # verify

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
# Enable guest VM to have its own virtual environment and git hooks
setup_bind_mount "$HOME/.bzfs-lima/.venv" "$(pwd)/.venv"
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

# Optionally, configure a basic mesh network where each VM can talk via ssh to each other VM.
export LIMA_MESH_VMS
mydir="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
"$mydir/lima_mesh.sh"

# Optionally, copy host ~/.bashrc into guest ~/.bashrc
if [[ "$LIMA_COPY_BASHRC" == "true" && -f "$HOME/.bashrc" ]]; then
    limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- bash -lc 'cat > ~/.bashrc' < "$HOME/.bashrc"
fi
echo "LIMA_SSH_PORT: $LIMA_SSH_PORT"

# Optionally, run the bzfs test suite inside of the VM
if [[ "$LIMA_NO_RUN_TESTS" == "false" ]]; then
    limactl shell --tty=false --workdir="$LIMA_WORKDIR" "$LIMA_VM_NAME" -- env \
        bzfs_test_mode="$bzfs_test_mode" \
        bzfs_test_no_run_quietly="$bzfs_test_no_run_quietly" \
        bash -lc './test.sh'
fi

# Alternatively (or subsequently), you can now run tests in a more flexible way by setting env vars for test.sh like so:
if false; then
    export bzfs_test_remote_userhost=127.0.0.1  # 127.0.0.1:$LIMA_SSH_PORT is the guest VM
    export bzfs_test_remote_path=mybzfs
    export bzfs_test_ssh_port="$LIMA_SSH_PORT"  # 127.0.0.1:$LIMA_SSH_PORT is the guest VM
    export bzfs_test_remote_private_key=~/.ssh/id_rsa_mylimavm  # change this to your preferred key
    export bzfs_test_mode=smoke
    export bzfs_test_no_run_quietly=false  # for AI agents; print test output only for failed tests; prevent polluting the context window with token noise
    # export bzfs_test_no_run_quietly=true # for humans; print all test output even during successful tests
    ./test.sh
fi
