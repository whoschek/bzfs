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

set -euo pipefail
ZFS_VERSION="$1"  # e.g. 'zfs-2.2' or 'zfs-2.3' or 'zfs-2.4'
SSH_PROGRAM="$2"  # 'ssh' or 'hpnssh'

cat /etc/redhat-release
# sudo dnf -y upgrade --refresh
cat /etc/redhat-release
sudo dnf -y install dnf-plugins-core
# sudo dnf config-manager --set-enabled crb

sudo dnf -y install openssh-clients openssh-server zstd coreutils
sudo systemctl enable --now sshd
if [[ "$SSH_PROGRAM" == "hpnssh" ]]; then  # see https://www.psc.edu/hpn-ssh-home/hpn-readme/
    sudo dnf -y copr enable rapier1/hpnssh  # see https://copr.fedorainfracloud.org/coprs/rapier1/hpnssh/
    sudo dnf -y install hpnssh-clients hpnssh-server
    hpnssh -V  # print version number
    sudo systemctl disable --now hpnsshd.socket || true  # disable hpnsshd service listening on port 22
    sudo systemctl disable --now hpnsshd || true         # disable hpnsshd service listening on port 2222
    sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' /etc/hpnssh/sshd_config  # remove existing ports
    printf '%s\n' "Port 2222" | sudo tee -a /etc/hpnssh/sshd_config > /dev/null  # add port 2222
    sudo pkill -x hpnsshd || true
    hpnsshd_bin="$(command -v hpnsshd)"
    sudo "$hpnsshd_bin" -f /etc/hpnssh/sshd_config
    if ! pgrep -x hpnsshd > /dev/null; then
        echo "ERROR: hpnsshd daemon failed to start." >&2
        exit 1
    fi
fi

sudo dnf -y install epel-release
sudo dnf repoinfo epel
sudo dnf -y install pv --enablerepo=epel
if ! sudo dnf -y install mbuffer --enablerepo=epel; then
    # workaround for the fact that EPEL for Almalinux-10 does not (yet) contain an RPM for `mbuffer`
    # see https://rpmfind.net/linux/rpm2html/search.php?query=mbuffer(x86-64)
    # guardrail: includepkgs=mbuffer prevents accidentally pulling a bunch of other EL9 packages into EL10
    sudo rpm --import https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-9
    sudo dnf -y install mbuffer \
        --repofrompath="epel9,https://dl.fedoraproject.org/pub/epel/9/Everything/$(rpm --eval '%{_arch}')/" \
        --setopt=epel9.gpgcheck=1 \
        --setopt=epel9.includepkgs=mbuffer \
        --enablerepo=epel9
fi

# Install ZFS:
sudo dnf install -y "https://zfsonlinux.org/epel/zfs-release-3-0$(rpm --eval '%{dist}').noarch.rpm"
sudo dnf repolist all | grep -i zfs
sudo dnf config-manager --disable 'zfs*'
sudo dnf install -y "kernel-devel-$(uname -r)" "kernel-headers-$(uname -r)"
if ! sudo dnf install -y zfs --enablerepo="epel,$ZFS_VERSION"; then
    arch="$(rpm --eval '%{_arch}')"
    if [[ "$arch" != "aarch64" ]]; then
        echo "ERROR: Failed to install zfs from repo '$ZFS_VERSION' on arch '$arch'." >&2
        exit 1
    fi
    # make it also work on aarch64, including guest VMs hosted by MacOS on Apple Silicon
    zfs_source_repo="${ZFS_VERSION}-source"
    build_dir="$(mktemp -d)"
    trap 'rm -rf "$build_dir"' EXIT
    echo "Falling back to source build from repo '$zfs_source_repo' on '$arch' ..."
    sudo dnf config-manager --set-enabled crb
    sudo dnf install -y rpm-build
    sudo dnf install -y dkms --enablerepo=epel
    dnf -y download --source --disablerepo='*' --enablerepo="$zfs_source_repo" --destdir "$build_dir" zfs-dkms zfs
    sudo dnf -y builddep "$build_dir"/zfs-dkms-[0-9]*.src.rpm "$build_dir"/zfs-[0-9]*.src.rpm
    rpmbuild --rebuild "$build_dir"/zfs-dkms-[0-9]*.src.rpm "$build_dir"/zfs-[0-9]*.src.rpm
    sudo dnf -y install "$HOME"/rpmbuild/RPMS/noarch/zfs-dkms-*.rpm
    sudo dnf -y install \
        "$HOME"/rpmbuild/RPMS/"$arch"/libnvpair[0-9]-[0-9]*.rpm \
        "$HOME"/rpmbuild/RPMS/"$arch"/libuutil[0-9]-[0-9]*.rpm \
        "$HOME"/rpmbuild/RPMS/"$arch"/libzpool[0-9]-[0-9]*.rpm \
        "$HOME"/rpmbuild/RPMS/"$arch"/libzfs[0-9]-[0-9]*.rpm \
        "$HOME"/rpmbuild/RPMS/"$arch"/zfs-[0-9]*.rpm \
        "$HOME"/rpmbuild/RPMS/noarch/zfs-dracut-*.rpm
fi
sudo modprobe zfs
zfs --version

mkdir -p "$HOME/.ssh"
rm -f "$HOME/.ssh/id_rsa" "$HOME/.ssh/id_rsa.pub"
ssh-keygen -t rsa -f "$HOME/.ssh/id_rsa" -q -N "" # create private key and public key
cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
chmod u=rwx,go= "$HOME/.ssh"
chmod u=rw,go= "$HOME/.ssh/id_rsa" "$HOME/.ssh/authorized_keys"
chmod u=rw,go=r "$HOME/.ssh/id_rsa.pub"
