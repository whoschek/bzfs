#!/usr/bin/env bash
set -euo pipefail

# sudo dnf -y upgrade --refresh
# sudo dnf -y install dnf-plugins-core
# sudo dnf config-manager --set-enabled crb
sudo dnf -y install epel-release
sudo dnf repoinfo epel
sudo dnf -y install pv --enablerepo=epel
sudo dnf -y install mbuffer --enablerepo=epel || {
  # Workaround: EPEL for AlmaLinux 10 may not yet contain an RPM for mbuffer.
  sudo rpm --import https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-9
  sudo dnf -y \
    --repofrompath=epel9,https://dl.fedoraproject.org/pub/epel/9/Everything/$(rpm --eval '%{_arch}')/ \
    --setopt=epel9.gpgcheck=1 \
    --setopt=epel9.includepkgs=mbuffer \  # prevent accidentally pulling a bunch of other EL9 packages into EL10
    --enablerepo=epel9 \
    install mbuffer
}
sudo dnf -y install openssh-clients openssh-server zstd coreutils
sudo systemctl enable --now sshd

# Install ZFS:
sudo dnf install -y https://zfsonlinux.org/epel/zfs-release-3-0$(rpm --eval "%{dist}").noarch.rpm
sudo dnf repolist all | grep -i zfs
sudo dnf config-manager --disable 'zfs*'
# sudo dnf config-manager --enable zfs-2.2-kmod   # or zfs-kmod on some setups
sudo dnf config-manager --enable zfs-2.2
sudo dnf install -y "kernel-devel-$(uname -r)" "kernel-headers-$(uname -r)"
sudo dnf install -y zfs --enablerepo=epel
sudo modprobe zfs
zfs --version

mkdir -p "$HOME/.ssh"
rm -f "$HOME/.ssh/id_rsa" "$HOME/.ssh/id_rsa.pub"
ssh-keygen -t rsa -f "$HOME/.ssh/id_rsa" -q -N "" # create private key and public key
cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
chmod 700 "$HOME/.ssh"
chmod 600 "$HOME/.ssh/id_rsa" "$HOME/.ssh/authorized_keys"
chmod 644 "$HOME/.ssh/id_rsa.pub"
