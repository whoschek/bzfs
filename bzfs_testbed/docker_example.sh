#!/usr/bin/env bash

set -e
sudo rm -fr /etc/hpnssh
sudo rsync -a /etc/ssh/ /etc/hpnssh
sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' /etc/hpnssh/sshd_config  # remove existing ports
printf '%s\n' "Port 2222" | sudo tee -a /etc/hpnssh/sshd_config > /dev/null  # add port 2222
sudo sed -i 's#/etc/ssh#/etc/hpnssh#g' /etc/hpnssh/sshd_config  # change all occurrences of /etc/ssh to /etc/hpnssh
sudo sed -i -E 's|^([[:space:]]*)Include([[:space:]]+)|\1# Include\2|' /etc/hpnssh/sshd_config  # comment out Include directives
sudo install -d -m 700 /tmp/mybzfs-root-ssh
cat ~/.ssh/id_rsa.pub | sudo tee /tmp/mybzfs-root-ssh/authorized_keys > /dev/null  # authorize the matching key for root
sudo chmod 600 /tmp/mybzfs-root-ssh/authorized_keys

export BZFS_GIT_TAG="v1.19.0"  # adjust as desired
export BZFS_DOCKER_OS="ubuntu-24.04"
export BZFS_DOCKER_IMAGE="docker.io/whoschek/bzfs:${BZFS_GIT_TAG}-${BZFS_DOCKER_OS}"

sudo nerdctl rm -f mybzfs > /dev/null 2>&1 || true
sudo nerdctl run -d --name mybzfs \
    --publish=2222:2222 \
    --mount type=bind,src=/etc/ssh,dst=/etc/ssh,readonly \
    --mount type=bind,src=/etc/hpnssh,dst=/etc/hpnssh,readonly \
    --mount type=bind,src=/etc/hostid,dst=/etc/hostid,readonly \
    --volume /tmp/mybzfs-root-ssh:/root/.ssh:ro \
    --device=/dev/zfs:/dev/zfs \
    --privileged \
    "${BZFS_DOCKER_IMAGE}" > /dev/null

sleep 1

ssh -p 2222 root@127.0.0.1 'zfs list'

sudo nerdctl rm -f mybzfs > /dev/null 2>&1 || true

printf 'Success!\n'
