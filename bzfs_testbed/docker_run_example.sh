#!/usr/bin/env bash
#
# Configures and runs a basic example rootful bzfs docker container.
set -eo pipefail

# configure ~/.ssh/ for the docker container (will be bind-mounted)
sudo rm -rf /bzfs.ssh
sudo mkdir -p /bzfs.ssh
sudo rsync -a ~/.ssh/ /bzfs.ssh
sudo chown -R root /bzfs.ssh
cat ~/.ssh/id_rsa.pub | sudo tee /bzfs.ssh/authorized_keys > /dev/null
sudo chmod u=rw,go= /bzfs.ssh/authorized_keys

# configure /etc/hpnssh/ for the docker container (will be bind-mounted)
sudo rm -rf /etc/hpnssh
sudo rsync -a /etc/ssh/ /etc/hpnssh
sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' /etc/hpnssh/sshd_config  # remove existing ports
printf '%s\n' "Port 2222" | sudo tee -a /etc/hpnssh/sshd_config > /dev/null  # add port 2222
sudo sed -i 's#/etc/ssh#/etc/hpnssh#g' /etc/hpnssh/sshd_config  # change all occurrences of /etc/ssh to /etc/hpnssh
sudo sed -i -E 's|^([[:space:]]*)Include([[:space:]]+)|\1# Include\2|' /etc/hpnssh/sshd_config  # comment out Include directives

BZFS_DOCKER_IMAGE="${BZFS_DOCKER_IMAGE:-docker.io/whoschek/bzfs:v1.19.0-ubuntu-24.04}"
CONTAINER_NAME="${CONTAINER_NAME:-bzfs}"
DOCKER_CLI="${DOCKER_CLI:-$(command -v nerdctl || command -v docker)}"  # Lima includes nerdctl which is compatible w/ docker

sudo "$DOCKER_CLI" rm -f "$CONTAINER_NAME" > /dev/null 2>&1 || true
sudo "$DOCKER_CLI" run -d \
    --name "$CONTAINER_NAME" \
    --privileged \
    --restart unless-stopped \
    --device /dev/zfs:/dev/zfs \
    --publish 2222:2222 \
    --volume /etc/ssh:/etc/ssh:ro \
    --volume /etc/hpnssh:/etc/hpnssh:ro \
    --volume /bzfs.ssh:/root/.ssh \
    --volume /etc/hostid:/etc/hostid:ro \
    --volume /etc/hostname:/etc/hostname:ro \
    --uts=host \
    "$BZFS_DOCKER_IMAGE"
sleep 1

ssh -p 2222 root@127.0.0.1 'zfs list'  # verify
sudo "$DOCKER_CLI" exec "$CONTAINER_NAME" \
    bzfs dst/foo/bar dst/foo/baz \
       --recursive \
       --create-src-snapshots \
       --create-src-snapshots-plan="{'prod':{'us-west':{'minutely':40,'hourly':36,'daily':31}}}"
ssh -p 2222 root@127.0.0.1 'zfs list -t snapshot'  # verify

sudo "$DOCKER_CLI" rm -f "$CONTAINER_NAME"
printf 'Success!\n'
