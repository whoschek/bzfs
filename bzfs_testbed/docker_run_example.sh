#!/usr/bin/env bash
#
# Configures and runs a basic example rootful container via docker compose.
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

DOCKER_CLI="$(command -v nerdctl || command -v docker)"  # Lima includes nerdctl which is compatible with docker CLI in this context
sudo "$DOCKER_CLI" compose -f docker-compose.yml up -d
sleep 1

ssh -p 2222 root@127.0.0.1 'zfs list'  # verify
sudo "$DOCKER_CLI" compose -f docker-compose.yml exec bzfs \
    bzfs dst/foo/bar dst/foo/baz \
       --recursive \
       --create-src-snapshots \
       --create-src-snapshots-plan="{'prod':{'us-west':{'minutely':40,'hourly':36,'daily':31}}}"
ssh -p 2222 root@127.0.0.1 'zfs list -t snapshot'  # verify

sudo "$DOCKER_CLI" compose -f docker-compose.yml down
printf 'Success!\n'
