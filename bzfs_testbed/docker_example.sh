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

DOCKER_CLI="$(command -v nerdctl || command -v docker)"  # Lima includes nerdctl which is compatible with docker CLI in this context
sudo "$DOCKER_CLI" compose -f docker-compose.yml up -d
sleep 1
ssh -p 2222 root@127.0.0.1 'zfs list'
sudo "$DOCKER_CLI" compose -f docker-compose.yml down
printf 'Success!\n'
