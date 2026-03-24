#!/usr/bin/env bash
#
# Configures and runs a basic example rootful bzfs docker container.
set -eo pipefail

# configure /etc/hpnssh/ for the docker container (will be bind-mounted)
sudo rm -rf /etc/hpnssh
sudo rsync -a /etc/ssh/ /etc/hpnssh
sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' /etc/hpnssh/sshd_config  # remove existing ports
printf '%s\n' "Port 2222" | sudo tee -a /etc/hpnssh/sshd_config > /dev/null  # add port 2222
sudo sed -i 's#/etc/ssh#/etc/hpnssh#g' /etc/hpnssh/sshd_config  # change all occurrences of /etc/ssh to /etc/hpnssh
sudo sed -i -E 's|^([[:space:]]*)Include([[:space:]]+)|\1# Include\2|' /etc/hpnssh/sshd_config  # comment out Include directives

BZFS_DOCKER_IMAGE="${BZFS_DOCKER_IMAGE:-v1.19.0-ubuntu-24.04}"
CONTAINER_NAME="${CONTAINER_NAME:-bzfs}"
DOCKER_CLI="${DOCKER_CLI:-$(command -v nerdctl || command -v docker)}"  # Lima includes nerdctl which is compatible w/ docker
CONTAINER_USER_NAME="$(id -un)"
CONTAINER_USER_UID="$(id -u)"
CONTAINER_USER_GID="$(id -g)"
CONTAINER_USER_HOME="$HOME"

container_exec() {
    sudo "$DOCKER_CLI" exec --user "${CONTAINER_USER_UID}:${CONTAINER_USER_GID}" "$CONTAINER_NAME" "$@"
}

#sudo "$DOCKER_CLI" rm -f "$CONTAINER_NAME"  # delete container

# 'run' container if it doesn't exist yet
if ! sudo "$DOCKER_CLI" container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
    sudo "$DOCKER_CLI" run -d \
        --name "$CONTAINER_NAME" \
        --env "BZFS_CONTAINER_USER_NAME=$CONTAINER_USER_NAME" \
        --env "BZFS_CONTAINER_USER_UID=$CONTAINER_USER_UID" \
        --env "BZFS_CONTAINER_USER_GID=$CONTAINER_USER_GID" \
        --env "BZFS_CONTAINER_USER_HOME=$CONTAINER_USER_HOME" \
        --privileged \
        --device /dev/zfs:/dev/zfs \
        --publish 2222:2222 \
        --volume /etc/ssh:/etc/ssh:ro \
        --volume /etc/hpnssh:/etc/hpnssh:ro \
        --volume "$CONTAINER_USER_HOME/.ssh:/bzfs.ssh:ro" \
        --volume /etc/hostid:/etc/hostid:ro \
        --volume /etc/hostname:/etc/hostname:ro \
        --uts=host \
        --add-host "$(hostname):127.0.0.1" \
        "$BZFS_DOCKER_IMAGE"
    sleep 1
# 'start' container if it isn't running yet aka if it has been stopped
elif [[ "$(sudo "$DOCKER_CLI" inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2> /dev/null)" != "true" ]]; then
    sudo "$DOCKER_CLI" start "$CONTAINER_NAME" > /dev/null
    sleep 1
fi

container_exec zfs list  # verify

container_exec bzfs testsrc01:src/foo/bar testdst01:dst/bar/baz \
    --ssh-src-port=2222 \
    --ssh-dst-port=2222 \
    --recursive \
    --create-src-snapshots \
    --create-src-snapshots-plan="{'prod':{'us-west':{'minutely':40,'hourly':36,'daily':31}}}"

container_exec zfs list -t snapshot  # verify

printf 'Success!\n'
