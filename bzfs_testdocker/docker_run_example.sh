#!/usr/bin/env bash
#
# Configures and runs a basic example rootful bzfs docker container.
# Run `up` on *each* of the testbed VMs to start containers everywhere, then run `runjob` once the peers are up.
# Works out of the box on VMs created via ../bzfs_testbed/lima_testbed.sh, except that it assumes that the $BZFS_DOCKER_IMAGE
# is available - for example run `sudo ./docker_image.sh` on each testbed VM to first generate this prerequisite.
#
# If ~/bzfs-config/bzfs-cron.d exists, each `up` copies its files into the container /etc/cron.d.
# See ./cronjob_example for a documented cron file that periodically runs the same job as `runjob`.
# If you change the image or port env vars or similar, run `down` and then `up` to recreate the container.
set -eo pipefail

usage() {
    prog_name="$(basename "$0")"
    cat << EOF
Usage: ${prog_name} up|down|runjob

Modes:
  up            Create or start the container, then reload cron jobs.
  down          Remove the container if it exists.
  runjob        Run the example job in the container.

EOF
}

if [[ "$#" -ne 1 ]]; then
    usage >&2
    exit 2
fi

BZFS_DOCKER_IMAGE="${BZFS_DOCKER_IMAGE:-v1.19.0-ubuntu-24.04}"
CONTAINER_SSH_PORT="${CONTAINER_SSH_PORT:-22}" # set this to 2222 if you want to use hpnsshd instead of OpenSSH sshd
CONTAINER_NAME=bzfs
CONTAINER_USER_NAME="$(id -un)"
CONTAINER_USER_UID="$(id -u)"
CONTAINER_USER_GID="$(id -g)"
CONTAINER_USER_HOME="$HOME"
DOCKER_CLI="${DOCKER_CLI:-$(command -v nerdctl || command -v docker)}"  # Lima includes nerdctl which is compatible w/ docker

container_exec() {
    sudo "$DOCKER_CLI" exec --user "${CONTAINER_USER_UID}:${CONTAINER_USER_GID}" "$CONTAINER_NAME" ${1+"$@"}
}

require_running_container() {
    if [[ "$(sudo "$DOCKER_CLI" inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2> /dev/null)" != "true" ]]; then
        echo "ERROR: Container '$CONTAINER_NAME' is not running; use 'up' first" >&2
        exit 1
    fi
}

case "$1" in
    up)
        # configure /etc/hpnssh/ for the docker container, using /etc/ssh/ as template; will be bind-mounted.
        sudo rm -rf /etc/hpnssh
        sudo cp -a /etc/ssh /etc/hpnssh
        sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' /etc/hpnssh/sshd_config  # remove existing ports
        printf '%s\n' "Port 2222" | sudo tee -a /etc/hpnssh/sshd_config > /dev/null  # add port 2222
        sudo sed -i 's#/etc/ssh#/etc/hpnssh#g' /etc/hpnssh/sshd_config  # change all occurrences of /etc/ssh to /etc/hpnssh
        sudo sed -i -E 's|^([[:space:]]*)Include([[:space:]]+)|\1# Include\2|' /etc/hpnssh/sshd_config  # comment out Include directives

        sudo "$DOCKER_CLI" image ls  # list all docker images in the local registry

        # precreate bind-mounted host paths as the invoking user so the container does not implicitly create root-owned dirs
        mkdir -p "$CONTAINER_USER_HOME/bzfs-config" "$CONTAINER_USER_HOME/bzfs-config/bzfs-cron.d"
        mkdir -p "$CONTAINER_USER_HOME/bzfs-job-logs" "$CONTAINER_USER_HOME/bzfs-logs"
        chmod u=rwx,go= "$CONTAINER_USER_HOME/bzfs-job-logs" "$CONTAINER_USER_HOME/bzfs-logs"

        # run container if it doesn't exist yet
        if ! sudo "$DOCKER_CLI" container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
            sudo "$DOCKER_CLI" run -d \
                --name "$CONTAINER_NAME" \
                --env "BZFS_CONTAINER_USER_NAME=$CONTAINER_USER_NAME" \
                --env "BZFS_CONTAINER_USER_UID=$CONTAINER_USER_UID" \
                --env "BZFS_CONTAINER_USER_GID=$CONTAINER_USER_GID" \
                --env "BZFS_CONTAINER_USER_HOME=$CONTAINER_USER_HOME" \
                --privileged \
                --device /dev/zfs:/dev/zfs \
                --publish "2222:$CONTAINER_SSH_PORT" \
                --volume /etc/ssh:/etc/ssh:ro \
                --volume /etc/hpnssh:/etc/hpnssh:ro \
                --volume "$CONTAINER_USER_HOME/.ssh:/bzfs.ssh:ro" \
                --volume "$CONTAINER_USER_HOME/bzfs-config:/bzfs-config:ro" \
                --volume "$CONTAINER_USER_HOME/bzfs-job-logs:$CONTAINER_USER_HOME/bzfs-job-logs" \
                --volume "$CONTAINER_USER_HOME/bzfs-logs:$CONTAINER_USER_HOME/bzfs-logs" \
                --volume /etc/hostid:/etc/hostid:ro \
                --volume /etc/hostname:/etc/hostname:ro \
                --uts=host \
                --add-host "$(hostname):127.0.0.1" \
                "$BZFS_DOCKER_IMAGE"
            sleep 1
        # start container if it isn't running yet aka if it has been stopped
        elif [[ "$(sudo "$DOCKER_CLI" inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2> /dev/null)" != "true" ]]; then
            sudo "$DOCKER_CLI" start "$CONTAINER_NAME"
            sleep 1
        fi

        # reload managed cron jobs from ~/bzfs-config/bzfs-cron.d and ensure cron is running
        sudo "$DOCKER_CLI" exec "$CONTAINER_NAME" bash -lc "
            pkill -x cron || true
            rm -f /etc/cron.d/bzfs-*
            for file in /bzfs-config/bzfs-cron.d/*; do
                [[ -f \"\$file\" ]] || continue
                install -o root -g root -m 644 \"\$file\" \"/etc/cron.d/bzfs-\${file##*/}\"
            done
            /usr/sbin/cron
        "
        ;;
    down)
        if sudo "$DOCKER_CLI" container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
            sudo "$DOCKER_CLI" rm -f "$CONTAINER_NAME"
        fi
        ;;
    runjob)
        require_running_container

        container_exec zfs list  # verify

        container_exec /bzfs/bzfs_testbed/bzfs_job_testbed.py \
            --create-src-snapshots --replicate  --prune-src-snapshots --prune-src-bookmarks --prune-dst-snapshots \
            --ssh-src-port=2222 \
            --ssh-dst-port=2222

        if false; then
            container_exec bzfs testsrc01:src/foo/bar testdst01:dst/bar/baz \
                --ssh-src-port=2222 \
                --ssh-dst-port=2222 \
                --recursive \
                --create-src-snapshots \
                --create-src-snapshots-plan="{'prod':{'us-west':{'minutely':40,'hourly':36,'daily':31}}}"
        fi

        container_exec zfs list -t snapshot  # verify
        ;;
    -h | --help)
        usage
        exit 0
        ;;
    *)
        echo "ERROR: Unknown argument: $1" >&2
        usage >&2
        exit 2
        ;;
esac

printf 'Success!\n'
