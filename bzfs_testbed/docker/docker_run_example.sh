#!/usr/bin/env bash
#
# Configures and runs a basic example rootful bzfs docker container.
# Run `up` on *each* of the testbed VMs to start containers everywhere, then run `runjob` once the peers are up.
# Works out of the box on VMs created via ../lima_testbed.sh, except that it assumes that the $BZFS_DOCKER_IMAGE
# is available - for example run `sudo ./docker_image.sh` on each testbed VM to first generate this prerequisite.
#
# If ~/bzfs-config/cron.d exists, each `up` copies its files into the container /etc/cron.d.
# See ./cronjob_example for a documented cron file that periodically runs the same job as `runjob`.
# If you change the image or port env vars or similar, run `down` and then `up` to recreate the container.
set -eo pipefail

usage() {
    prog_name="$(basename "$0")"
    cat << EOF
Usage: ${prog_name} up|down|runjob|shell

Commands:
  up            Create or start the container, then reload cron jobs.
  down          Remove the container if it exists.
  runjob        Run the example job in the container.
  shell         Enter an interactive shell in the container.

EOF
}

if [[ "$#" -ne 1 ]]; then
    usage >&2
    exit 2
fi

BZFS_DOCKER_IMAGE="${BZFS_DOCKER_IMAGE:-v1.19.0-ubuntu-24.04}"
BZFS_DOCKER_INSTALL_HPNSSH="${BZFS_DOCKER_INSTALL_HPNSSH:-false}"
CONTAINER_NAME=bzfs
CONTAINER_USER_NAME="$(id -un)"
CONTAINER_USER_UID="$(id -u)"
CONTAINER_USER_GID="$(id -g)"
CONTAINER_USER_HOME="$HOME"
DOCKER_CLI="${DOCKER_CLI:-$(command -v nerdctl || command -v docker)}"  # Lima includes nerdctl which is compatible w/ docker
DOCKER_CLI="sudo $DOCKER_CLI"

container_exec() {
    $DOCKER_CLI exec --user "${CONTAINER_USER_UID}:${CONTAINER_USER_GID}" "$CONTAINER_NAME" ${1+"$@"}
}

require_running_container() {
    if [[ "$($DOCKER_CLI inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2> /dev/null)" != "true" ]]; then
        echo "ERROR: Container '$CONTAINER_NAME' is not running; use 'up' first" >&2
        exit 1
    fi
}

case "$1" in
    up)
        mkdir -p "$CONTAINER_USER_HOME/bzfs-config/etc" "$CONTAINER_USER_HOME/bzfs-config/cron.d"
        mkdir -p "$CONTAINER_USER_HOME/bzfs-job-logs" "$CONTAINER_USER_HOME/bzfs-logs"
        chmod u=rwx,go= "$CONTAINER_USER_HOME/bzfs-job-logs" "$CONTAINER_USER_HOME/bzfs-logs"

        # configure /etc/ssh/ for the docker container, using /etc/ssh/ as template; will be bind-mounted
        etc_ssh_volume_source="$CONTAINER_USER_HOME/bzfs-config/etc/ssh"
        sudo rm -rf "$etc_ssh_volume_source"
        sudo cp -a /etc/ssh "$etc_ssh_volume_source"
        sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' "$etc_ssh_volume_source/sshd_config"  # remove existing ports

        # configure /etc/hpnssh/ for the docker container, using /etc/ssh/ as template; will be bind-mounted
        etc_hpnssh_volume_source="$CONTAINER_USER_HOME/bzfs-config/etc/hpnssh"
        sudo rm -rf "$etc_hpnssh_volume_source"
        sudo cp -a /etc/ssh "$etc_hpnssh_volume_source"
        sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/d' "$etc_hpnssh_volume_source/sshd_config"  # remove existing ports
        sudo sed -i 's#/etc/ssh#/etc/hpnssh#g' "$etc_hpnssh_volume_source/sshd_config"  # change all occurrences of /etc/ssh to /etc/hpnssh
        sudo sed -i -E 's|^([[:space:]]*)Include([[:space:]]+)|\1# Include\2|' "$etc_hpnssh_volume_source/sshd_config"  # comment out Include directives

        if [[ "$BZFS_DOCKER_INSTALL_HPNSSH" == "true" ]]; then
            printf '%s\n' "Port 2222" | sudo tee -a "$etc_hpnssh_volume_source/sshd_config" > /dev/null  # add port 2222
        else
            printf '%s\n' "Port 2222" | sudo tee -a "$etc_ssh_volume_source/sshd_config" > /dev/null  # add port 2222
        fi

        $DOCKER_CLI image ls  # list all docker images in the local registry

        # run container if it doesn't exist yet
        if ! $DOCKER_CLI container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
            $DOCKER_CLI run -d \
                --name "$CONTAINER_NAME" \
                --env "BZFS_CONTAINER_USER_NAME=$CONTAINER_USER_NAME" \
                --env "BZFS_CONTAINER_USER_UID=$CONTAINER_USER_UID" \
                --env "BZFS_CONTAINER_USER_GID=$CONTAINER_USER_GID" \
                --env "BZFS_CONTAINER_USER_HOME=$CONTAINER_USER_HOME" \
                --privileged \
                --device /dev/zfs:/dev/zfs \
                --publish "2222:2222" \
                --volume "$etc_ssh_volume_source:/etc/ssh:ro" \
                --volume "$etc_hpnssh_volume_source:/etc/hpnssh:ro" \
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
        elif [[ "$($DOCKER_CLI inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2> /dev/null)" != "true" ]]; then
            $DOCKER_CLI start "$CONTAINER_NAME"
            sleep 1
        fi

        # reload managed cron jobs from ~/bzfs-config/cron.d and ensure cron is running
        $DOCKER_CLI exec "$CONTAINER_NAME" bash -lc "
            pkill -x cron || true
            rm -f /etc/cron.d/bzfs-*
            for file in /bzfs-config/cron.d/*; do
                [[ -f \"\$file\" ]] || continue
                install -o root -g root -m u=rw,go=r \"\$file\" \"/etc/cron.d/bzfs-\${file##*/}\"
            done
            /usr/sbin/cron
        "
        ;;
    down)
        if $DOCKER_CLI container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
            $DOCKER_CLI rm -f "$CONTAINER_NAME"
        fi
        ;;
    runjob)
        require_running_container
        container_exec zfs list  # verify
        container_exec /bzfs/bzfs_testbed/bzfs_job_testbed.py \
            --create-src-snapshots --replicate  --prune-src-snapshots --prune-src-bookmarks --prune-dst-snapshots \
            --ssh-src-port=2222 \
            --ssh-dst-port=2222

        # container_exec zfs list -t snapshot  # verify
        ;;
    shell)
        require_running_container
        $DOCKER_CLI exec -it \
            --user "${CONTAINER_USER_UID}:${CONTAINER_USER_GID}" \
            --workdir "$CONTAINER_USER_HOME" \
            "$CONTAINER_NAME" \
            bash -l
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
