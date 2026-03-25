#!/usr/bin/env bash
#
# Configures and runs a basic example rootful bzfs docker container.
# Run `up` on *each* of the testbed VMs to start containers everywhere, then run `runjob` once the peers are up.
# Works out of the box on VMs created via ../lima_testbed.sh, except that it assumes that the $BZFS_DOCKER_IMAGE
# is available - for example run `sudo ./docker_image.sh` on each testbed VM to first generate this prerequisite.
#
# If ~/bzfs-config/cron.d exists, container startup copies its files into /etc/cron.d.
# See ./cronjob_example for a documented cron file that periodically runs the same job as `runjob`.
# If you change the image, managed cron files, port env vars or similar, run `down` and then `up` to recreate the
# container.

set -eo pipefail

usage() {
    prog_name="$(basename "$0")"
    cat << EOF
Usage: ${prog_name} up|down|runjob|shell

Commands:
  up            Create or start the container.
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

# If enabled ban an IP for 15 minutes after 10 failed SSH authentications within 5 minutes.
BZFS_FAIL2BAN_ENABLED="${BZFS_FAIL2BAN_ENABLED:-false}"
BZFS_FAIL2BAN_BANTIME="${BZFS_FAIL2BAN_BANTIME:-15m}"
BZFS_FAIL2BAN_MAXRETRY="${BZFS_FAIL2BAN_MAXRETRY:-10}"
BZFS_FAIL2BAN_FINDTIME="${BZFS_FAIL2BAN_FINDTIME:-5m}"
BZFS_FAIL2BAN_IGNOREIP="${BZFS_FAIL2BAN_IGNOREIP:-}"  # Example safe allowlist: '127.0.0.1/8 ::1'

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
        mkdir -p "$CONTAINER_USER_HOME/bzfs-job-logs" "$CONTAINER_USER_HOME/bzfs-logs" "$CONTAINER_USER_HOME/bzfs-var-log"
        chmod u=rwx,go= "$CONTAINER_USER_HOME/bzfs-job-logs" "$CONTAINER_USER_HOME/bzfs-logs" "$CONTAINER_USER_HOME/bzfs-var-log"

        # configure /etc/ssh/ for the docker container, using /etc/ssh/ as template; will be bind-mounted
        etc_ssh_volume_source="$CONTAINER_USER_HOME/bzfs-config/etc/ssh"
        sudo rm -rf "$etc_ssh_volume_source"
        sudo cp -a /etc/ssh "$etc_ssh_volume_source"

        # configure /etc/hpnssh/ for the docker container, using /etc/ssh/ as template; will be bind-mounted
        etc_hpnssh_volume_source="$CONTAINER_USER_HOME/bzfs-config/etc/hpnssh"
        sudo rm -rf "$etc_hpnssh_volume_source"
        sudo cp -a /etc/ssh "$etc_hpnssh_volume_source"
        sudo sed -i 's#/etc/ssh#/etc/hpnssh#g' "$etc_hpnssh_volume_source/sshd_config"  # change all occurrences of /etc/ssh to /etc/hpnssh

        for sshd_config in "$etc_ssh_volume_source/sshd_config" "$etc_hpnssh_volume_source/sshd_config"; do
            sudo sed -i -E '/^[[:space:]]*#?[[:space:]]*Port[[:space:]]+[0-9]+/Id' "$sshd_config"  # remove existing ports
            sudo sed -i -E 's|^([[:space:]]*)Include([[:space:]]+)|\1# Include\2|I' "$sshd_config"  # comment out Include directives
            # comment out any non-comment line that contains a PasswordAuthentication directive unless it says 'no'
            sudo sed -i -E '/^[[:space:]]*#/!{/PasswordAuthentication/I{/^PasswordAuthentication no$/I! s/^/# /;}}' "$sshd_config"
            sudo sed -i '1i PubkeyAuthentication yes' "$sshd_config"  # prepend explicit pubkey auth directive
            sudo sed -i '1i PermitRootLogin no' "$sshd_config"  # prepend strict safe directive
            sudo sed -i '1i KbdInteractiveAuthentication no' "$sshd_config"  # prepend strict safe directive
            sudo sed -i '1i PasswordAuthentication no' "$sshd_config"  # prepend strict safe directive
        done

        if [[ "$BZFS_DOCKER_INSTALL_HPNSSH" == "true" ]]; then
            sudo sed -i '1i Port 2222' "$etc_hpnssh_volume_source/sshd_config"  # prepend port 2222
        else
            sudo sed -i '1i Port 2222' "$etc_ssh_volume_source/sshd_config"  # prepend port 2222
        fi

        $DOCKER_CLI image ls  # list all docker images in the local registry

        # run container if it doesn't exist yet
        if ! $DOCKER_CLI container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
            $DOCKER_CLI run -d \
                --name "$CONTAINER_NAME" \
                --restart unless-stopped \
                --env "BZFS_CONTAINER_USER_NAME=$CONTAINER_USER_NAME" \
                --env "BZFS_CONTAINER_USER_UID=$CONTAINER_USER_UID" \
                --env "BZFS_CONTAINER_USER_GID=$CONTAINER_USER_GID" \
                --env "BZFS_CONTAINER_USER_HOME=$CONTAINER_USER_HOME" \
                --env "BZFS_DOCKER_INSTALL_HPNSSH=$BZFS_DOCKER_INSTALL_HPNSSH" \
                --env "BZFS_FAIL2BAN_ENABLED=$BZFS_FAIL2BAN_ENABLED" \
                --env "BZFS_FAIL2BAN_BANTIME=$BZFS_FAIL2BAN_BANTIME" \
                --env "BZFS_FAIL2BAN_MAXRETRY=$BZFS_FAIL2BAN_MAXRETRY" \
                --env "BZFS_FAIL2BAN_FINDTIME=$BZFS_FAIL2BAN_FINDTIME" \
                --env "BZFS_FAIL2BAN_IGNOREIP=$BZFS_FAIL2BAN_IGNOREIP" \
                --env "BZFS_FAIL2BAN_PORT=2222" \
                --privileged \
                --device /dev/zfs:/dev/zfs \
                --publish "2222:2222" \
                --volume "$etc_ssh_volume_source:/etc/ssh:ro" \
                --volume "$etc_hpnssh_volume_source:/etc/hpnssh:ro" \
                --volume "$CONTAINER_USER_HOME/.ssh:/bzfs.ssh:ro" \
                --volume "$CONTAINER_USER_HOME/bzfs-config:/bzfs-config:ro" \
                --volume "$CONTAINER_USER_HOME/bzfs-job-logs:$CONTAINER_USER_HOME/bzfs-job-logs" \
                --volume "$CONTAINER_USER_HOME/bzfs-logs:$CONTAINER_USER_HOME/bzfs-logs" \
                --volume "$CONTAINER_USER_HOME/bzfs-var-log:/var/log" \
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

        ;;
    down)
        if $DOCKER_CLI container inspect "$CONTAINER_NAME" > /dev/null 2>&1; then
            $DOCKER_CLI stop "$CONTAINER_NAME"
            $DOCKER_CLI rm "$CONTAINER_NAME"
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
