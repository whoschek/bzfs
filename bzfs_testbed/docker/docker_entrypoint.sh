#!/usr/bin/env bash
#
# Creates a container user that matches the invoking host user, seeds `~/.ssh`, and grants passwordless `sudo` access to
# `zfs` and `zpool`.
set -euo pipefail

ensure_container_user() {
    local container_user_name="${BZFS_CONTAINER_USER_NAME:?BZFS_CONTAINER_USER_NAME must not be empty}"
    local container_user_uid="${BZFS_CONTAINER_USER_UID:?BZFS_CONTAINER_USER_UID must not be empty}"
    local container_user_gid="${BZFS_CONTAINER_USER_GID:?BZFS_CONTAINER_USER_GID must not be empty}"
    local container_user_home="${BZFS_CONTAINER_USER_HOME:?BZFS_CONTAINER_USER_HOME must not be empty}"

    if ! getent group "$container_user_gid" > /dev/null; then
        groupadd --gid "$container_user_gid" "$container_user_name"
    fi
    if ! id -u "$container_user_name" > /dev/null 2>&1; then
        useradd --uid "$container_user_uid" --gid "$container_user_gid" \
            --home-dir "$container_user_home" --no-create-home --shell /bin/bash "$container_user_name"
    fi

    mkdir -p "$container_user_home/.ssh"
    cp -a /bzfs.ssh/. "$container_user_home/.ssh/"
    mkdir -p "$container_user_home/.ssh/bzfs"
    chmod u=rwx,go= "$container_user_home/.ssh"
    chown -R "$container_user_uid:$container_user_gid" "$container_user_home"
    if [[ -f "$container_user_home/.ssh/authorized_keys" ]]; then
        chmod u=rw,go= "$container_user_home/.ssh/authorized_keys"
    fi

    printf '%s ALL=(root) NOPASSWD: %s, %s\n' "$container_user_name" "$(command -v zfs)" "$(command -v zpool)" \
        > /etc/sudoers.d/bzfs-container-user
    chmod u=r,g=r,o= /etc/sudoers.d/bzfs-container-user
}

ensure_container_user
exec "$@"
