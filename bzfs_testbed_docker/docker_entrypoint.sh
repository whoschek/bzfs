#!/usr/bin/env bash
#
# Creates a user inside of the container that matches the invoking host user, seeds `~/.ssh`, and grants passwordless `sudo`
# access to `zfs` for that user inside of the container.

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
    chmod u=rwx,go= "$container_user_home/.ssh" "$container_user_home/.ssh/bzfs"
    chown -R "$container_user_uid:$container_user_gid" "$container_user_home"

    printf '%s ALL=(root) NOPASSWD: %s\n' "$container_user_name" "$(command -v zfs)" > /etc/sudoers.d/bzfs-container-user
    chmod u=r,g=r,o= /etc/sudoers.d/bzfs-container-user
}

# Security policy: Ban an IP for <ban_time> minutes after <maxretry> failed SSH authentications within <findtime> seconds.
configure_fail2ban() {
    local fail2ban_enabled="${BZFS_FAIL2BAN_ENABLED:?BZFS_FAIL2BAN_ENABLED must not be empty}"
    local fail2ban_bantime="${BZFS_FAIL2BAN_BANTIME:?BZFS_FAIL2BAN_BANTIME must not be empty}"
    local fail2ban_maxretry="${BZFS_FAIL2BAN_MAXRETRY:?BZFS_FAIL2BAN_MAXRETRY must not be empty}"
    local fail2ban_findtime="${BZFS_FAIL2BAN_FINDTIME:?BZFS_FAIL2BAN_FINDTIME must not be empty}"
    local fail2ban_port="${BZFS_FAIL2BAN_PORT:?BZFS_FAIL2BAN_PORT must not be empty}"
    local fail2ban_ignoreip="${BZFS_FAIL2BAN_IGNOREIP-}"

    if [[ "$fail2ban_enabled" == "false" ]]; then
        return
    fi
    mkdir -p /etc/fail2ban/jail.d /run/fail2ban
    touch /var/log/sshd.log /var/log/hpnsshd.log /var/log/fail2ban.log
    chmod u=rw,g=r,o=r /var/log/sshd.log /var/log/hpnsshd.log /var/log/fail2ban.log
    cat > /etc/fail2ban/jail.d/bzfs-sshd.local << EOF
[DEFAULT]
backend = auto
ignoreip = ${fail2ban_ignoreip}

[sshd]
enabled = true
port = ssh,${fail2ban_port}
logpath = /var/log/sshd.log
          /var/log/hpnsshd.log
bantime = ${fail2ban_bantime}
maxretry = ${fail2ban_maxretry}
findtime = ${fail2ban_findtime}
EOF

    fail2ban-client stop > /dev/null 2>&1 || true
    rm -f /run/fail2ban/fail2ban.sock
    "$(command -v fail2ban-server)" -b --logtarget /var/log/fail2ban.log
    fail2ban-client ping > /dev/null
}

ensure_container_user
configure_fail2ban
exec "$@"
