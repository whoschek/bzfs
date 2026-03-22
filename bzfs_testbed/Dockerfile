# Build a bzfs docker image for testbed use, for example via ./docker_create.sh.
#
# Example:
#   export BZFS_GIT_TAG="v1.19.0"  # adjust as desired
#   export BZFS_DOCKER_OS="ubuntu-24.04"
#   export BZFS_DOCKER_IMAGE="docker.io/mydockerhubuser/bzfs:${BZFS_GIT_TAG}-${BZFS_DOCKER_OS}"
#   sudo docker run --rm -it --name mybzfs \
#       --publish=2222:2222 \
#       --volume /etc/ssh:/etc/ssh:ro \
#       --volume /etc/hpnssh:/etc/hpnssh:ro \
#       --volume /etc/hostid:/etc/hostid:ro \
#       --device=/dev/zfs:/dev/zfs \
#       --privileged \
#       "${BZFS_DOCKER_IMAGE}"
#
# Notes:
# - The host must already have ZFS installed and the zfs kernel module loaded.
# - The container auto-starts `sshd` on port 22 and `hpnsshd` on port 2222.
# - For config files, `sshd` uses `/etc/ssh`, while `hpnsshd` uses `/etc/hpnssh`.
# - The bind-mounted `/etc/hpnssh/sshd_config` should typically define `Port 2222`.
# - `--device=/dev/zfs` exposes the host ZFS control device to the container.
# - `--privileged` keeps `zfs` and `zpool` subcommands working inside the container.
# - `/etc/hostid` is bind-mounted so `zfs` and `zpool` work properly inside the container and don't act like the
#   host's pools belong to some other machine.
ARG BZFS_DOCKER_BASE_IMAGE=ubuntu:24.04
FROM ${BZFS_DOCKER_BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ARG BZFS_GIT_REMOTE=https://github.com/whoschek/bzfs.git
ARG BZFS_GIT_TAG

RUN apt-get -y -qq update \
    && apt-get -y full-upgrade \
    && apt-get -y install openssh-server python3 zstd mbuffer pv \
    && apt-get -y install git \
    && apt-get -y install zfsutils-linux \
    && apt-get -y install software-properties-common \
    && add-apt-repository -y ppa:rapier1/hpnssh \
    && apt-get -y -qq update \
    && apt-get -y install hpnssh \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --single-branch --branch "${BZFS_GIT_TAG}" "${BZFS_GIT_REMOTE}" /bzfs \
    && ln -sf /bzfs/bzfs_main/bzfs /usr/local/bin/bzfs \
    && ln -sf /bzfs/bzfs_main/bzfs_jobrunner /usr/local/bin/bzfs_jobrunner

WORKDIR /bzfs

EXPOSE 22 2222

CMD ["/bin/bash", "-lc", "/usr/sbin/sshd -D -e & /usr/sbin/hpnsshd -D -e & wait -n"]
