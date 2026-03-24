# Build a bzfs docker image for testbed use.
# The docker image is a userland artefact; it does not contain a Linux kernel or kernel module.
ARG BZFS_DOCKER_BASE_IMAGE=ubuntu:24.04
FROM ${BZFS_DOCKER_BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ARG BZFS_GIT_REMOTE=https://github.com/whoschek/bzfs.git
ARG BZFS_GIT_TAG

COPY docker_entrypoint.sh /usr/local/bin/bzfs-container-entrypoint

RUN apt-get -y -qq update \
    && apt-get -y full-upgrade \
    && apt-get -y install openssh-server cron python3 zstd mbuffer pv \
    && apt-get -y install git \
    && apt-get -y install zfsutils-linux \
    && apt-get -y install software-properties-common \
    && add-apt-repository -y ppa:rapier1/hpnssh \
    && apt-get -y -qq update \
    && apt-get -y install hpnssh \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --single-branch --branch "${BZFS_GIT_TAG}" "${BZFS_GIT_REMOTE}" /bzfs \
    && ln -sf /bzfs/bzfs_main/bzfs /usr/local/bin/bzfs \
    && ln -sf /bzfs/bzfs_main/bzfs_jobrunner /usr/local/bin/bzfs_jobrunner \
    && chmod u=rwx,go=rx /usr/local/bin/bzfs-container-entrypoint

WORKDIR /bzfs

EXPOSE 22 2222

ENTRYPOINT ["/usr/local/bin/bzfs-container-entrypoint"]
CMD ["/bin/bash", "-lc", "mkdir -p /run/sshd && /usr/sbin/sshd -D -e & /usr/sbin/hpnsshd -D -e & wait -n"]
