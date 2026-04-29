# Build a bzfs docker image for testbed use.
# The docker image is a userland artefact; it does not contain a Linux kernel or kernel module.

ARG BZFS_DOCKER_BASE_IMAGE=ubuntu:24.04
FROM ${BZFS_DOCKER_BASE_IMAGE}

ARG BZFS_GIT_REMOTE=https://github.com/whoschek/bzfs.git
ARG BZFS_GIT_TAG  # for example 'v1.19.0'

# For hpnssh see https://launchpad.net/~rapier1/+archive/ubuntu/hpnssh and https://www.psc.edu/hpn-ssh-home/hpn-ssh-faq
ARG BZFS_DOCKER_INSTALL_HPNSSH=true  # hpnssh is optional

COPY docker_entrypoint.sh /usr/local/bin/docker_entrypoint.sh

RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 -y -qq update \
    && apt-get -y dist-upgrade \
    && apt-get -y install openssh-client openssh-server fail2ban \
    && apt-get -y install sudo cron python3 zstd mbuffer pv \
    && apt-get -y install git \
    && apt-get -y install zfsutils-linux \
    && if [ "$BZFS_DOCKER_INSTALL_HPNSSH" = "true" ]; then \
           apt-get -y install software-properties-common; \
           add-apt-repository -y --no-update ppa:rapier1/hpnssh; \
           apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 -y -qq update; \
           apt-get -y install hpnssh-client hpnssh-server; \
       fi \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --single-branch --branch="$BZFS_GIT_TAG" "$BZFS_GIT_REMOTE" /bzfs \
    && ln -s /bzfs/bzfs_main/bzfs /usr/local/bin/bzfs \
    && ln -s /bzfs/bzfs_main/bzfs_jobrunner /usr/local/bin/bzfs_jobrunner \
    && chmod u=rwx,go=rx /usr/local/bin/docker_entrypoint.sh

WORKDIR /bzfs

EXPOSE 2222

ENTRYPOINT ["/usr/local/bin/docker_entrypoint.sh"]
