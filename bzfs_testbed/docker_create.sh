#!/usr/bin/env bash
#
# Creates a minimal Docker container for bzfs. Assumes Docker and Git are
# installed on the host. Uses a tiny shell script so the package list, tag
# resolution, and recreate flow stay easy to audit and maintain.

set -euo pipefail
BZFS_GIT_REMOTE="${BZFS_GIT_REMOTE:-https://github.com/whoschek/bzfs.git}"
BZFS_DOCKER_OS="${BZFS_DOCKER_OS:-ubuntu-24.04}"
BZFS_DOCKER_PUSH="${BZFS_DOCKER_PUSH:-false}"
BZFS_DOCKER_REGISTRY_PREFIX="${BZFS_DOCKER_REGISTRY_PREFIX:-}"  # e.g. 'docker.io/mydockerhubuser'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BZFS_DOCKERFILE="${SCRIPT_DIR}/Dockerfile.$BZFS_DOCKER_OS"
BZFS_DOCKER_IMAGE="bzfs:$BZFS_DOCKER_OS"
BZFS_DOCKER_CONTAINER="bzfs-$BZFS_DOCKER_OS"

latest_stable_bzfs_tag() {
    git ls-remote --refs --tags "$BZFS_GIT_REMOTE" "refs/tags/v*" |
          awk '{print $2}' |
          sed 's#^refs/tags/##' |
          grep -E '^v[0-9]+([.][0-9]+)*$' |
          LC_ALL=C sort -V |
          tail -n 1
}

BZFS_GIT_TAG="${BZFS_GIT_TAG:-$(latest_stable_bzfs_tag)}"
if [[ -z "$BZFS_GIT_TAG" ]]; then
    echo "ERROR: Failed to determine a stable bzfs git tag from $BZFS_GIT_REMOTE" >&2
    exit 1
fi
if [[ ! -f "$BZFS_DOCKERFILE" ]]; then
    echo "ERROR: Dockerfile not found: $BZFS_DOCKERFILE" >&2
    exit 1
fi
if [[ "$BZFS_DOCKER_PUSH" == "true" && -z "$BZFS_DOCKER_REGISTRY_PREFIX" ]]; then
    echo "ERROR: BZFS_DOCKER_REGISTRY_PREFIX is required when BZFS_DOCKER_PUSH=true" >&2
    exit 1
fi
BZFS_DOCKER_PUSH_IMAGE="${BZFS_DOCKER_REGISTRY_PREFIX}/bzfs:${BZFS_DOCKER_OS}-${BZFS_GIT_TAG}"

if docker container inspect "$BZFS_DOCKER_CONTAINER" > /dev/null 2>&1; then
    echo "Removing existing $BZFS_DOCKER_CONTAINER ..."
    docker rm -f "$BZFS_DOCKER_CONTAINER"
fi

docker build \
    --build-arg "BZFS_GIT_REMOTE=$BZFS_GIT_REMOTE" \
    --build-arg "BZFS_GIT_TAG=$BZFS_GIT_TAG" \
    --tag "$BZFS_DOCKER_IMAGE" \
    --file "$BZFS_DOCKERFILE" \
    "$SCRIPT_DIR"

if [[ "$BZFS_DOCKER_PUSH" == "true" ]]; then
    docker tag "$BZFS_DOCKER_IMAGE" "$BZFS_DOCKER_PUSH_IMAGE"
    docker push "$BZFS_DOCKER_PUSH_IMAGE"
fi

docker run -d --name "$BZFS_DOCKER_CONTAINER" --hostname "$BZFS_DOCKER_CONTAINER" "$BZFS_DOCKER_IMAGE"

printf 'Created container %s from %s at tag %s\n' "$BZFS_DOCKER_CONTAINER" "$BZFS_GIT_REMOTE" "$BZFS_GIT_TAG"
if [[ "$BZFS_DOCKER_PUSH" == "true" ]]; then
    printf 'Pushed image %s\n' "$BZFS_DOCKER_PUSH_IMAGE"
fi
