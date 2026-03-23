#!/usr/bin/env bash
#
# Copyright 2024 Wolfgang Hoschek AT mac DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script builds minimal docker images with a checkout of the latest stable `v*` git tag of `bzfs`.
# If you want to publish the images to a registry such as Docker Hub after `docker login`, set
# `BZFS_DOCKER_PUSH=true` and `BZFS_DOCKER_REGISTRY_PREFIX=...`.
# The pushed image references are `${BZFS_DOCKER_REGISTRY_PREFIX}/bzfs:${BZFS_GIT_TAG}-${BZFS_DOCKER_OS}`,
# for example `docker.io/mydockerhubuser/bzfs:v1.19.0-ubuntu-24.04`.
# Published images include both `linux/amd64` and `linux/arm64`.
set -eo pipefail
script_dir="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
BZFS_GIT_REMOTE="${BZFS_GIT_REMOTE:-https://github.com/whoschek/bzfs.git}"
BZFS_DOCKERFILE="${BZFS_DOCKERFILE:-${script_dir}/Dockerfile}"
BZFS_DOCKER_OS_LIST="${BZFS_DOCKER_OS_LIST:-ubuntu-24.04}"
BZFS_DOCKER_BASE_IMAGES="${BZFS_DOCKER_BASE_IMAGES:-ubuntu:24.04}"
BZFS_DOCKER_PUSH="${BZFS_DOCKER_PUSH:-false}"
BZFS_DOCKER_REGISTRY_PREFIX="${BZFS_DOCKER_REGISTRY_PREFIX:-}"  # e.g. 'docker.io/mydockerhubuser'
BZFS_DOCKER_PLATFORMS="${BZFS_DOCKER_PLATFORMS:-linux/amd64,linux/arm64}"

fetch_latest_stable_bzfs_tag() {
    git ls-remote --refs --tags --sort='version:refname' "$BZFS_GIT_REMOTE" "refs/tags/v*" |
          sed 's#^[^[:space:]]*[[:space:]]refs/tags/##' |
          grep -E '^v[0-9]+([.][0-9]+)*$' |
          tail -n 1
}

host_platform() {
    case "$(docker version --format '{{.Server.Arch}}')" in
        amd64 | x86_64) echo "linux/amd64" ;;
        arm64 | aarch64) echo "linux/arm64" ;;
        *)
            echo "ERROR: Unsupported docker server arch" >&2
            exit 1
            ;;
    esac
}

docker_buildx_build() {
    docker buildx build \
        --build-arg "BZFS_DOCKER_BASE_IMAGE=$bzfs_docker_base_image" \
        --build-arg "BZFS_GIT_REMOTE=$BZFS_GIT_REMOTE" \
        --build-arg "BZFS_GIT_TAG=$BZFS_GIT_TAG" \
        --file "$BZFS_DOCKERFILE" \
        "$@"
}

latest_stable_bzfs_tag="$(fetch_latest_stable_bzfs_tag)"
BZFS_GIT_TAG="${BZFS_GIT_TAG:-$latest_stable_bzfs_tag}"
if [[ -z "$latest_stable_bzfs_tag" ]]; then
    echo "ERROR: Failed to determine a stable bzfs git tag from $BZFS_GIT_REMOTE" >&2
    exit 1
fi
if [[ ! -f "$BZFS_DOCKERFILE" ]]; then
    echo "ERROR: Dockerfile not found: $BZFS_DOCKERFILE" >&2
    exit 1
fi
if [[ "$BZFS_DOCKER_PUSH" == "true" && -z "$BZFS_DOCKER_REGISTRY_PREFIX" ]]; then
    echo "ERROR: BZFS_DOCKER_REGISTRY_PREFIX must not be empty with BZFS_DOCKER_PUSH=true" >&2
    exit 1
fi

IFS=',' read -r -a bzfs_docker_os_list <<< "$BZFS_DOCKER_OS_LIST"
IFS=',' read -r -a bzfs_docker_base_images <<< "$BZFS_DOCKER_BASE_IMAGES"
if [[ ${#bzfs_docker_os_list[@]} -ne ${#bzfs_docker_base_images[@]} ]]; then
    echo "ERROR: BZFS_DOCKER_OS_LIST and BZFS_DOCKER_BASE_IMAGES must contain the same number of items" >&2
    exit 1
fi

for i in "${!bzfs_docker_os_list[@]}"; do
    bzfs_docker_os="${bzfs_docker_os_list[$i]}"
    bzfs_docker_base_image="${bzfs_docker_base_images[$i]}"
    if [[ -z "$bzfs_docker_os" || -z "$bzfs_docker_base_image" ]]; then
        echo "ERROR: BZFS_DOCKER_OS_LIST and BZFS_DOCKER_BASE_IMAGES must not contain empty items" >&2
        exit 1
    fi

    bzfs_docker_tag="${BZFS_GIT_TAG}-${bzfs_docker_os}"
    bzfs_docker_container="bzfs-$bzfs_docker_tag"
    if docker container inspect "$bzfs_docker_container" > /dev/null 2>&1; then
        echo "Removing existing $bzfs_docker_container ..."
        docker rm -f "$bzfs_docker_container"
    fi

    docker_buildx_build \
        --load \
        --platform "$(host_platform)" \
        --tag "$bzfs_docker_tag" \
        "$script_dir"
    printf 'Built image %s from %s at tag %s\n' "$bzfs_docker_tag" "$BZFS_GIT_REMOTE" "$BZFS_GIT_TAG"

    # sanity check
    docker run --rm "$bzfs_docker_tag" bash -lc '
        set -euo pipefail
        command -v bzfs > /dev/null
        command -v bzfs_jobrunner > /dev/null
        bzfs --help > /dev/null
        bzfs_jobrunner --help > /dev/null
    '
    printf 'docker run sanity check passed for %s\n' "$bzfs_docker_tag"

    if [[ "$BZFS_DOCKER_PUSH" == "true" ]]; then
        bzfs_docker_registry_repo="${BZFS_DOCKER_REGISTRY_PREFIX}/${BZFS_DOCKER_IMAGE:-bzfs}"
        bzfs_docker_registry_tags=(--tag "${bzfs_docker_registry_repo}:${bzfs_docker_tag}")
        if [[ "$BZFS_GIT_TAG" == "$latest_stable_bzfs_tag" ]]; then
            bzfs_docker_registry_tags+=(--tag "${bzfs_docker_registry_repo}:latest-${bzfs_docker_os}")
        fi
        docker_buildx_build \
            --platform "$BZFS_DOCKER_PLATFORMS" \
            "${bzfs_docker_registry_tags[@]}" \
            --push \
            "$script_dir"
        printf 'Pushed image to registry %s\n' "${bzfs_docker_registry_tags[*]}"
    fi
done

printf 'Success!\n'
