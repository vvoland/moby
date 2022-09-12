#!/usr/bin/env zsh
set -xe

DOCKER_LINKMODE=dynamic docker buildx bake

killall "Docker" || true
killall "Docker Desktop" || true

DESKTOP_PATH="/Applications/Docker.app"
SOURCE_PATH=$(pwd)
TAR_TMP=$(mktemp -d)

cp "${DESKTOP_PATH}/Contents/Resources/linuxkit/services.tar" "${TAR_TMP}"
tar -C "${TAR_TMP}" -xf "${TAR_TMP}/services.tar"
rm "${TAR_TMP}/services.tar"
cp "${SOURCE_PATH}/bundles/binary-daemon/dockerd" "${TAR_TMP}/containers/services/docker/lower/usr/bin/dockerd-c8d"
tar -C "${TAR_TMP}" -cf "${TAR_TMP}/services.tar" containers

# Make sure that Docker Desktop is not running
sleep 10
cp "${TAR_TMP}/services.tar" "${DESKTOP_PATH}/Contents/Resources/linuxkit/services.tar"
open /Applications/Docker.app
