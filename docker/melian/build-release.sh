#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Builds the FE, BE and meta-service of one commit and publishes them as
#   <registry>:fe-4.1.melian-<suffix>, be-4.1.melian-<suffix>, ms-4.1.melian-<suffix>
#
# Usage: docker/melian/build-release.sh <suffix> [--jobs N] [--no-push]
#
# Run it from the root of a checkout whose tracked files are exactly the commit to release.
# Each image is the upstream 4.1.4 image with the components this fork changes replaced:
# doris-fe.jar and fe-common jar for the FE, doris_be for the BE, doris_cloud for the
# meta-service. Every image carries the commit in the org.opencontainers.image.revision label
# and in /opt/apache-doris/<component>/melian-release.txt.

set -euo pipefail

REGISTRY="${MELIAN_DORIS_REGISTRY:-205498861750.dkr.ecr.us-east-1.amazonaws.com/apache/doris}"
REGION="${AWS_REGION:-us-east-1}"
FE_BASE="apache/doris:fe-4.1.4@sha256:3cbfc9b6293057a0b7f10c2e597f20f4a6bafed0729dfa9795db21eae2e136bb"
BE_BASE="apache/doris:be-4.1.4@sha256:08c139f3d0ec45090a5214fd76f19f84b871d05f1a2db83b5e35a85c193617b2"
MS_BASE="apache/doris:ms-4.1.4@sha256:b3c6d8db1b7d3f2bf361a49d8c89c4bbfb987a23ac20f38695a936c666fd5e25"
FE_COMMON_JAR="fe-common-1.2-SNAPSHOT.jar"

die() {
    echo "build-release: $*" >&2
    exit 1
}

SUFFIX="${1:-}"
[[ "${SUFFIX}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "usage: $0 <suffix> [--jobs N] [--no-push]"
shift
JOBS="$(nproc)"
PUSH=1
while [[ $# -gt 0 ]]; do
    case "$1" in
    --jobs)
        JOBS="$2"
        shift 2
        ;;
    --no-push)
        PUSH=0
        shift
        ;;
    *) die "unknown argument: $1" ;;
    esac
done

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"
[[ -z "$(git status --porcelain --untracked-files=no)" ]] || die "tracked files differ from HEAD; release a clean checkout"
[[ -z "$(git submodule status --recursive | grep -E '^[+U]' || true)" ]] || die "a submodule is not at the commit HEAD records"
SHA="$(git rev-parse HEAD)"
COMMIT_EPOCH="$(git show -s --format=%ct HEAD)"
START_EPOCH="$(date +%s)"
echo "build-release: suffix=${SUFFIX} commit=${SHA}"

declare -A TAGS=(
    [fe]="fe-4.1.melian-${SUFFIX}"
    [be]="be-4.1.melian-${SUFFIX}"
    [ms]="ms-4.1.melian-${SUFFIX}"
)
REPO="${REGISTRY#*/}"
REGISTRY_HOST="${REGISTRY%%/*}"
if [[ "${PUSH}" -eq 1 ]]; then
    # ECR tags are immutable: refuse a suffix any component already uses.
    for component in fe be ms; do
        if aws ecr describe-images --region "${REGION}" --repository-name "${REPO}" \
            --image-ids "imageTag=${TAGS[${component}]}" >/dev/null 2>&1; then
            die "${REGISTRY}:${TAGS[${component}]} already exists"
        fi
    done
fi

if [[ -f custom_env.sh ]]; then
    # shellcheck disable=SC1091
    source custom_env.sh
fi
export DISABLE_BE_JAVA_EXTENSIONS=ON DISABLE_BUILD_AZURE=ON
export CCACHE_BASEDIR="${CCACHE_BASEDIR:-${HOME}}"

# Each build replaces its own directory under output/, so a binary copied below is always the
# one built here; the build-time check guards against a build that failed to replace it.
./build.sh --fe -j "${JOBS}"
./build.sh --be -j "${JOBS}"
./build.sh --cloud -j "${JOBS}"

declare -A BINARIES=(
    [fe]="output/fe/lib/doris-fe.jar output/fe/lib/${FE_COMMON_JAR}"
    [be]="output/be/lib/doris_be"
    [ms]="output/ms/lib/doris_cloud"
)
for component in fe be ms; do
    for binary in ${BINARIES[${component}]}; do
        [[ -f "${binary}" ]] || die "missing ${binary}"
        built="$(stat -c %Y "${binary}")"
        if [[ "${built}" -lt "${START_EPOCH}" || "${built}" -lt "${COMMIT_EPOCH}" ]]; then
            die "${binary} was not built by this run (mtime ${built}, run start ${START_EPOCH})"
        fi
    done
done

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/melian-release-${SUFFIX}.XXXXXX")"
trap 'rm -rf "${STAGE}"' EXIT
release_file() {
    printf 'commit=%s\nsuffix=%s\ncomponent=%s\nbuilt_at=%s\n' "${SHA}" "${SUFFIX}" "$1" \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}

mkdir -p "${STAGE}/fe" "${STAGE}/be" "${STAGE}/ms"
cp output/fe/lib/doris-fe.jar "output/fe/lib/${FE_COMMON_JAR}" "${STAGE}/fe/"
cp output/be/lib/doris_be "${STAGE}/be/"
cp output/ms/lib/doris_cloud "${STAGE}/ms/"
for component in fe be ms; do
    release_file "${component}" >"${STAGE}/${component}/melian-release.txt"
done

cat >"${STAGE}/fe/Dockerfile" <<EOF
FROM ${FE_BASE}
LABEL org.opencontainers.image.revision="${SHA}" melian.release="${SUFFIX}"
COPY doris-fe.jar ${FE_COMMON_JAR} /opt/apache-doris/fe/lib/
COPY melian-release.txt /opt/apache-doris/fe/melian-release.txt
RUN chown root:root /opt/apache-doris/fe/lib/doris-fe.jar /opt/apache-doris/fe/lib/${FE_COMMON_JAR} \\
    && chmod 600 /opt/apache-doris/fe/lib/doris-fe.jar /opt/apache-doris/fe/lib/${FE_COMMON_JAR}
EOF
cat >"${STAGE}/be/Dockerfile" <<EOF
FROM ${BE_BASE}
LABEL org.opencontainers.image.revision="${SHA}" melian.release="${SUFFIX}"
COPY doris_be /opt/apache-doris/be/lib/doris_be
COPY melian-release.txt /opt/apache-doris/be/melian-release.txt
RUN chown root:root /opt/apache-doris/be/lib/doris_be && chmod 700 /opt/apache-doris/be/lib/doris_be
EOF
cat >"${STAGE}/ms/Dockerfile" <<EOF
FROM ${MS_BASE}
LABEL org.opencontainers.image.revision="${SHA}" melian.release="${SUFFIX}"
COPY doris_cloud /opt/apache-doris/ms/lib/doris_cloud
COPY melian-release.txt /opt/apache-doris/ms/melian-release.txt
RUN chown root:root /opt/apache-doris/ms/lib/doris_cloud && chmod 700 /opt/apache-doris/ms/lib/doris_cloud
EOF

DOCKER="${DOCKER:-sudo docker}"
if [[ "${PUSH}" -eq 1 ]]; then
    aws ecr get-login-password --region "${REGION}" |
        ${DOCKER} login --username AWS --password-stdin "${REGISTRY_HOST}" >/dev/null
fi

# The binaries need libjvm from the image's JDK on the library path even for --version. The
# expression is expanded inside the image, so it stays unexpanded here.
# shellcheck disable=SC2016
JVM_PATH='$(dirname "$(find / -name libjvm.so 2>/dev/null | head -1)")'
declare -A SMOKE=(
    [fe]="python3 -c 'import re,zipfile; s=zipfile.ZipFile(\"/opt/apache-doris/fe/lib/doris-fe.jar\").read(\"build/org/apache/doris/common/Version.java\").decode(); print(chr(10).join(re.findall(r\"DORIS_BUILD_(?:VERSION|SHORT_HASH|TIME) = .*\", s)))' && java -version"
    [be]="export LD_LIBRARY_PATH=${JVM_PATH}; /opt/apache-doris/be/lib/doris_be --version"
    [ms]="cd /opt/apache-doris/ms/lib && export LD_LIBRARY_PATH=.:${JVM_PATH}; ./doris_cloud --version"
)
declare -A IMAGE_PATH=(
    [fe]="/opt/apache-doris/fe/lib/doris-fe.jar"
    [be]="/opt/apache-doris/be/lib/doris_be"
    [ms]="/opt/apache-doris/ms/lib/doris_cloud"
)

for component in fe be ms; do
    image="${REGISTRY}:${TAGS[${component}]}"
    ${DOCKER} build -q -t "${image}" "${STAGE}/${component}" >/dev/null
    staged="${STAGE}/${component}/$(basename "${IMAGE_PATH[${component}]}")"
    in_image="$(${DOCKER} run --rm --entrypoint sha256sum "${image}" "${IMAGE_PATH[${component}]}" | cut -d' ' -f1)"
    [[ "${in_image}" == "$(sha256sum "${staged}" | cut -d' ' -f1)" ]] || die "${image}: ${IMAGE_PATH[${component}]} is not the staged binary"
    ${DOCKER} run --rm --entrypoint cat "${image}" "/opt/apache-doris/${component}/melian-release.txt" |
        grep -qx "commit=${SHA}" || die "${image}: release file does not name ${SHA}"
    label="$(${DOCKER} inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "${image}")"
    [[ "${label}" == "${SHA}" ]] || die "${image}: revision label is ${label}"
    echo "build-release: smoke ${image}"
    # Capture the output first: cutting the stream short would fail the check on SIGPIPE.
    smoke_output="$(${DOCKER} run --rm --entrypoint bash "${image}" -c "${SMOKE[${component}]}" 2>&1)" ||
        die "${image}: smoke check failed: ${smoke_output}"
    printf '%s\n' "${smoke_output}" | sed -n '1,8p'
done

for component in fe be ms; do
    image="${REGISTRY}:${TAGS[${component}]}"
    if [[ "${PUSH}" -eq 1 ]]; then
        ${DOCKER} push "${image}" >/dev/null
        digest="$(aws ecr describe-images --region "${REGION}" --repository-name "${REPO}" \
            --image-ids "imageTag=${TAGS[${component}]}" --query 'imageDetails[0].imageDigest' --output text)"
        echo "build-release: pushed ${image} ${digest}"
        ${DOCKER} rmi "${image}" >/dev/null
    else
        echo "build-release: built ${image} (not pushed)"
    fi
done
echo "build-release: done, commit ${SHA}"
