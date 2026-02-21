#!/bin/sh
# Maven Wrapper Script
# This script downloads and runs Maven if not installed

MAVEN_VERSION=3.9.9
MAVEN_HOME="${HOME}/.m2/wrapper/maven-${MAVEN_VERSION}"
MAVEN_URL="https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz"

if [ ! -d "${MAVEN_HOME}" ]; then
    echo "Downloading Maven ${MAVEN_VERSION}..."
    mkdir -p "${HOME}/.m2/wrapper"
    cd "${HOME}/.m2/wrapper"
    curl -sL "${MAVEN_URL}" | tar xz
    cd - > /dev/null
fi

export MAVEN_HOME
export PATH="${MAVEN_HOME}/bin:${PATH}"

exec "${MAVEN_HOME}/bin/mvn" "$@"
