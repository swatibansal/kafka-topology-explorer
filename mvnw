#!/bin/sh
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# Minimal Maven Wrapper script.
# Downloads Apache Maven 3.9.6 on first run, then delegates to it.

MAVEN_VERSION="3.9.6"
WRAPPER_DIR="${HOME}/.m2/wrapper/dists/apache-maven-${MAVEN_VERSION}"
MVN_BIN="${WRAPPER_DIR}/bin/mvn"

if [ ! -f "${MVN_BIN}" ]; then
  echo "[mvnw] Apache Maven ${MAVEN_VERSION} not found. Downloading..."
  mkdir -p "${WRAPPER_DIR}"
  DIST_URL="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "${DIST_URL}" | tar -xz -C "${WRAPPER_DIR}" --strip-components=1
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- "${DIST_URL}" | tar -xz -C "${WRAPPER_DIR}" --strip-components=1
  else
    echo "[mvnw] ERROR: neither curl nor wget found. Install one and retry." >&2
    exit 1
  fi
  echo "[mvnw] Maven ${MAVEN_VERSION} installed to ${WRAPPER_DIR}"
fi

# Prefer Java 21+ — use java_home helper on macOS if JAVA_HOME is not already set.
if [ -z "${JAVA_HOME}" ]; then
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null)"
    export JAVA_HOME
  fi
fi

exec "${MVN_BIN}" "$@"
