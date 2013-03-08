#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`


function print() {
  if [ "${ECHO}" != "false" ]; then
    echo "$@"
  fi
}

ECHO="true"
if [ "${1}" == "-silent" ]; then
  ECHO="false"
  # if OOZIE_HOME is already set warn it will be ignored
  #
  if [ "${OOZIE_HOME}" != "" ]; then
    print "WARNING: current setting of OOZIE_HOME ignored"
  fi
fi

print

# setting OOZIE_HOME to the installation dir, it cannot be changed
#
export OOZIE_HOME=${BASEDIR}
oozie_home=${OOZIE_HOME}
print "Setting OOZIE_HOME:          ${OOZIE_HOME}"

if [ "${OOZIE_CHECK_OWNER}" = "true" ]; then
  # checking that the user starting/stopping/setting-up Oozie is the owner
  #
  oozie_user=`ls -ld ${OOZIE_HOME} | awk '{print $3}'`
  current_user=`id | awk '{print $1}' | sed 's/.*(//' | sed 's/).*//'`
  if [ "${oozie_user}" != "${current_user}" ]; then
    echo
    echo "ERROR: current user '${current_user}' different from Oozie user '${oozie_user}'"
    echo
    exit -1
  fi
fi

# if the installation has a env file, source it
# this is for native packages installations
#
if [ -e "${OOZIE_HOME}/bin/oozie-env.sh" ]; then
  print "Sourcing:                    ${OOZIE_HOME}/bin/oozie-env.sh"
  source ${OOZIE_HOME}/bin/oozie-env.sh
  grep "^ *export " ${OOZIE_HOME}/bin/oozie-env.sh | sed 's/ *export/  setting/'
fi

# verify that the sourced env file didn't change OOZIE_HOME
# if so, warn and revert
#
if [ "${OOZIE_HOME}" != "${oozie_home}" ]; then
  print "WARN: OOZIE_HOME resetting to ''${OOZIE_HOME}'' ignored"
  export OOZIE_HOME=${oozie_home}
  print "  using OOZIE_HOME:        ${OOZIE_HOME}"
fi

if [ "${OOZIE_CONFIG}" = "" ]; then
  export OOZIE_CONFIG=${OOZIE_HOME}/conf
  print "Setting OOZIE_CONFIG:        ${OOZIE_CONFIG}"
else
  print "Using   OOZIE_CONFIG:        ${OOZIE_CONFIG}"
fi
oozie_config=${OOZIE_CONFIG}

# if the configuration dir has a env file, source it
#
if [ -e "${OOZIE_CONFIG}/oozie-env.sh" ]; then
  print "Sourcing:                    ${OOZIE_CONFIG}/oozie-env.sh"
  source ${OOZIE_CONFIG}/oozie-env.sh
  grep "^ *export " ${OOZIE_CONFIG}/oozie-env.sh | sed 's/ *export/  setting/'
fi

# verify that the sourced env file didn't change OOZIE_HOME
# if so, warn and revert
#
if [ "${OOZIE_HOME}" != "${oozie_home}" ]; then
  print "WARN: OOZIE_HOME resetting to ''${OOZIE_HOME}'' ignored"
  export OOZIE_HOME=${oozie_home}
fi

# verify that the sourced env file didn't change OOZIE_CONFIG
# if so, warn and revert
#
if [ "${OOZIE_CONFIG}" != "${oozie_config}" ]; then
  print "WARN: OOZIE_CONFIG resetting to ''${OOZIE_CONFIG}'' ignored"
  export OOZIE_CONFIG=${oozie_config}
fi

if [ "${OOZIE_CONFIG_FILE}" = "" ]; then
  export OOZIE_CONFIG_FILE="oozie-site.xml"
  print "Setting OOZIE_CONFIG_FILE:   ${OOZIE_CONFIG_FILE}"
else
  print "Using   OOZIE_CONFIG_FILE:   ${OOZIE_CONFIG_FILE}"
fi

if [ "${OOZIE_DATA}" = "" ]; then
  export OOZIE_DATA=${OOZIE_HOME}/data
  print "Setting OOZIE_DATA:          ${OOZIE_DATA}"
else
  print "Using   OOZIE_DATA:          ${OOZIE_DATA}"
fi

if [ "${OOZIE_LOG}" = "" ]; then
  export OOZIE_LOG=${OOZIE_HOME}/logs
  print "Setting OOZIE_LOG:           ${OOZIE_LOG}"
else
  print "Using   OOZIE_LOG:           ${OOZIE_LOG}"
fi

if [ ! -f ${OOZIE_LOG} ]; then
  mkdir -p ${OOZIE_LOG}
fi

if [ "${OOZIE_LOG4J_FILE}" = "" ]; then
  export OOZIE_LOG4J_FILE="oozie-log4j.properties"
  print "Setting OOZIE_LOG4J_FILE:    ${OOZIE_LOG4J_FILE}"
else
  print "Using   OOZIE_LOG4J_FILE:    ${OOZIE_LOG4J_FILE}"
fi

if [ "${OOZIE_LOG4J_RELOAD}" = "" ]; then
  export OOZIE_LOG4J_RELOAD="10"
  print "Setting OOZIE_LOG4J_RELOAD:  ${OOZIE_LOG4J_RELOAD}"
else
  print "Using   OOZIE_LOG4J_RELOAD:  ${OOZIE_LOG4J_RELOAD}"
fi

if [ "${OOZIE_HTTP_HOSTNAME}" = "" ]; then
  export OOZIE_HTTP_HOSTNAME=`hostname -f`
  print "Setting OOZIE_HTTP_HOSTNAME: ${OOZIE_HTTP_HOSTNAME}"
else
  print "Using   OOZIE_HTTP_HOSTNAME: ${OOZIE_HTTP_HOSTNAME}"
fi

if [ "${OOZIE_HTTP_PORT}" = "" ]; then
  export OOZIE_HTTP_PORT=11000
  print "Setting OOZIE_HTTP_PORT:     ${OOZIE_HTTP_PORT}"
else
  print "Using   OOZIE_HTTP_PORT:     ${OOZIE_HTTP_PORT}"
fi

if [ "${OOZIE_ADMIN_PORT}" = "" ]; then
  export OOZIE_ADMIN_PORT=`expr $OOZIE_HTTP_PORT +  1`
  print "Setting OOZIE_ADMIN_PORT:     ${OOZIE_ADMIN_PORT}"
else
  print "Using   OOZIE_ADMIN_PORT:     ${OOZIE_ADMIN_PORT}"
fi

if [ "${OOZIE_HTTPS_PORT}" = "" ]; then
  export OOZIE_HTTPS_PORT=11443
  print "Setting OOZIE_HTTPS_PORT:     ${OOZIE_HTTPS_PORT}"
else
  print "Using   OOZIE_HTTPS_PORT:     ${OOZIE_HTTPS_PORT}"
fi

if [ "${OOZIE_BASE_URL}" = "" ]; then
  export OOZIE_BASE_URL="http://${OOZIE_HTTP_HOSTNAME}:${OOZIE_HTTP_PORT}/oozie"
  print "Setting OOZIE_BASE_URL:      ${OOZIE_BASE_URL}"
else
  print "Using   OOZIE_BASE_URL:      ${OOZIE_BASE_URL}"
fi

if [ "${CATALINA_BASE}" = "" ]; then
  export CATALINA_BASE=${OOZIE_HOME}/oozie-server
  print "Setting CATALINA_BASE:       ${CATALINA_BASE}"
else
  print "Using   CATALINA_BASE:       ${CATALINA_BASE}"
fi

if [ "${OOZIE_HTTPS_KEYSTORE_FILE}" = "" ]; then
  export OOZIE_HTTPS_KEYSTORE_FILE=${HOME}/.keystore
  print "Setting OOZIE_HTTPS_KEYSTORE_FILE:     ${OOZIE_HTTPS_KEYSTORE_FILE}"
else
  print "Using   OOZIE_HTTPS_KEYSTORE_FILE:     ${OOZIE_HTTPS_KEYSTORE_FILE}"
fi

if [ "${OOZIE_HTTPS_KEYSTORE_PASS}" = "" ]; then
  export OOZIE_HTTPS_KEYSTORE_PASS=password
  print "Setting OOZIE_HTTPS_KEYSTORE_PASS:     ${OOZIE_HTTPS_KEYSTORE_PASS}"
else
  print "Using   OOZIE_HTTPS_KEYSTORE_PASS:     ${OOZIE_HTTPS_KEYSTORE_PASS}"
fi

if [ "${CATALINA_OUT}" = "" ]; then
  export CATALINA_OUT=${OOZIE_LOG}/catalina.out
  print "Setting CATALINA_OUT:        ${CATALINA_OUT}"
else
  print "Using   CATALINA_OUT:        ${CATALINA_OUT}"
fi

if [ "${CATALINA_PID}" = "" ]; then
  export CATALINA_PID=${OOZIE_HOME}/oozie-server/temp/oozie.pid
  print "Setting CATALINA_PID:        ${CATALINA_PID}"
else
  print "Using   CATALINA_PID:        ${CATALINA_PID}"
fi

export CATALINA_OPTS="${CATALINA_OPTS} -Dderby.stream.error.file=${OOZIE_LOG}/derby.log"

print
