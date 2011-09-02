#!/bin/bash
#
# Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
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

if [ ! -e "${BASEDIR}/oozie-server/webapps/oozie.war" ]; then
  echo "INFO: Oozie WAR has not been set up, setting it up"
  ${BASEDIR}/bin/oozie-setup.sh
fi

if [ "${OOZIE_HOME}" = "" ]; then
  export OOZIE_HOME=${BASEDIR}
fi
echo "Using OOZIE_HOME:       ${OOZIE_HOME}"

if [ "${OOZIE_CONFIG_DIR}" = "" ]; then
  OOZIE_CONFIG_DIR=${OOZIE_HOME}/conf
fi
echo "Using OOZIE_CONFIG_DIR: ${OOZIE_CONFIG_DIR}"

if [ -e "${OOZIE_CONFIG_DIR}/oozie-env.sh" ]; then
  source ${OOZIE_CONFIG_DIR}/oozie-env.sh
fi

if [ "${OOZIE_HTTP_PORT}" = "" ]; then
  OOZIE_HTTP_PORT=11000
fi
if [ "${OOZIE_HTTP_HOSTNAME}" = "" ]; then
  OOZIE_HTTP_HOSTNAME=`hostname -f`
fi
if [ "${OOZIE_BASE_URL}" = "" ]; then
  OOZIE_BASE_URL="http://${OOZIE_HTTP_HOSTNAME}:${OOZIE_HTTP_PORT}/oozie"
fi

echo "Using [HOST:PORT]:      [${OOZIE_HTTP_HOSTNAME}:${OOZIE_HTTP_PORT}]"
echo

# The Java System property 'oozie.http.port' it is not used by Oozie,
# it is used in Tomcat's server.xml configuration file
#
export CATALINA_OPTS="${CATALINA_OPTS} -Doozie.http.port=${OOZIE_HTTP_PORT} -Doozie.base.url=${OOZIE_BASE_URL}"

${BASEDIR}/oozie-server/bin/catalina.sh start 

if [ "$?" == "0" ]; then
  echo
  echo "Oozie started"
  echo
fi
