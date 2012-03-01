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

OOZIE_HOME=${BASEDIR}
OOZIE_CONFIG=${OOZIE_HOME}/conf
OOZIE_LOG=${OOZIE_HOME}/logs
OOZIE_DATA=${OOZIE_HOME}/data

if [ -f ${OOZIE_HOME}/bin/oozie-env.sh ]
then
  source ${OOZIE_HOME}/bin/oozie-env.sh
fi

if [ ! -d ${OOZIE_CONFIG} ]
then
  echo
  echo "ERROR: Oozie configuration directory could not be found at ${OOZIE_CONFIG}"
  echo
  exit 1
fi

OOZIEDB_OPTS="-Doozie.home.dir=${OOZIE_HOME}";
OOZIEDB_OPTS="${OOZIEDB_OPTS} -Doozie.config.dir=${OOZIE_CONFIG}";
OOZIEDB_OPTS="${OOZIEDB_OPTS} -Doozie.log.dir=${OOZIE_LOG}";
OOZIEDB_OPTS="${OOZIEDB_OPTS} -Doozie.data.dir=${OOZIE_DATA}";

OOZIECPPATH=""
for i in "${BASEDIR}/libtools/"*.jar; do
  OOZIECPPATH="${OOZIECPPATH}:$i"
done
for i in "${BASEDIR}/libext/"*.jar; do
  OOZIECPPATH="${OOZIECPPATH}:$i"
done


if test -z ${JAVA_HOME}
then
    JAVA_BIN=java
else
    JAVA_BIN=${JAVA_HOME}/bin/java
fi

while [[ ${1} =~ ^\-D ]]; do
  JAVA_PROPERTIES="${JAVA_PROPERTIES} ${1}"
  shift
done

${JAVA_BIN} ${OOZIEDB_OPTS} ${JAVA_PROPERTIES} -cp ${OOZIECPPATH} org.apache.oozie.tools.OozieDBCLI "${@}"
