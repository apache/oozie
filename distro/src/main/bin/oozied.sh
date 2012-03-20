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

if [ $# -le 0 ]; then
  echo "Usage: oozied.sh (start|stop|run) [<catalina-args...>]"
  exit 1
fi

actionCmd=$1
shift

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

source ${BASEDIR}/bin/oozie-sys.sh

CATALINA=${OOZIE_CATALINA_HOME:-${BASEDIR}/oozie-server}/bin/catalina.sh

setup_catalina_opts() {
  # The Java System property 'oozie.http.port' it is not used by Oozie,
  # it is used in Tomcat's server.xml configuration file
  #
  echo "Using   CATALINA_OPTS:       ${CATALINA_OPTS}"

  catalina_opts="-Doozie.home.dir=${OOZIE_HOME}";
  catalina_opts="${catalina_opts} -Doozie.config.dir=${OOZIE_CONFIG}";
  catalina_opts="${catalina_opts} -Doozie.log.dir=${OOZIE_LOG}";
  catalina_opts="${catalina_opts} -Doozie.data.dir=${OOZIE_DATA}";

  catalina_opts="${catalina_opts} -Doozie.config.file=${OOZIE_CONFIG_FILE}";

  catalina_opts="${catalina_opts} -Doozie.log4j.file=${OOZIE_LOG4J_FILE}";
  catalina_opts="${catalina_opts} -Doozie.log4j.reload=${OOZIE_LOG4J_RELOAD}";

  catalina_opts="${catalina_opts} -Doozie.http.hostname=${OOZIE_HTTP_HOSTNAME}";
  catalina_opts="${catalina_opts} -Doozie.admin.port=${OOZIE_ADMIN_PORT}";
  catalina_opts="${catalina_opts} -Doozie.http.port=${OOZIE_HTTP_PORT}";
  catalina_opts="${catalina_opts} -Doozie.base.url=${OOZIE_BASE_URL}";

  echo "Adding to CATALINA_OPTS:     ${catalina_opts}"

  export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts}"
}

setup_oozie() {
  if [ ! -e "${CATALINA_BASE}/webapps/oozie.war" ]; then
    echo "WARN: Oozie WAR has not been set up at ''${CATALINA_BASE}/webapps'', doing default set up"
    ${BASEDIR}/bin/oozie-setup.sh
    if [ "$?" != "0" ]; then
      exit -1
    fi
  fi
  echo
}

case $actionCmd in
  (start|run)
    setup_catalina_opts
    setup_oozie 
    ;;
  (stop)
    setup_catalina_opts

    # A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
    export JAVA_OPTS=${CATALINA_OPTS}
    ;;
esac

exec $CATALINA $actionCmd "$@"
