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

if [ -e "${BASEDIR}/oozie-server" ]; then
  export OOZIE_USE_TOMCAT=1
else
  export OOZIE_USE_TOMCAT=0
fi

if [ $# -le 0 ]; then
  if [ "${OOZIE_USE_TOMCAT}" -eq "1" ]; then
    echo "Usage: oozied.sh (start|stop|run) [<catalina-args...>]"
  else
    echo "Usage: oozied.sh (start|stop|run)"
  fi
  exit 1
fi

actionCmd=$1
shift

if [ "${OOZIE_USE_TOMCAT}" == "1" ]; then
  source ${BASEDIR}/bin/oozie-tomcat-server.sh
  tomcat_main $actionCmd
else
  source ${BASEDIR}/bin/oozie-jetty-server.sh
  jetty_main $actionCmd
fi
