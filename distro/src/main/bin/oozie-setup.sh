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

function printUsage() {
  echo
  echo " Usage  : oozie-setup.sh <OPTIONS>"
  echo "          [-extjs EXTJS_PATH] (expanded or ZIP, to enable the Oozie webconsole)"
  echo "          [-hadoop HADOOP_VERSION HADOOP_PATH] (Hadoop version [0.20.1|0.20.2|0.20.104|0.20.200]"
  echo "                                                and Hadoop install dir)"
  echo "          [-jars JARS_PATH] (multiple JAR path separated by ':')"
  echo "          (without options does default setup, without the Oozie webconsole)"
  echo
  echo " EXTJS can be downloaded from http://www.extjs.com/learn/Ext_Version_Archives"
  echo
}

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

source ${BASEDIR}/bin/oozie-sys.sh -silent

addExtjs=""
addJars=""
addHadoopJars=""
extjsHome=""
jarsPath=""
hadoopVersion=""
hadoopPath=""
inputWar="${OOZIE_HOME}/oozie.war"
outputWar="${CATALINA_BASE}/webapps/oozie.war"
outputWarExpanded="${CATALINA_BASE}/webapps/oozie"

while [ $# -gt 0 ]
do
  if [ "$1" = "-extjs" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, ExtJS path"
      echo
      printUsage
      exit -1
    fi
    extjsHome=$1
    addExtjs=true
  elif [ "$1" = "-jars" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, JARs path"
      echo
      printUsage
      exit -1
    fi
    jarsPath=$1
    addJars=true
  elif [ "$1" = "-hadoop" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option values, HADOOP_VERSION & HADOOP_HOME_PATH"
      echo
      printUsage
      exit -1
    elif [ $# -eq 1 ]; then
      echo
      echo "Missing option value, HADOOP_HOME_PATH"
      echo
      printUsage
      exit -1
    fi
    hadoopVersion=$1
    shift
    hadoopPath=$1
    addHadoopJars=true
  else
    printUsage
    exit -1
  fi
  shift
done

if [ -e "${CATALINA_PID}" ]; then
  echo
  echo "ERROR: Stop Oozie first"
  echo
  exit -1
fi

if [ -e "${outputWar}" ]; then
  chmod -f u+w ${outputWar}
  rm -rf ${outputWar}
fi
rm -rf ${outputWarExpanded}

echo

if [ "${addExtjs}${addJars}${addHadoopJars}" == "" ]; then
  echo "INFO: Doing default installation, Oozie webconsole disabled"
  cp ${inputWar} ${outputWar}
else
  OPTIONS=""
  if [ "${addExtjs}" != "" ]; then
    OPTIONS="-extjs ${extjsHome}"
  fi
  if [ "${addJars}" != "" ]; then
    OPTIONS="${OPTIONS} -jars ${jarsPath}"
  fi
  if [ "${addHadoopJars}" != "" ]; then
    OPTIONS="${OPTIONS} -hadoop ${hadoopVersion} ${hadoopPath}"
  fi

  ${OOZIE_HOME}/bin/addtowar.sh -inputwar ${inputWar} -outputwar ${outputWar} ${OPTIONS}

  if [ "$?" != "0" ]; then
    exit -1
  fi
fi

echo "INFO: Oozie is ready to be started"

echo

