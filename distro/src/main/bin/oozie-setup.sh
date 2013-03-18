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

function printUsage() {
  echo
  echo " Usage  : oozie-setup.sh <Command and OPTIONS>"
  echo "          prepare-war [-hadoop HADOOP_VERSION HADOOP_PATH] [-extjs EXTJS_PATH] [-jars JARS_PATH] [-secure]"
  echo "                      (prepare-war is to prepare war files for oozie)"
  echo "                      (Hadoop version [0.20.1|0.20.2|0.20.104|0.20.200|0.23.x|2.x] and Hadoop install dir)"
  echo "                      (EXTJS_PATH is expanded or ZIP, to enable the Oozie webconsole)"
  echo "                      (JARS_PATH is multiple JAR path separated by ':')"
  echo "                      (-secure will configure the war file to use HTTPS (SSL))"
  echo "          sharelib create -fs FS_URI [-locallib SHARED_LIBRARY] (create sharelib for oozie,"
  echo "                                                                FS_URI is the fs.default.name"
  echo "                                                                for hdfs uri; SHARED_LIBRARY, path to the"
  echo "                                                                Oozie sharelib to install, it can be a tarball"
  echo "                                                                or an expanded version of it. If ommited,"
  echo "                                                                the Oozie sharelib tarball from the Oozie"
  echo "                                                                installation directory will be used)"
  echo "                                                                (action failes if sharelib is already installed"
  echo "                                                                in HDFS)"
  echo "          sharelib upgrade -fs FS_URI [-locallib SHARED_LIBRARY] (upgrade existing sharelib, fails if there"
  echo "                                                                  is no existing sharelib installed in HDFS)"
  echo "          (without options prints this usage information)"
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
prepareWar=""
inputWar="${OOZIE_HOME}/oozie.war"
outputWar="${CATALINA_BASE}/webapps/oozie.war"
outputWarExpanded="${CATALINA_BASE}/webapps/oozie"
secure=""
secureConfigsDir="${CATALINA_BASE}/conf/ssl"

while [ $# -gt 0 ]
do
  if [ "$1" = "sharelib" ]; then
    shift
    OOZIEFSCLI_OPTS="-Doozie.home.dir=${OOZIE_HOME}";
    OOZIEFSCLI_OPTS="${OOZIEFSCLI_OPTS} -Doozie.config.dir=${OOZIE_CONFIG}";
    OOZIEFSCLI_OPTS="${OOZIEFSCLI_OPTS} -Doozie.log.dir=${OOZIE_LOG}";
    OOZIEFSCLI_OPTS="${OOZIEFSCLI_OPTS} -Doozie.data.dir=${OOZIE_DATA}";
    OOZIEFSCLI_OPTS="${OOZIEFSCLI_OPTS} -Dderby.stream.error.file=${OOZIE_LOG}/derby.log"

    OOZIECPPATH=""
    OOZIECPPATH=${BASEDIR}/libtools/'*':${BASEDIR}/libext/'*'

    if test -z ${JAVA_HOME}; then
      JAVA_BIN=java
    else
      JAVA_BIN=${JAVA_HOME}/bin/java
    fi

    ${JAVA_BIN} ${OOZIEFSCLI_OPTS} -cp ${OOZIECPPATH} org.apache.oozie.tools.OozieSharelibCLI "${@}"
    exit 0
  elif [ "$1" = "-extjs" ]; then
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
  elif [ "$1" = "-secure" ]; then
    shift
    secure=true
  elif [ "$1" = "prepare-war" ]; then
    prepareWar=true
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

echo

if [ "${addExtjs}${addJars}${addHadoopJars}${prepareWar}" == "" ]; then
  echo "no arguments given"
  printUsage
  exit -1
else
  if [ -e "${outputWar}" ]; then
      chmod -f u+w ${outputWar}
      rm -rf ${outputWar}
  fi
  rm -rf ${outputWarExpanded}

  # Adding extension JARs

  libext=${OOZIE_HOME}/libext
  if [ -d "${libext}" ]; then
    if [ `ls ${libext} | grep \.jar\$ | wc -c` != 0 ]; then
      for i in "${libext}/"*.jar; do
        echo "INFO: Adding extension: $i"
        jarsPath="${jarsPath}:$i"
        addJars="true"
      done
    fi
    if [ -f "${libext}/ext-2.2.zip" ]; then
      extjsHome=${libext}/ext-2.2.zip
      addExtjs=true
    fi
  # find war files (e.g., workflowgenerator) under /libext and deploy
    if [ `ls ${libext} | grep \.war\$ | wc -c` != 0 ]; then
      for i in "${libext}/"*.war; do
        echo "INFO: Deploying extention: $i"
        cp $i ${CATALINA_BASE}/webapps/
      done
    fi
  fi

  OPTIONS=""
  if [ "${addExtjs}" != "" ]; then
    OPTIONS="-extjs ${extjsHome}"
  else
    echo "INFO: Oozie webconsole disabled, ExtJS library not specified"
  fi
  if [ "${addJars}" != "" ]; then
    OPTIONS="${OPTIONS} -jars ${jarsPath}"
  fi
  if [ "${addHadoopJars}" != "" ]; then
    OPTIONS="${OPTIONS} -hadoop ${hadoopVersion} ${hadoopPath}"
  fi
  if [ "${secure}" != "" ]; then
    OPTIONS="${OPTIONS} -secureWeb ${secureConfigsDir}/ssl-web.xml"
    #Use the SSL version of server.xml in oozie-server
    cp ${secureConfigsDir}/ssl-server.xml ${CATALINA_BASE}/conf/server.xml
    echo "INFO: Using secure server.xml"
  else
    #Use the regular version of server.xml in oozie-server
    cp ${secureConfigsDir}/server.xml ${CATALINA_BASE}/conf/server.xml
  fi

  ${OOZIE_HOME}/bin/addtowar.sh -inputwar ${inputWar} -outputwar ${outputWar} ${OPTIONS}

  if [ "$?" != "0" ]; then
    exit -1
  fi

  echo

  echo "INFO: Oozie is ready to be started"

  echo

fi
