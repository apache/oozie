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
  echo "          prepare-war [-d directory] [-secure] (-d identifies an alternative directory for processing jars"
  echo "                                                -secure will configure the war file to use HTTPS (SSL))"
  echo "                                                -hadoop HADOOP_VERIONS HADOOP_HOME will add hadoop jars"
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
  echo "          db create|upgrade|postupgrade -run [-sqlfile <FILE>] (create, upgrade or postupgrade oozie db with an"
  echo "                                                                optional sql File)"
  echo "          (without options prints this usage information)"
  echo
  echo " EXTJS can be downloaded from http://www.extjs.com/learn/Ext_Version_Archives"
  echo
}

#Creating temporary directory
function prepare() {
  tmpDir=/tmp/oozie-war-packing-$$
  rm -rf ${tmpDir}
  mkdir ${tmpDir}
  tmpWarDir=${tmpDir}/oozie-war
  mkdir ${tmpWarDir}
  checkExec "creating staging directory ${tmpDir}"
}

#cleans up temporary directory
function cleanUp() {
  if [ ! "${tmpDir}" = "" ]; then
    rm -rf ${tmpDir}
    checkExec "deleting staging directory ${tmpDir}"
  fi
}

#check execution of command
function checkExec() {
  if [ $? -ne 0 ]
  then
    echo
    echo "Failed: $1"
    echo
    cleanUp
    exit -1;
  fi
}

#check that a file/path exists
function checkFileExists() {
  if [ ! -e ${1} ]; then
    echo
    echo "File/Dir does no exist: ${1}"
    echo
    cleanUp
    exit -1
  fi
}

#check that a file/path does not exist
function checkFileDoesNotExist() {
  if [ -e ${1} ]; then
    echo
    echo "File/Dir already exists: ${1}"
    echo
    cleanUp
    exit -1
  fi
}

read_dom () {
    local IFS=\>
    read -d \< ENTITY CONTENT
}

function getKeyStoreLocationAndPassword() {
  if [[ -n $(find ${OOZIE_HOME}/../../conf -name "ssl-server.xml" 2>&1) ]]; then
sslServer="${OOZIE_HOME}/../../conf/ssl-server.xml"

      locationFound=""
      location=""
      passwordFound=""
      password=""

      #parse xml and get keystore location and password
      while read_dom; do
if [[ $ENTITY = "name" ]]; then
if [[ $CONTENT = "ssl.server.keystore.location" ]]; then
locationFound="true"
              fi
if [[ $CONTENT = "ssl.server.keystore.password" ]]; then
passwordFound="true"
              fi
fi
if [[ $location = "" && $locationFound = "true" && $ENTITY = "value" ]]; then
location=`echo $CONTENT`
          fi
if [[ $password = "" && $passwordFound = "true" && $ENTITY = "value" ]]; then
password=`echo $CONTENT`
          fi
done < ${sslServer}

      #replace these values in oozie's ssl-server.xml
      sed -i -e "s@\${oozie.https.keystore.file}@${location}@g" ${CATALINA_BASE}/conf/server.xml
      sed -i -e "s@\${oozie.https.keystore.pass}@${password}@g" ${CATALINA_BASE}/conf/server.xml
  fi

echo "Keystore location successfully added"
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
addHadoopJars=""
addBothHadoopJars=false
additionalDir=""
extjsHome=""
jarsPath=""
prepareWar=""
inputWar="${OOZIE_HOME}/oozie.war"
outputWar="${CATALINA_BASE}/webapps/oozie.war"
outputWarExpanded="${CATALINA_BASE}/webapps/oozie"
secure=""
secureConfigsDir="${CATALINA_BASE}/conf/ssl"
MapRHomeDir=/opt/mapr
hadoopVersionFile="${MapRHomeDir}/conf/hadoop_version"

if [ -e ${hadoopVersionFile} ]; then
  addBothHadoopJars=true
fi

while [ $# -gt 0 ]
do
  if [ "$1" = "sharelib" ] || [ "$1" = "db" ]; then
    OOZIE_OPTS="-Doozie.home.dir=${OOZIE_HOME}";
    OOZIE_OPTS="${OOZIE_OPTS} -Doozie.config.dir=${OOZIE_CONFIG}";
    OOZIE_OPTS="${OOZIE_OPTS} -Doozie.log.dir=${OOZIE_LOG}";
    OOZIE_OPTS="${OOZIE_OPTS} -Doozie.data.dir=${OOZIE_DATA}";
    OOZIE_OPTS="${OOZIE_OPTS} -Dderby.stream.error.file=${OOZIE_LOG}/derby.log"

    #Create lib directory from war if lib doesn't exist
    if [ ! -d "${BASEDIR}/lib" ]; then
      mkdir ${BASEDIR}/lib
      unzip ${BASEDIR}/oozie.war WEB-INF/lib/*.jar -d ${BASEDIR}/lib > /dev/null
      mv ${BASEDIR}/lib/WEB-INF/lib/*.jar ${BASEDIR}/lib/
      rmdir ${BASEDIR}/lib/WEB-INF/lib
      rmdir ${BASEDIR}/lib/WEB-INF
    fi

    OOZIECPPATH=""
    OOZIECPPATH=${BASEDIR}/lib/'*':${BASEDIR}/libtools/'*':${BASEDIR}/libext/'*'

    if test -z ${JAVA_HOME}; then
      JAVA_BIN=java
    else
      JAVA_BIN=${JAVA_HOME}/bin/java
    fi

    if [ "$1" = "sharelib" ]; then
      shift
      ${JAVA_BIN} ${OOZIE_OPTS} -cp ${OOZIECPPATH} org.apache.oozie.tools.OozieSharelibCLI "${@}"
    else
      shift
      ${JAVA_BIN} ${OOZIE_OPTS} -cp ${OOZIECPPATH} org.apache.oozie.tools.OozieDBCLI "${@}"
    fi
    exit $?
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
  elif [ "$1" = "-secure" ]; then
    shift
    secure=true
  elif [ "$1" = "-d" ]; then
    shift
    additionalDir=$1
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

if [ "${prepareWar}${addHadoopJars}" == "" ]; then
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
  if [ "${additionalDir}" != "" ]; then
    libext=${additionalDir}
  fi

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

  prepare

  checkFileExists ${inputWar}
  checkFileDoesNotExist ${outputWar}

  if [ "${addExtjs}" = "true" ]; then
    checkFileExists ${extjsHome}
  fi

  if [ "${addJars}" = "true" ]; then
      for jarPath in ${jarsPath//:/$'\n'}
      do
        checkFileExists ${jarPath}
      done
  fi

  #Unpacking original war
  unzip ${inputWar} -d ${tmpWarDir} > /dev/null
  checkExec "unzipping Oozie input WAR"

  components=""

  if [ "${secure}" != "" ]; then
    #Use the SSL version of server.xml in oozie-server
    checkFileExists ${secureConfigsDir}/ssl-server.xml
    cp ${secureConfigsDir}/ssl-server.xml ${CATALINA_BASE}/conf/server.xml
    getKeyStoreLocationAndPassword
    #Inject the SSL version of web.xml in oozie war
    checkFileExists ${secureConfigsDir}/ssl-web.xml
    cp ${secureConfigsDir}/ssl-web.xml ${tmpWarDir}/WEB-INF/web.xml
    echo "INFO: Using secure server.xml and secure web.xml"
  else
    #Use the regular version of server.xml in oozie-server
    checkFileExists ${secureConfigsDir}/server.xml
    cp ${secureConfigsDir}/server.xml ${CATALINA_BASE}/conf/server.xml
    #No need to restore web.xml because its already in the original WAR file
  fi

  OPTIONS=""
  if [ "${addExtjs}" != "" ]; then
    OPTIONS="-extjs ${extjsHome}"
  fi

  if [ "${addJars}" != "" ]; then
    OPTIONS="${OPTIONS} -jars ${jarsPath}"
  fi
  if [ ${addBothHadoopJars} = false -a "${addHadoopJars}" != "" ]; then
    OPTIONS="${OPTIONS} -hadoop ${hadoopVersion} ${hadoopPath}"
  fi
  if [ "${secure}" != "" ]; then
    OPTIONS="${OPTIONS} -secureWeb ${secureConfigsDir}/ssl-web.xml"
    #Use the SSL version of server.xml in oozie-server
    cp ${secureConfigsDir}/ssl-server.xml ${CATALINA_BASE}/conf/server.xml
    getKeyStoreLocationAndPassword
    echo "INFO: Using secure server.xml"
  else
    #Use the regular version of server.xml in oozie-server
    cp ${secureConfigsDir}/server.xml ${CATALINA_BASE}/conf/server.xml
  fi

  if [ ${addBothHadoopJars} = false ]; then
    ${OOZIE_HOME}/bin/addtowar.sh -inputwar ${inputWar} -outputwar ${outputWar} ${OPTIONS}
    echo "${hadoopVersion}" > ${OOZIE_LOG}/hadoop_version.log
  else
    # Build 2 wars. One with Hadoop1 jars and the other one with Hadoop2 jars.
    source ${hadoopVersionFile}
    oozie_hadoop1_war=${OOZIE_HOME}/oozie-hadoop1.war
    oozie_hadoop2_war=${OOZIE_HOME}/oozie-hadoop2.war
    hadoop1Path=${MapRHomeDir}/hadoop/hadoop-${classic_version}
    hadoop2Path=${MapRHomeDir}/hadoop/hadoop-${yarn_version}

    if [ -e "${oozie_hadoop1_war}" ]; then
      chmod -f u+w ${oozie_hadoop1_war}
      rm -rf ${oozie_hadoop1_war}
    fi

    if [ -e "${oozie_hadoop2_war}" ]; then
      chmod -f u+w ${oozie_hadoop2_war}
      rm -rf ${oozie_hadoop2_war}
    fi

    # Build oozie war with Hadoop1
    ${OOZIE_HOME}/bin/addtowar.sh -inputwar ${inputWar} -outputwar ${oozie_hadoop1_war} ${OPTIONS} -hadoop ${classic_version} ${hadoop1Path}
    # Build oozie war with Hadoop2
    ${OOZIE_HOME}/bin/addtowar.sh -inputwar ${inputWar} -outputwar ${oozie_hadoop2_war} ${OPTIONS} -hadoop ${yarn_version} ${hadoop2Path}
  fi

  if [ "$?" != "0" ]; then
    exit -1
  fi

  echo

  echo "INFO: Oozie is ready to be started"

  echo

fi
