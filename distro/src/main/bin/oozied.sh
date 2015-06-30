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
  echo "Usage: oozied.sh (start|stop|run|status) [<catalina-args...>]"
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
MAPR_CONF_DIR=/opt/mapr/conf
ENV_FILE=env.sh
HADOOP_BASE_DIR=/opt/mapr/hadoop/hadoop-

source ${BASEDIR}/bin/oozie-sys.sh
# MapR change. Source env.sh if it exists
if [[ -n $(find ${MAPR_CONF_DIR} -name "${ENV_FILE}" -print) ]]; then
    source ${MAPR_CONF_DIR}/env.sh
fi

CATALINA=${OOZIE_CATALINA_HOME:-${BASEDIR}/oozie-server}/bin/catalina.sh

# Find hadoop conf directory, to be passed to oozie-site.xml
# Find hadoop home
function real_script_name() {
        local base=$1
        local real
        if readlink -f $base >/dev/null 2>&1; then
                # Darwin/Mac OS X
                real=`readlink -f $base`
        fi
        if [[ "$?" != "0" || -z "$real" ]]; then
                # Linux
                local bin=$(cd -P -- "$(dirname -- "$base")">/dev/null && pwd -P)
                local script="$(basename -- "$base")"
                real="$bin/$script"
        fi
        echo "$real"
}
# MapR change
if [ -e ${MAPR_CONF_DIR}/hadoop_version ]; then
  # MapR version >= 4.0.1
  mapreduce_mode=
  [ ! -z "${MAPR_MAPREDUCE_MODE}" ] && mapreduce_mode=${MAPR_MAPREDUCE_MODE}
  [ -z "${mapreduce_mode}" ] && source ${MAPR_CONF_DIR}/hadoop_version && mapreduce_mode=$default_mode

  if [ "${mapreduce_mode}" = "classic" ]; then
    # Classic
    mode=1
  else
    # Yarn
    mode=2
  fi
  else
    mode=""
fi

hadoop_bin=`real_script_name "/usr/bin/hadoop${mode}"`
hadoop_home="${hadoop_bin_dir}/../"
version=`/usr/bin/hadoop${mode} version`
confDir="hadoop-conf"
hadoopVersionFile="${MAPR_CONF_DIR}/hadoop_version"
if [ -f ${hadoopVersionFile} ]
then
    hadoop_mode=`cat ${hadoopVersionFile} | grep default_mode | cut -d '=' -f 2`
    if [ "$hadoop_mode" = "yarn" ]; then
	    version_hadoop=`cat ${hadoopVersionFile} | grep yarn_version | cut -d '=' -f 2`
	    confDir=${HADOOP_BASE_DIR}${version_hadoop}/etc/hadoop/
    elif [ "$hadoop_mode" = "classic" ]; then
    	version_hadoop=`cat ${hadoopVersionFile} | grep classic_version | cut -d '=' -f 2`
    	confDir=${HADOOP_BASE_DIR}${version_hadoop}/conf/
    else
	    echo 'Unknown hadoop version'
    fi

else
    version_cmd="hadoop version"
    res=`eval $CMD`
    HADOOP_VERSION_PATH=`readlink \`which hadoop\` | awk -F "/" '{print$5}'`
    version_hadoop=`echo ${HADOOP_VERSION_PATH} | cut -d'-' -f 2`
    confDir=${HADOOP_BASE_DIR}${version_hadoop}/conf/
fi


setup_catalina_opts() {
  # The Java System properties 'oozie.http.port' and 'oozie.https.port' are not
  # used by Oozie, they are used in Tomcat's server.xml configuration file
  #
  echo "Using   CATALINA_OPTS:       ${CATALINA_OPTS}"

  catalina_opts="-Doozie.home.dir=${OOZIE_HOME}";
  catalina_opts="${catalina_opts} -Doozie.config.dir=${OOZIE_CONFIG}";
  catalina_opts="${catalina_opts} -Doozie.log.dir=${OOZIE_LOG}";
  catalina_opts="${catalina_opts} -Doozie.data.dir=${OOZIE_DATA}";
  catalina_opts="${catalina_opts} -Doozie.instance.id=${OOZIE_INSTANCE_ID}"

  catalina_opts="${catalina_opts} -Doozie.config.file=${OOZIE_CONFIG_FILE}";

  catalina_opts="${catalina_opts} -Doozie.log4j.file=${OOZIE_LOG4J_FILE}";
  catalina_opts="${catalina_opts} -Doozie.log4j.reload=${OOZIE_LOG4J_RELOAD}";

  catalina_opts="${catalina_opts} -Doozie.http.hostname=${OOZIE_HTTP_HOSTNAME}";
  catalina_opts="${catalina_opts} -Doozie.admin.port=${OOZIE_ADMIN_PORT}";
  catalina_opts="${catalina_opts} -Doozie.http.port=${OOZIE_HTTP_PORT}";
  catalina_opts="${catalina_opts} -Doozie.https.port=${OOZIE_HTTPS_PORT}";
  catalina_opts="${catalina_opts} -Doozie.base.url=${OOZIE_BASE_URL}";
  catalina_opts="${catalina_opts} -Doozie.https.keystore.file=${OOZIE_HTTPS_KEYSTORE_FILE}";
  catalina_opts="${catalina_opts} -Doozie.https.keystore.pass=${OOZIE_HTTPS_KEYSTORE_PASS}";
  catalina_opts="${catalina_opts} -Dmapr.library.flatclass=true";
  catalina_opts="${catalina_opts} ${MAPR_AUTH_CLIENT_OPTS}";
  catalina_opts="${catalina_opts} -Dhadoop_conf_directory=${confDir}";

  # add required native libraries such as compression codecs
  # MAPR CHANGE: Add mapr lib to the java library path
  catalina_opts="${catalina_opts} -Djava.library.path=${JAVA_LIBRARY_PATH}:/opt/mapr/lib";
  # MAPR Change: Set parameters in oozie-site.xml based on if MapR security is enabled or not
  if [ "$MAPR_SECURITY_STATUS" = "true" ]; then
      catalina_opts="${catalina_opts} -Dmapr_sec_type=org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler"
      catalina_opts="${catalina_opts} -Dmapr_sec_enabled=true"
      catalina_opts="${catalina_opts} -Dmapr_signature_secret=com.mapr.security.maprauth.MaprSignatureSecretFactory"
  else
      catalina_opts="${catalina_opts} -Dmapr_sec_type=simple"
      catalina_opts="${catalina_opts} -Dmapr_sec_enabled=false"
      catalina_opts="${catalina_opts} -Dmapr_signature_secret=oozie"
  fi

  echo "Adding to CATALINA_OPTS:     ${catalina_opts}"

  export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts}"
}

setup_oozie() {
  if [ "${mode}" != "" ]; then
    # This means we are operating with MapR >= 4.0.1 and need to copy over correct war before startup.
    if [ ! -e "${OOZIE_HOME}/oozie-hadoop${mode}.war" ]; then
      echo "WARN: Oozie WAR has not been set up at ''${OOZIE_HOME}'', doing default set up"
      ${BASEDIR}/bin/oozie-setup.sh
      if [ "$?" != "0" ]; then
        exit -1
      fi
    fi
    oozie_hadoop_version=""
    oozie_hadoop_version_file=${OOZIE_HOME}/oozie-hadoop-version
    # Determine the current war's hadoop version.
    if [ -f ${oozie_hadoop_version_file} ]; then
      source ${oozie_hadoop_version_file}
    fi

    #replace lib to correct
    rm -rf ${BASEDIR}/lib
    mkdir ${BASEDIR}/lib
    unzip ${BASEDIR}/oozie-hadoop${mode}.war WEB-INF/lib/*.jar -d ${BASEDIR}/lib > /dev/null
    mv ${BASEDIR}/lib/WEB-INF/lib/*.jar ${BASEDIR}/lib/
    rmdir ${BASEDIR}/lib/WEB-INF/lib
    rmdir ${BASEDIR}/lib/WEB-INF

    # If needed, copy the correct oozie war over.
    if [ ! -e ${CATALINA_BASE}/webapps/oozie.war -o "${mode}" != "${oozie_hadoop_version}" -o ${OOZIE_HOME}/oozie-hadoop${mode}.war -nt ${CATALINA_BASE}/webapps/oozie.war ]; then
      cp ${OOZIE_HOME}/oozie-hadoop${mode}.war ${CATALINA_BASE}/webapps/oozie.war
      rm -rf ${CATALINA_BASE}/webapps/oozie
      echo "oozie_hadoop_version=${mode}" > ${oozie_hadoop_version_file}
    fi

    # default share dir
    directory=/oozie/share

    if hadoop fs -test -d ${directory} ; then
      hadoop fs -rmr ${directory}
    fi
    hadoop fs -mkdir -p $directory
    hadoop fs -put ${OOZIE_HOME}/share2/* ${directory}

    hadoop fs -rmr ${directory}/lib/distcp/*
    hadoop fs -rmr ${directory}/lib/oozie/*
    if [ "${mode}" == "1" ]; then
      hadoop fs -put ${OOZIE_HOME}/share1/lib/distcp/* ${directory}/lib/distcp/
      hadoop fs -put ${OOZIE_HOME}/share1/lib/oozie/* ${directory}/lib/oozie/
    else
      hadoop fs -put ${OOZIE_HOME}/share2/lib/distcp/* ${directory}/lib/distcp/
      hadoop fs -put ${OOZIE_HOME}/share2/lib/oozie/* ${directory}/lib/oozie/
    fi

  fi
  if [ ! -e "${CATALINA_BASE}/webapps/oozie.war" ]; then
    echo "WARN: Oozie WAR has not been set up at '${CATALINA_BASE}/webapps', doing default set up"
    ${BASEDIR}/bin/oozie-setup.sh prepare-war
    if [ "$?" != "0" ]; then
      exit -1
    fi
  fi
  echo
}

setup_ooziedb() {
  echo "Setting up oozie DB"
  ${BASEDIR}/bin/ooziedb.sh create -run
  if [ "$?" != "0" ]; then
    exit -1
  fi
  echo
}

case $actionCmd in
  (start|run)
    setup_catalina_opts
    setup_oozie
    setup_ooziedb
    #TODO setup default oozie sharelib
    ;;
  (stop)
    if [[ $@ != "-force" ]]; then
      actionCmd=${actionCmd}" -force"
    fi
    setup_catalina_opts

    # A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
    export JAVA_OPTS=${CATALINA_OPTS}
    ;;
  (status)
    if [ ! -z "$CATALINA_PID" ]; then
     if [ -f "$CATALINA_PID" ]; then
       if [ -s "$CATALINA_PID" ]; then
         if [ -r "$CATALINA_PID" ]; then
           PID=`cat "$CATALINA_PID"`
           ps -p $PID >/dev/null 2>&1
           if [ $? -eq 0 ] ; then
             echo "Tomcat is running with PID $PID."
             exit 0
           fi
          fi
       fi
     fi
    fi
    echo "Most likely Tomcat is not running"
    exit 1

    ;;

esac

exec $CATALINA $actionCmd "$@"
