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
#     http://www.apache.org/licenses/LICENSE-2.0
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


if [ "$1" == "-full" ]; then
  FULLDISTRO=true
  shift
fi

if [ "$1" == "-h" ]; then
  echo
  echo "**persistence.xml replacement**"
  echo
  echo "usage: $0 [-ddbtype] [-uusername] [-ppassword] [-lurl]"
  echo
  echo "**mvn help**"
  mvn -h
  exit 0
fi

function checkExitStatus {
  if [ "$?" != "0" ]; then
    echo
    echo "ERROR, Oozie distro could not be created ${1}"
    echo
    cleanUpLocalRepo
    exit -1
  fi
}

function cleanUpLocalRepo {
  rm -rf ~/.m2/repository/org/apache/oozie/*
  rm -rf $PWD/core/mem
}

#process createjpaconf.sh arguments - begin
while getopts :d:u:p:l: opt
do
    case "$opt" in
      d)  DBTYPE="$OPTARG";;
      u)  USERNAME="$OPTARG";;
      p)  PASSWORD="$OPTARG";;
      l)  DBURL="$OPTARG";;
      \?) #unknown flag
          break;;
    esac
done

if [ -z "$DBTYPE" ]; then
   echo "[INFO] Use default persistence.xml!!"
else
   if [ -z "$USERNAME" ]; then
        echo "[ERROR] DB UserName required!!"
        exit 1
   fi
   if [ -z "$DBURL" ]; then
        echo "[ERROR] DB URL required!!"
        exit 1
   fi
   [[ "$DBTYPE" = [-]* ]] && { echo "[ERROR] Wrong DBTYPE!!" ; exit 1 ; }
   [[ "$USERNAME" = [-]* ]] && { echo "[ERROR] Wrong USERNAME" ; exit 1 ; }
   [[ "$PASSWORD" = [-]* ]] && { echo "[ERROR] Wrong PASSWORD" ; exit 1 ; }
   [[ "$DBURL" = [-]* ]] && { echo "[ERROR] Wrong DBURL" ; exit 1 ; }
   echo "[INFO]	Use replaced persistence.xml!!"
   shift $(( $OPTIND - 1 ))
   SCRIPT_DIR=$(dirname $0)
   if [ -z "$PASSWORD" ]; then
      ${SCRIPT_DIR}/createjpaconf.sh -d${DBTYPE} -u${USERNAME} -l${DBURL}
   else
      ${SCRIPT_DIR}/createjpaconf.sh -d${DBTYPE} -u${USERNAME} -p${PASSWORD} -l${DBURL}
   fi
fi
#process createjpaconf.sh arguments - end

export DATETIME=`date -u "+%Y.%m.%d-%H:%M:%SGMT"`
cd ${BASEDIR}
export SVNREV=`svn info | grep "Revision" | awk '{print $2}'`
export SVNURL=`svn info | grep "URL" | awk '{print $2}'`

#clean up local repo
#ln -s $PWD/client/src $PWD/client_enhanced/src
#ln -s $PWD/client_enhanced/pom.xml.enhance $PWD/client_enhanced/pom.xml
cleanUpLocalRepo

MVN_OPTS="-Dbuild.time=${DATETIME} -Dsvn.revision=${SVNREV} -Dsvn.url=${SVNURL}"

cd client
mvn clean package -Doozie.build.jpa.enhanced=false ${MVN_OPTS} $*
mvn assembly:single -Doozie.build.jpa.enhanced=false ${MVN_OPTS} $*
cd ..

#clean, compile, test, package, install
mvn clean install ${MVN_OPTS} $*
checkExitStatus "running: clean compile, test, package, install"

#if [ "$FULLDISTRO" == "true" ]; then

  #clover
  #mvn clover2:instrument clover2:aggregate clover2:clover ${MVN_OPTS} $*
  #checkExitStatus  "running: clover"

  #dependencies report
  #mvn project-info-reports:dependencies ${MVN_OPTS} $*
  #checkExitStatus  "running: dependencies"

  #findbugs report
  #mvn findbugs:findbugs ${MVN_OPTS} $*
  #checkExitStatus  "running: findbugs"

#fi

#javadocs
mvn javadoc:javadoc ${MVN_OPTS} $*
checkExitStatus  "running: javadoc"

cd docs
mvn clean
mvn site:site assembly:single
checkExitStatus  "running: docs site"
cd ..

#putting together distro
mvn assembly:single ${MVN_OPTS} $*
checkExitStatus  "running: assembly"

cleanUpLocalRepo
#unlink $PWD/client_enhanced/src
#unlink $PWD/client_enhanced/pom.xml

echo
echo "Oozie distro created, DATE[${DATETIME}] SVN-REV[${SVNREV}], available at [${BASEDIR}/distro/target]"
echo

