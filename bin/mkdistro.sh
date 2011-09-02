#!/bin/sh
#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
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
}

export DATETIME=`date -u "+%Y.%m.%d-%H:%M:%SGMT"`
cd ${BASEDIR}
export SVNREV=`svn info | grep "Revision" | awk '{print $2}'`
export SVNURL=`svn info | grep "URL" | awk '{print $2}'`

#clean up local repo
cleanUpLocalRepo

MVN_OPTS="-Dbuild.time=${DATETIME} -Dsvn.revision=${SVNREV} -Dsvn.url=${SVNURL}"

#clean, compile, test, package, install
mvn clean install ${MVN_OPTS} $*
checkExitStatus "running: clean compile, test, package, install"

if [ "$FULLDISTRO" == "true" ]; then
  #cobertura
  mvn cobertura:cobertura ${MVN_OPTS} $*
  checkExitStatus  "running: cobertura"

  #dependencies report
  mvn project-info-reports:dependencies ${MVN_OPTS} $*
  checkExitStatus  "running: dependencies"

  #TODO findbugs report
fi

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

echo
echo "Oozie distro created, DATE[${DATETIME}] SVN-REV[${SVNREV}], available at [${BASEDIR}/distro/target]"
echo