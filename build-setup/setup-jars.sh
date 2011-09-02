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
BASEDIR=`cd ${BASEDIR};pwd`

function checkExitStatus {
  if [ "$?" != "0" ]; then
    echo "ERROR, Oozie development environment could not be configured"
    exit -1
  fi
}

function installArtifact {
  jar="packages/${2}-${3}.jar"
  installJar ${1} ${2} ${3} ${jar}
}

function installJar {
  pom="packages/${2}-${3}.pom"
  mvn install:install-file -Dpackaging=jar -DgroupId=${1} -DartifactId=${2} -Dversion=${3} -Dfile=${4} -DpomFile=${pom}
  checkExitStatus
}

cd ${BASEDIR}
checkExitStatus

#Hadoop 0.20.0
installArtifact org.apache.hadoop hadoop-core 0.20.0
installArtifact org.apache.hadoop hadoop-streaming 0.20.0
installArtifact org.apache.hadoop hadoop-core 0.20.1
installArtifact org.apache.hadoop hadoop-streaming 0.20.1

#Pig 0.2.0
installArtifact org.apache.hadoop pig 0.2.0-H20-J660

echo
echo "JAR artifacts for Oozie development installed successfully"
echo

