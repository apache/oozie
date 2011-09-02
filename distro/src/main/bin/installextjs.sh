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

EXT_JS=$1
OOZIE_WAR=${BASEDIR}/wars/oozie.war

if [ ! -e ${EXT_JS} ]; then
  echo "ExtJS 2.x distribution not found"
  exit -1
fi

if [ ! -e ${OOZIE_WAR} ]; then
  echo "Oozie WAR not found"
  exit -1
fi

CURRENT_DIR=`pwd`

TMP_DIR=/tmp/oozie-install-$$
rm -rf ${TMP_DIR}
mkdir ${TMP_DIR}

mkdir ${TMP_DIR}/extjs
unzip ${EXT_JS} -d ${TMP_DIR}/extjs > /dev/null

mkdir ${TMP_DIR}/ooziewar
unzip ${OOZIE_WAR} -d ${TMP_DIR}/ooziewar > /dev/null

cd ${TMP_DIR}

if [ -e ooziewar/ext-2.2/resources ]; then
  echo "Specified Oozie WAR '${OOZIE_WAR}' already contains ExtJS library files"
  exit -1
fi

mv extjs/ext-2*/* ooziewar/ext-2.2

cd ${TMP_DIR}/ooziewar
zip -r oozie.war * > /dev/null

cd ${CURRENT_DIR}

if [ ! -e ${OOZIE_WAR}.ori ]; then
  mv ${OOZIE_WAR} ${OOZIE_WAR}.ori
fi

cp ${TMP_DIR}/ooziewar/oozie.war ${OOZIE_WAR}

rm -rf ${TMP_DIR}

echo "Oozie WAR '${OOZIE_WAR}' file now contains ExtJS library"

