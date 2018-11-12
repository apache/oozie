#!/usr/bin/env bash
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

#Utility method
function setRevUrl() {
  if command -v git  >/dev/null; then
    if [ -d ".git" ]; then
      VC_REV=$(git branch -v | awk '/^\*/ {printf("%s@%s\n", $2, $3); }')
      VC_URL=$(git remote -v | grep origin | grep fetch | awk '{print $2}')
    fi
  fi

   #If nothing found
   if [ "${VC_REV}" == "" ]; then
       VC_REV="unavailable"
       VC_URL="unavailable"
   fi
   export VC_REV
   export VC_URL
}

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=$(ls -ld "${PRG}")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "${PRG}")/"$link"
  fi
done

BASEDIR=$(dirname "${PRG}")
BASEDIR=$(cd "${BASEDIR}"/.. && pwd) || { echo 'ERROR, directory change failed' ; exit -1; }

cd "${BASEDIR}" || { echo 'ERROR, directory change failed' ; exit -1; }

DATETIME=$(date -u "+%Y.%m.%d-%H:%M:%SGMT")
export DATETIME
setRevUrl

MVN_OPTS="-Dbuild.time=${DATETIME} -Dvc.revision=${VC_REV} -Dvc.url=${VC_URL} -DgenerateDocs"

DATETIME2=$(date -u "+%Y%m%d-%H%M%SGMT")
export DATETIME2

if ! mvn clean package assembly:single "${MVN_OPTS}" "$@"; then
  echo
  echo "ERROR, Oozie distro creation failed"
  echo
  exit -1
else
  echo
  echo "Oozie distro created, DATE[${DATETIME}] VC-REV[${VC_REV}], available at [${BASEDIR}/distro/target]"
  echo
fi
