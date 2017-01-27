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

#Utility method
function setRevUrl() {
   #Checking svn first
   if which svn >/dev/null; then
      if [ -d ".svn" ]; then
         export VC_REV=`svn info | grep "Revision" | awk '{print $2}'`
         export VC_URL=`svn info | grep "URL" | awk '{print $2}'`
      fi
   fi

   #Checking for git if SVN is not there
   if [ "${VC_REV}" == "" ]; then
      if which git  >/dev/null; then
          if [ -d ".git" ]; then
             export VC_REV=`git branch -v | awk '/^\*/ {printf("%s@%s\n", $2, $3); }'`
	     export VC_URL=`git remote -v | grep origin | grep fetch | awk '{print $2}'`
         fi
      fi
   fi

   #If nothing found
   if [ "${VC_REV}" == "" ]; then
       export VC_REV="unavailable"
       export VC_URL="unavailable"
   fi
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

cd ${BASEDIR}

export DATETIME=`date -u "+%Y.%m.%d-%H:%M:%SGMT"`
setRevUrl

MVN_OPTS="-Dbuild.time=${DATETIME} -Dvc.revision=${VC_REV} -Dvc.url=${VC_URL} -DgenerateDocs"

export DATETIME2=`date -u "+%Y%m%d-%H%M%SGMT"`
mvn clean package assembly:single jdeb:jdeb ${MVN_OPTS} "$@"

if [ "$?" != "0" ]; then
  echo
  echo "ERROR, Oozie distro creation failed"
  echo
  exit -1
else
  echo
  echo "Oozie distro created, DATE[${DATETIME}] VC-REV[${VC_REV}], available at [${BASEDIR}/distro/target]"
  echo
fi
