#!/bin/bash
#
# Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
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

cd ${BASEDIR}

export DATETIME=`date -u "+%Y.%m.%d-%H:%M:%SGMT"`
export VC_REV=`git branch -v | awk '/^\*/ {printf("%s@%s\n", $2, $3); }'`
# Out canonical repo is @GitHub -- hence hardcoding
export VC_URL="git://github.com/yahoo/oozie.git"

MVN_OPTS="-Dbuild.time=${DATETIME} -Dvc.revision=${VC_REV} -Dvc.url=${VC_URL} -DgenerateDocs"

export DATETIME2=`date -u "+%Y%m%d-%H%M%SGMT"`
mvn clean package assembly:single ${MVN_OPTS} $* 2>&1 | tee ${BASEDIR}/mkdistro-${DATETIME2}.out

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
