#!/bin/sh
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

# To run build in hudson, the default env variables should be defined in the environment. If not, please do the exports before
# invoking this script.
# export WORKSPACE=PATH_TO_WORKSPACE
# export TOOLS_HOME=PATH_TO_COMMON_LIBRAARY_HOME
# export OOZIE_GIT_REPO=git://github.com/apache/oozie.git

# You can also explicitly change JAVA_HOME or M3_HOME below.

export JAVA_HOME=${TOOLS_HOME}/java/latest
export M3_HOME=${TOOLS_HOME}/maven/apache-maven-3.0.3
export PATH=$JAVA_HOME/bin:$M3_HOME/bin:$PATH

#Please uncomment these lines if the git repo needs to download
#git clone -o origin $OOZIE_GIT_REPO $WORKSPACE
#cd $WORKSPACE

M2DIR=`mktemp -d /tmp/oozie-m2.XXXXX`
bin/mkdistro.sh $1 -Dmaven.repo.local=$M2DIR
EXIT=$?
rm -rf $M2DIR
exit $EXIT