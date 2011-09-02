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

rm -rf workflows input-data coordinator
cp -R seed/workflows seed/input-data seed/coordinator .

EXAMPLES=`ls workflows`
APPDIR=`pwd`/workflows

for example in ${EXAMPLES};
do
    LIBPATH=${APPDIR}/${example}/lib
    rm -rf ${LIBPATH}
    mkdir -p ${LIBPATH}
    cp seed/lib/*.jar ${LIBPATH}

    echo "oozie.wf.application.path=hdfs://localhost:9000/tmp/`whoami`/workflows/${example}" > ${example}-job.properties
done

echo "oozie.coord.application.path=hdfs://localhost:9000/tmp/`whoami`/coordinator" > coord-job.properties