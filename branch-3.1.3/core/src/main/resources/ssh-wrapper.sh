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

sleep 1
callbackCmnd=${1}
shift
callbackUrl=${1}
shift
callbackPost=${1}
shift
actionId=${1}
shift
dir=`dirname $0`
mpid=`echo $$`
echo $mpid > $dir/$actionId.pid
stdout="$dir/$mpid.$actionId.stdout"
stderr="$dir/$mpid.$actionId.stderr"
cmnd="${*}"
if $cmnd >>${stdout} 2>>${stderr}; then
    export callbackUrl=`echo ${callbackUrl} | sed -e 's/#status/OK/'`
else
    export callbackUrl=`echo ${callbackUrl} | sed -e 's/#status/ERROR/'`
    touch $dir/$mpid.$actionId.error
fi
sleep 1

if [ `du -k ${stdout} | awk '{print $1}'` -gt 8 ]
then
   $callbackPost=_
fi

if [ $callbackPost == "_" ]
then
   export callback="$callbackCmnd $callbackUrl"
else
   callbackOptions=`echo ${callbackPost} | sed -e "s/%%%/ /g"`
   stdout_temp=`echo ${stdout} | sed -e 's/\//%%%/g'`
   callbackOptions=`echo ${callbackOptions} | sed -e "s/#stdout/${stdout_temp}/" | sed -e 's/%%%/\//g'`
   export callback="$callbackCmnd $callbackUrl $callbackOptions"
fi

if $callback; then
    echo "Callback operation successful."
else
    echo "Not able to perform callback operation." >>${stderr}
fi
