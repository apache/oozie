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

# Set Oozie specific environment variables here.

# Settings for the Embedded Tomcat that runs Oozie
# Java System properties for Oozie should be specified in this variable
#
export CATALINA_OPTS="$CATALINA_OPTS -Xmx1024m"

# Oozie configuration file to load from Oozie configuration directory
#
# export OOZIE_CONFIG_FILE=oozie-site.xml

# Oozie logs directory
#
# export OOZIE_LOG=${OOZIE_HOME}/logs

# Oozie Log4J configuration file to load from Oozie configuration directory
#
# export OOZIE_LOG4J_FILE=oozie-log4j.properties

# Reload interval of the Log4J configuration file, in seconds
#
# export OOZIE_LOG4J_RELOAD=10

# The port Oozie server runs
#
# export OOZIE_HTTP_PORT=11000

# The port Oozie server runs if using SSL (HTTPS)
#
# export OOZIE_HTTPS_PORT=11443

# The host name Oozie server runs on
#
# export OOZIE_HTTP_HOSTNAME=`hostname -f`

# The base URL for callback URLs to Oozie
#
# export OOZIE_BASE_URL="http://${OOZIE_HTTP_HOSTNAME}:${OOZIE_HTTP_PORT}/oozie"

# The location of the keystore for the Oozie server if using SSL (HTTPS)
#
# export OOZIE_HTTPS_KEYSTORE_FILE=${HOME}/.keystore

# The password of the keystore for the Oozie server if using SSL (HTTPS)
#
# export OOZIE_HTTPS_KEYSTORE_PASS=password

# The Oozie Instance ID
#
# export OOZIE_INSTANCE_ID="${OOZIE_HTTP_HOSTNAME}"