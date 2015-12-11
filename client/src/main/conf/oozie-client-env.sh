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

# Oozie URL - used as the value for the '-oozie' option if set
# The URL the Oozie server is listening on
#
# export OOZIE_URL=http://$(hostname -f):11000/oozie

# Timezone - used as the value for the '-timezone' option if set
# Options: See 'oozie info -timezones' for a list
#
# export OOZIE_TIMEZONE=GMT

# Authenticaton type - used as value for the '-auth' option if set
# Note that Oozie normally determines this automatically so you usually won't need to set it
# Options: [SIMPLE|KERBEROS]
#
# export OOZIE_AUTH=SIMPLE

# Oozie client java options
# Use this to set specific JVM options for the Oozie client
#
# export OOZIE_CLIENT_OPTS=

