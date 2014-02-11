@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem      http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@rem Set Oozie specific environment variables here.

@rem Settings for the Embedded Tomcat that runs Oozie
@rem Java System properties for Oozie should be specified in this variable
@rem
set CATALINA_OPTS=%CATALINA_OPTS% -Xmx1024m

@rem Oozie configuration file to load from Oozie configuration directory
@rem
@rem set OOZIE_CONFIG_FILE=oozie-site.xml

@rem Oozie logs directory
@rem
@rem set OOZIE_LOG=%OOZIE_HOME%\logs

@rem Oozie Log4J configuration file to load from Oozie configuration directory
@rem
@rem set OOZIE_LOG4J_FILE=oozie-log4j.properties

@rem Reload interval of the Log4J configuration file, in seconds
@rem
@rem set OOZIE_LOG4J_RELOAD=10

@rem The port Oozie server runs
@rem
@rem set OOZIE_HTTP_PORT=11000

@rem The port Oozie server runs if using SSL (HTTPS)
@rem
@rem export OOZIE_HTTPS_PORT=11443

@rem The host name Oozie server runs on
@rem
@rem set OOZIE_HTTP_HOSTNAME=%COMPUTERNAME%

@rem The base URL for callback URLs to Oozie
@rem
@rem set OOZIE_BASE_URL=http://%OOZIE_HTTP_HOSTNAME%:%OOZIE_HTTP_PORT%/oozie

@rem The location of the keystore for the Oozie server if using SSL (HTTPS)
@rem
@rem set OOZIE_HTTPS_KEYSTORE_FILE=%HOME%/.keystore

@rem The password of the keystore for the Oozie server if using SSL (HTTPS)
@rem
@rem set OOZIE_HTTPS_KEYSTORE_PASS=password

@rem The Oozie Instance ID
@rem
@rem set OOZIE_INSTANCE_ID=%OOZIE_HTTP_HOSTNAME%