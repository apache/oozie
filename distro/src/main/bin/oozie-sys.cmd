@rem echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@echo off

set OOZIE_HOME=%~dp0..
echo Setting OOZIE_HOME to '%OOZIE_HOME%'
set oozie_home_tmp=%OOZIE_HOME%

if not defined OOZIE_CONFIG (
  set OOZIE_CONFIG=%OOZIE_HOME%\conf
  echo "Setting OOZIE_CONFIG:        %OOZIE_CONFIG%"
) else (
  echo "Using   OOZIE_CONFIG:        %OOZIE_CONFIG%"
)
set oozie_config_tmp=%OOZIE_CONFIG%

@REM if the configuration dir has a env file, source it
if exist "%OOZIE_CONFIG%\oozie-env.CMD" (
  ECHO "Sourcing:                    %OOZIE_CONFIG%\oozie-env.cmd"
  call %OOZIE_CONFIG%\oozie-env.cmd
)

@REM verify that the sourced env file didn't change OOZIE_HOME
@REM if so, warn and revert
if not "%OOZIE_HOME%" == "%oozie_home_tmp%" (
  echo "WARN: OOZIE_HOME resetting to ''%OOZIE_HOME%' ignored"
  set OOZIE_HOME=%oozie_home_tmp%
)

@REM verify that the sourced env file didn't change OOZIE_CONFIG
@REM if so, warn and revert
if not "%OOZIE_CONFIG%" == "%oozie_config_tmp%" (
  print "WARN: OOZIE_CONFIG resetting to ''%OOZIE_CONFIG%'' ignored"
  set OOZIE_CONFIG=%oozie_config_tmp%
)

if not defined OOZIE_CONFIG_FILE (
  set OOZIE_CONFIG_FILE=oozie-site.xml
  echo Setting OOZIE_CONFIG_FILE:   '%OOZIE_CONFIG_FILE%'
) else (
  echo Using   OOZIE_CONFIG_FILE:   '%OOZIE_CONFIG_FILE%'
)

if not defined OOZIE_DATA (
  set OOZIE_DATA=%OOZIE_HOME%\data
  echo Setting OOZIE_DATA:   '%OOZIE_DATA%'
) else (
  echo Using   OOZIE_DATA:   '%OOZIE_DATA%'
)

if not defined OOZIE_LOG (
  set OOZIE_LOG=%OOZIE_HOME%\logs
  echo Setting OOZIE_LOG:   '%OOZIE_LOG%'
) else (
  echo Using   OOZIE_LOG:   '%OOZIE_LOG%'
)

if not exist %OOZIE_LOG% (
    echo Creating log directory:    %OOZIE_LOG%
    md %OOZIE_LOG%
) else (
    echo Using log directory:   %OOZIE_LOG%
)

if not defined OOZIE_LOG4J_FILE (
  set OOZIE_LOG4J_FILE=oozie-log4j.properties
  echo Setting OOZIE_LOG4J_FILE:   '%OOZIE_LOG4J_FILE%'
) else (
  echo Using   OOZIE_LOG4J_FILE:   '%OOZIE_LOG4J_FILE%'
)

if not defined OOZIE_LOG4J_RELOAD (
  set OOZIE_LOG4J_RELOAD=10
  echo Setting OOZIE_LOG4J_RELOAD:   '%OOZIE_LOG4J_RELOAD%'
) else (
  echo Using   OOZIE_LOG4J_RELOAD:   '%OOZIE_LOG4J_RELOAD%'
)

if not defined OOZIE_HTTP_HOSTNAME (
  set OOZIE_HTTP_HOSTNAME=%COMPUTERNAME%
  echo Setting OOZIE_HTTP_HOSTNAME:   '%OOZIE_HTTP_HOSTNAME%'
) else (
  echo Using   OOZIE_HTTP_HOSTNAME:   '%OOZIE_HTTP_HOSTNAME%'
)

if not defined OOZIE_HTTP_PORT (
  set OOZIE_HTTP_PORT=11000
  echo Setting OOZIE_HTTP_PORT:   '%OOZIE_HTTP_PORT%'
) else (
  echo Using   OOZIE_HTTP_PORT:   '%OOZIE_HTTP_PORT%'
)

if not defined OOZIE_ADMIN_PORT (
  SET /a OOZIE_ADMIN_PORT=%OOZIE_HTTP_PORT%+1
  echo Setting OOZIE_ADMIN_PORT:   '%OOZIE_ADMIN_PORT%'
) else (
  echo Using   OOZIE_ADMIN_PORT:   '%OOZIE_ADMIN_PORT%'
)

if not defined OOZIE_HTTPS_PORT (
  SET OOZIE_HTTPS_PORT=11443
  echo Setting OOZIE_HTTPS_PORT:   '%OOZIE_HTTPS_PORT%'
) else (
  echo Using   OOZIE_HTTPS_PORT:   '%OOZIE_HTTPS_PORT%'
)

if not defined OOZIE_BASE_URL (
  set OOZIE_BASE_URL=http://%OOZIE_HTTP_HOSTNAME%:%OOZIE_HTTP_PORT%/oozie
  echo Setting OOZIE_BASE_URL:   '%OOZIE_BASE_URL%'
) else (
  echo Using   OOZIE_BASE_URL:   '%OOZIE_BASE_URL%'
)

if not defined CATALINA_BASE (
  set CATALINA_BASE=%OOZIE_HOME%\oozie-server
  echo Setting CATALINA_BASE:   '%CATALINA_BASE%'
) else (
  echo Using   CATALINA_BASE:   '%CATALINA_BASE%'
)

if not defined OOZIE_HTTPS_KEYSTORE_FILE (
  set OOZIE_HTTPS_KEYSTORE_FILE=%HOME%\.keystore
  echo Setting OOZIE_HTTPS_KEYSTORE_FILE:   '%OOZIE_HTTPS_KEYSTORE_FILE%'
) else (
  echo Using   OOZIE_HTTPS_KEYSTORE_FILE:   '%OOZIE_HTTPS_KEYSTORE_FILE%'
)

if not defined OOZIE_HTTPS_KEYSTORE_PASS (
  set OOZIE_HTTPS_KEYSTORE_PASS=password
  echo Setting OOZIE_HTTPS_KEYSTORE_PASS:   '%OOZIE_HTTPS_KEYSTORE_PASS%'
) else (
  echo Using   OOZIE_HTTPS_KEYSTORE_PASS:   '%OOZIE_HTTPS_KEYSTORE_PASS%'
)

if not defined OOZIE_INSTANCE_ID (
  set OOZIE_INSTANCE_ID=%OOZIE_HTTP_HOSTNAME%
  echo Setting OOZIE_INSTANCE_ID:   '%OOZIE_INSTANCE_ID%'
) else (
  echo Using   OOZIE_INSTANCE_ID:   '%OOZIE_INSTANCE_ID%'
)

if not defined CATALINA_HOME (
  set CATALINA_HOME=%OOZIE_HOME%\oozie-server
  echo Setting CATALINA_HOME:   '%CATALINA_HOME%'
) else (
  echo Using   CATALINA_HOME:   '%CATALINA_HOME%'
)

if not defined CATALINA_OUT (
  set CATALINA_OUT=%OOZIE_LOG%\catalina.out
  echo Setting CATALINA_OUT:   '%CATALINA_OUT%'
) else (
  echo Using   CATALINA_OUT:   '%CATALINA_OUT%'
)

if not defined CATALINA_PID (
  set CATALINA_PID=%OOZIE_HOME%\oozie-server\temp\oozie.pid
  echo Setting CATALINA_PID:   '%CATALINA_PID%'
) else (
  echo Using   CATALINA_PID:   '%CATALINA_PID%'
)

set CATALINA_OPTS=%CATALINA_OPTS% -Dderby.stream.error.file=%OOZIE_LOG%\derby.log
