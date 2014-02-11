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

@echo off

set argC=0
for %%x in (%*) do Set /A argC+=1

if %argC% == 0 (
  echo "Usage: oozied.cmd (start|stop|run) [<catalina-args...>]"
  exit 1
)

set actionCmd=%1

set BASEDIR=%~dp0
set BASEDIR=%BASEDIR%\..
call %BASEDIR%\bin\oozie-sys.cmd


set CATALINA=%BASEDIR%\oozie-server\bin\catalina.bat

if "%actionCmd%" == "start" goto setup_catalina_opts
if "%actionCmd%" == "run" goto setup_catalina_opts

goto exec_catalina

:setup
    echo "WARN: Oozie WAR has not been set up at '%CATALINA_BASE%\webapps', doing default set up"
    call %BASEDIR%\bin\oozie-setup.cmd
    if errorlevel 1(
      exit /b %errorlevel%
    )
    goto :EOF

:setup_catalina_opts
  @REM The Java System properties 'oozie.http.port' and 'oozie.https.port' are not
  @REM used by Oozie, they are used in Tomcat's server.xml configuration file

  echo "Using CATALINA_OPTS:  %CATALINA_OPTS%"
  echo "OOZIE_HOME" %OOZIE_HOME%
  set catalina_opts_tmp=-Doozie.home.dir=%OOZIE_HOME%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.config.dir=%OOZIE_CONFIG%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.log.dir=%OOZIE_LOG%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.data.dir=%OOZIE_DATA%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.instance.id=%OOZIE_INSTANCE_ID%

  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.config.file=%OOZIE_CONFIG_FILE%

  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.log4j.file=%OOZIE_LOG4J_FILE%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.log4j.reload=%OOZIE_LOG4J_RELOAD%

  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.http.hostname=%OOZIE_HTTP_HOSTNAME%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.http.port=%OOZIE_HTTP_PORT%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.https.port=%OOZIE_HTTPS_PORT%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.base.url=%OOZIE_BASE_URL%

  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.https.keystore.file=%OOZIE_HTTPS_KEYSTORE_FILE%
  set catalina_opts_tmp=%catalina_opts_tmp% -Doozie.https.keystore.pass=%OOZIE_HTTPS_KEYSTORE_PASS%

  # add required native libraries such as compression codecs
  set catalina_opts_tmp=%catalina_opts_tmp% -Djava.library.path=%JAVA_LIBRARY_PATH%
  echo "Adding to CATALINA_OPTS:     %catalina_opts_tmp%"
  set CATALINA_OPTS=%CATALINA_OPTS% %catalina_opts_tmp%
  echo CATALINA_OPTS:     %CATALINA_OPTS%

:setup_oozie
  if NOT EXIST "%CATALINA_BASE%\webapps\oozie.war" call setup


:exec_catalina
  ECHO EXECUTING CATALINA "%CATALINA% %actionCmd%"
  call %CATALINA% %actionCmd%
  if errorlevel 1 (
    echo
    echo "ERROR: Oozie %actionCmd% aborted"
    echo
    exit /b %errorlevel%
  ) else (
    echo
    echo "Oozie %actionCmd% succeeded"
    echo
  )
