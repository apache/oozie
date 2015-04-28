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
setlocal enabledelayedexpansion

set BASEDIR=%~dp0..
pushd %BASEDIR%
set BASEDIR=%cd%
popd

set OOZIE_HOME=%BASEDIR%
set OOZIE_CONFIG=%OOZIE_HOME%\conf
set OOZIE_LOG=%OOZIE_HOME%\logs
set OOZIE_DATA=%OOZIE_HOME%\data

@rem Verify the \conf directory is available
if not exist %OOZIE_CONFIG%\NUL (
  echo.
  echo."ERROR: Oozie configuration directory could not be found at %OOZIE_CONFIG%"
  echo.
  exit 1
)

@rem Check if oozie-env.cmd is provided under \bin and import user defined
@rem settings defined in it if it exists
if exist "%OOZIE_HOME%\bin\oozie-env.cmd" (
  call "%OOZIE_HOME%\bin\oozie-env.cmd"
)
if exist "%OOZIE_CONFIG%\oozie-env.cmd" (
  call "%OOZIE_CONFIG%\oozie-env.cmd"
)

@rem Set Oozie Options
set OOZIEDB_OPTS=-Doozie.home.dir=%OOZIE_HOME%
set OOZIEDB_OPTS=%OOZIEDB_OPTS% -Doozie.config.dir=%OOZIE_CONFIG%
set OOZIEDB_OPTS=%OOZIEDB_OPTS% -Doozie.log.dir=%OOZIE_LOG%
set OOZIEDB_OPTS=%OOZIEDB_OPTS% -Doozie.data.dir=%OOZIE_DATA%

@rem Set Classpath to load oozie dependencies
set OOZIECPPATH=.

@rem Add libtools to the classpath
set OOZIECPPATH=%OOZIECPPATH%;%BASEDIR%\libtools\*
@rem Add lib to the classpath
set OOZIECPPATH=%OOZIECPPATH%;%BASEDIR%\lib\*
@rem Add extra_libs to the classpath
set OOZIECPPATH=%OOZIECPPATH%;%BASEDIR%\..\extra_libs\*

@rem Set JAVA_BIN based on JAVA_HOME
if "%JAVA_HOME%"=="" (
    set JAVA_BIN=java
) else (
    set JAVA_BIN=%JAVA_HOME%\bin\java
)

%JAVA_BIN% %JAVA_PROPERTIES% %OOZIEDB_OPTS% -classpath %OOZIECPPATH% org.apache.oozie.tools.OozieDBCLI %*
