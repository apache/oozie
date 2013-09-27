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

set OOZIECPPATH=.

for  %%i IN (%BASEDIR%\lib\*.jar) do set OOZIECPPATH=!OOZIECPPATH!;%%i

set JAVA_PROPERTIES=
set OOZIE_PROPERTIES=

:copyJavaProperties
set str=%~1
if "%str:~0,2%"=="-D" (
  set JAVA_PROPERTIES=!JAVA_PROPERTIES! %str%
) else (
  goto :copyOozieProperties
)
SHIFT
goto :copyJavaProperties

:copyOozieProperties
set str=%~1
if not "%str%"=="" (
  set OOZIE_PROPERTIES=!OOZIE_PROPERTIES! %str%
) else (
  goto :startExecuting
)
SHIFT
goto :copyOozieProperties

:startExecuting

if "%JAVA_HOME%"=="" (
  set JAVA_BIN=java
) else (
  set JAVA_BIN=%JAVA_HOME%\bin\java
)

%JAVA_BIN% %JAVA_PROPERTIES% -cp %OOZIECPPATH% org.apache.oozie.cli.OozieCLI %OOZIE_PROPERTIES%
