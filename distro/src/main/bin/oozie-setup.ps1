### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

param(
[Parameter(Mandatory=$true,Position = 0)] [string] $Command="",
[string] $D="",
[switch] $Secure,
[Parameter(ValueFromRemainingArguments = $true)][string[]]$args)

# FUNCTIONS

Function PrintUsage { param ()
    Write-Output  "Usage  : oozie-setup.ps1 COMMAND [OPTIONS]"
    Write-Output  "          prepare-war [-d directory] [-secure] (-d identifies an alternative directory for processing jars"
    Write-Output  "                                                -secure will configure the war file to use HTTPS (SSL))"
    Write-Output  "          sharelib create -fs FS_URI [-locallib SHARED_LIBRARY] (create sharelib for oozie,"
    Write-Output  "                                                                FS_URI is the fs.default.name"
    Write-Output  "                                                                for hdfs uri; SHARED_LIBRARY, path to the"
    Write-Output  "                                                                Oozie sharelib to install, it can be a tarball"
    Write-Output  "                                                                or an expanded version of it. If ommited,"
    Write-Output  "                                                                the Oozie sharelib tarball from the Oozie"
    Write-Output  "                                                                installation directory will be used)"
    Write-Output  "                                                                (action failes if sharelib is already installed"
    Write-Output  "                                                                in HDFS)"
    Write-Output  "          sharelib upgrade -fs FS_URI [-locallib SHARED_LIBRARY] ([deprecated]"
    Write-Output  "                                                                  [use create command to create new version]"
    Write-Output  "                                                                  upgrade existing sharelib, fails if there"
    Write-Output  "                                                                  is no existing sharelib installed in HDFS)"
    Write-Output  "          db create|upgrade|postupgrade -run [-sqlfile <FILE>] (create, upgrade or postupgrade oozie db with an"
    Write-Output  "                                                                optional sql File)"
    Write-Output  "         EXTJS can be downloaded from http://www.extjs.com/learn/Ext_Version_Archives"
}

function Expand-ZIPFile($file, $destination){
    $shell = new-object -com shell.application
    $zip = $shell.NameSpace($file)
    foreach($item in $zip.items()){
        $shell.Namespace($destination).copyhere($item)
    }
}

# MAIN()

# The script will terminate if any steps fail
$ErrorActionPreference = "Stop"

# Constants
$EXT_SUBDIR = "ext-2.2"
$OOZIE_HOME = (Split-Path $MyInvocation.MyCommand.Path) + "\.."
$OOZIE_HOME = Resolve-Path $OOZIE_HOME

$CATALINA_BASE = ""
if ($env:CATALINA_BASE){
    $CATALINA_BASE = "$env:CATALINA_BASE"
}else{
    $CATALINA_BASE = "$OOZIE_HOME\oozie-server"
}

# Finds JAR.EXE and Java
$JAR_EXE=""
$JAVA_BIN=""
if ($env:JAVA_HOME) {
    $JAR_EXE = "$env:JAVA_HOME\bin\jar.exe"
    $JAVA_BIN = "$env:JAVA_HOME\bin\java.exe"
} else {
    Write-Output "WARN: JAVA_HOME not defined. oozie-setup.ps1 will relay on the PATH environment variable to use JAR.exe"
    $JAR_EXE = "jar.exe"
    $JAVA_BIN = "java.exe"
}

if (($Command -eq "sharelib") -Or ($Command -eq "db")) {
          $OOZIE_OPTS="-Doozie.home.dir=$OOZIE_HOME";
          $OOZIE_OPTS="$OOZIE_OPTS -Doozie.config.dir=$OOZIE_HOME\conf";
          $OOZIE_OPTS="$OOZIE_OPTS -Doozie.log.dir=$OOZIE_HOME\log";
          $OOZIE_OPTS="$OOZIE_OPTS -Doozie.data.dir=$OOZIE_HOME\data";
          $OOZIE_OPTS="$OOZIE_OPTS -Dderby.stream.error.file=$OOZIE_HOME\log\derby.log"

          $OOZIECPPATH=""
          $OOZIECPPATH="$OOZIE_HOME\libtools\*;$OOZIE_HOME\lib\*;$OOZIE_HOME\..\extra_libs\*"

          $COMMAND_OPTS=[string]$args

          if ($Command -eq "sharelib") {
            Start-Process $JAVA_BIN -ArgumentList "$OOZIE_OPTS -cp $OOZIECPPATH org.apache.oozie.tools.OozieSharelibCLI $COMMAND_OPTS" -Wait -NoNewWindow
          } elseif ($Command -eq "db") {
            Start-Process $JAVA_BIN -ArgumentList "$OOZIE_OPTS -cp $OOZIECPPATH org.apache.oozie.tools.OozieDBCLI $COMMAND_OPTS" -Wait -NoNewWindow
          }
          exit 0
}elseif ($Command -eq "prepare-war"){

    $InputWar = "$OOZIE_HOME\oozie.war"
    $OutputWar = "$OOZIE_HOME\oozie-server\webapps\oozie.war"
    $SecureConfigsDir="$CATALINA_BASE\conf\ssl"
    $ExtraLibs = Resolve-Path "$OOZIE_HOME\..\extra_libs"
    $EXTJS = "$ExtraLibs\ext-2.2.zip"

    # Validates the input\output wars
    if (!(Test-Path $InputWar)){
        PrintUsage
        throw "Path '$InputWar' doesn't exist"
    }
    if (!$InputWar.ToLower().EndsWith(".war")){
        PrintUsage
        throw "Invalid input war file '$InputWar'"
    }
    if (!$OutputWar.ToLower().EndsWith(".war")){
        PrintUsage
        throw "Invalid input war file '$OutputWar'"
    }
    if ($OutputWar -ieq $InputWar){
        PrintUsage
        throw "Invalid output\input war file. Both parameters cannot be the same file"
    }
    # Deletes previous output wars.
    if (Test-Path $OutputWar){
        Write-Output "Deleting existing output .war '$OutputWar'"
        Remove-Item -Force -Path $OutputWar
    }

    # Selects\Creates the temp directory
    $OOZIE_TEMP = "$OOZIE_HOME\temp"
    $OOZIE_WEB_INF_LIB = "$OOZIE_TEMP\WEB-INF\lib"

    Write-Output "Creating OOZIE_TEMP directory '$OOZIE_TEMP'"
    if (Test-Path "$OOZIE_TEMP") { Remove-Item "$OOZIE_TEMP" -Force -Recurse }
    $x = New-Item "$OOZIE_TEMP" -type directory

    # Extract the InputWar
    pushd $OOZIE_TEMP
    cmd /c $JAR_EXE xvf $InputWar
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to execute 'jar xvf'. Error ($LASTEXITCODE)"
    }
    popd
    # Copy EXT_JS files
    if ((Test-Path $EXTJS)) {
        $EXTJS_HOME = "$ExtraLibs\ext-2.2"

        if (Test-Path "$EXTJS_HOME") { Remove-Item "$EXTJS_HOME" -Force -Recurse }
        $x = New-Item "$EXTJS_HOME" -type directory

        Expand-ZIPFile -File $EXTJS -Destination $EXTJS_HOME
        cp -r "$EXTJS_HOME\ext-2.2" "$OOZIE_TEMP"
    }else{
        Write-Output "INFO: Oozie webconsole disabled, ExtJS library not specified"
    }

    # Copy additional Jars
    if ($D -ne "") {
        $ExtraLibs = $D
    }
    Write-Output "   Adding JarFiles: $ExtraLibs\*.jar"
    cp -r $ExtraLibs\*.jar $OOZIE_WEB_INF_LIB

    if ($Secure) {
        #Use the SSL version of server.xml in oozie-server
        if (Test-Path $SecureConfigsDir\ssl-server.xml){
            cp $SecureConfigsDir\ssl-server.xml $CATALINA_BASE\conf\server.xml
        }

        #Inject the SSL version of web.xml in oozie war
        if (Test-Path $SecureConfigsDir\ssl-web.xml){
            cp $SecureConfigsDir\ssl-web.xml $OOZIE_TEMP\WEB-INF\web.xml
        }

        Write-Output "INFO: Using secure server.xml and secure web.xml"
    }else{
        #Use the regular version of server.xml in oozie-server
        if (Test-Path $SecureConfigsDir\server.xml){
            cp $SecureConfigsDir\server.xml $CATALINA_BASE\conf\server.xml
        }
        #No need to restore web.xml because its already in the original WAR file
    }

    Write-Output "Recreating the new war file '$OutputWar'..."

    cmd /c $JAR_EXE cvf $OutputWar -C $OOZIE_TEMP '.'
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to execute 'jar uvf'. Error ($LASTEXITCODE)"
    }

    Write-Output "Done! $counter files added"
} else {
    PrintUsage
    exit -1
}
