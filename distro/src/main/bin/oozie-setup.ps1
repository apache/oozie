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
[Parameter(Mandatory=$true)] [string] $InputWar,
[Parameter(Mandatory=$true)] [string] $OutputWar,
[string[]] $Jars,
[string] $HadoopVersion,
[string] $HadoopHome,
[string] $ExtJS)

# FUNCTIONS

Function GetHadoopJars { param (
    [Parameter(Mandatory=$true)] [string] $HadoopVersion,
    [Parameter(Mandatory=$true)] [string] $HadoopHome)
    if ($HadoopVersion -ieq "0.20.1")
    {
        $jarsList = "$hadoopHome\hadoop*core*.jar"
    }
    elseif ($HadoopVersion -ieq "0.20.2")
    {
        $jarsList = "$hadoopHome\hadoop*core*.jar"
    }
    elseif ($HadoopVersion -ieq "0.20.104")
    {
        $jarsList =
            "$hadoopHome\hadoop*core*.jar",
            "$hadoopHome\lib\jackson-core-asl*.jar",
            "$hadoopHome\lib\jackson-mapper-asl-*.jar"
    }
    else
    {
        $jarsList =
            "$hadoopHome\hadoop-core-$HadoopVersion.jar",
            "$hadoopHome\lib\jackson-core-asl*.jar",
            "$hadoopHome\lib\jackson-mapper-asl-*.jar",
            "$hadoopHome\lib\commons-configuration-*.jar"
    }
    $hadoopJars = @()
    $jarsList | % { if ((Test-Path $_)) {
        $hadoopJars += $_
        }
        else {
            Write-Output "Unable to find Hadoop Jar '$_'"
        }}
    $hadoopJars
}

Function PrintUsage { param ()
    Write-Output  "Usage  : oozie-setup.ps1 [OPTIONS]"
    Write-Output  "         [-ExtJS EXTJS_PATH] (expanded or ZIP, to enable the Oozie webconsole)"
    Write-Output  "         [-HadoopHome HADOOP_PATH -HadoopVersion HADOOP_VERSION] (Hadoop version [1.2.0-SNAPSHOT])"
    Write-Output  "                (Hadoop version [1.2.0-SNAPSHOT])"
    Write-Output  "         [-jars [JAR_PATH, ... ] ] "
    Write-Output  "         EXTJS can be downloaded from http://www.extjs.com/learn/Ext_Version_Archives"
}

# MAIN()

# The script will terminate if any steps fail
$ErrorActionPreference = "Stop"

# Constants
$EXT_SUBDIR = "ext-2.2"
$HADOOP_DEFAULT_VERSION = "1.2.0-SNAPSHOT"

# Finds JAR.EXE

$JAR_EXE=""
if ($env:JAVA_HOME) {
    $JAR_EXE = "$env:JAVA_HOME\bin\jar.exe"
} else {
    Write-Output "WARN: JAVA_HOME not defined. oozie-setup.ps1 will rely on the PATH environment variable to use JAR.exe"
    $JAR_EXE = "jar.exe"
}

# Validates that both/neither $HadoopHome and $HadoopVersion are provided
if ($HadoopHome -and !$HadoopVersion) {
    PrintUsage
    throw "Need to specify -HadoopVersion for -HadoopHome='$HadoopHome'"
}
if ($HadoopVersion -and !$HadoopHome) {
    PrintUsage
    throw "Need to specify -HadoopHome for -HadoopVersion='$HadoopHome'"
}

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
$OOZIE_HOME = (Split-Path $MyInvocation.MyCommand.Path) + "\.."
$OOZIE_HOME = Resolve-Path $OOZIE_HOME
$OOZIE_TEMP = "$OOZIE_HOME\temp"
$OOZIE_WEB_INF_LIB = "$OOZIE_TEMP\WEB-INF\lib"

Write-Output "Creating OOZIE_TEMP directory '$OOZIE_TEMP'"
if (Test-Path "$OOZIE_TEMP") { Remove-Item "$OOZIE_TEMP" -Force -Recurse }
$x = New-Item "$OOZIE_WEB_INF_LIB" -type directory

# Creates the new OutputWar
$x = Copy-Item $InputWar $OutputWar

# Copy hadoop files
if ($HadoopVersion -or $HadoopHome) {
    Write-Output "Extracting files from path '$HadoopHome' from version '$HadoopVersion'"
    if (!(Test-Path $HadoopHome)) { throw "Unable to find Hadoop Home '$HadoopHome'" }
    $HadoopFiles = GetHadoopJars -HadoopVersion $HadoopVersion -HadoopHome $HadoopHome
    $HadoopFiles | % {
        Write-Output "   Adding HadoopFiles: $_"
        Copy-Item $_ $OOZIE_WEB_INF_LIB -force
    }
}

# Copy EXT_JS files
if ($ExtJS) {
    Write-Output "ExtJS not currently supported!"
}

# Copy additional Jars
$Jars | % {
    Write-Output "   Adding JarFiles: $_"
    Copy-Item $_ $OOZIE_WEB_INF_LIB -force
}

$counter = (Get-ChildItem $OOZIE_WEB_INF_LIB).Length
IF ($counter -gt 0) {
    Write-Output "Adding files to the war file '$OutputWar'..."

    "$JAR_EXE"

    cmd /c $JAR_EXE uvf $OutputWar -C $OOZIE_TEMP WEB-INF\lib


    if ($LASTEXITCODE -ne 0) {
        throw "Unable to execute 'jar uvf'. Error ($LASTEXITCODE)"
    }

    Write-Output "Done! $counter files added"
}
