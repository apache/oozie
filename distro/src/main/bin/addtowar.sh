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

#Creating temporary directory
function prepare() {
  tmpDir=/tmp/oozie-war-packing-$$
  rm -rf ${tmpDir}
  mkdir ${tmpDir}
  tmpWarDir=${tmpDir}/oozie-war
  mkdir ${tmpWarDir}
  checkExec "creating staging directory ${tmpDir}"
}

#cleans up temporary directory
function cleanUp() {
  if [ ! "${tmpDir}" = "" ]; then
    rm -rf ${tmpDir}    
    checkExec "deleting staging directory ${tmpDir}"
  fi
}

#check execution of command
function checkExec() {
  if [ $? -ne 0 ]
  then
    echo
    echo "Failed: $1"
    echo
    cleanUp
    exit -1;
  fi
}

#check that a file/path exists
function checkFileExists() {
  if [ ! -e ${1} ]; then
    echo
    echo "File/Dir does no exist: ${1}"
    echo
    cleanUp
    exit -1
  fi
}

#check that a file/path does not exist
function checkFileDoesNotExist() {
  if [ -e ${1} ]; then
    echo
    echo "File/Dir already exists: ${1}"
    echo
    cleanUp
    exit -1
  fi
}

#finds a file under a directory any depth, file returns in variable RET
function findFile() {
   RET=`find -H ${1} -name ${2} | grep -e "[0-9.a${hadoopJarsSuffix}].jar"`
   RET=`echo ${RET} | sed "s/ .*//"`
   if [ "${RET}" = "" ]; then
     echo
     echo "File '${2}' not found in '${1}'"
     echo
     cleanUp
     exit -1;
   fi  
}
  
function checkOption() {
  if [ "$2" = "" ]; then
    echo 
    echo "Missing option: ${1}"
    echo
    printUsage
    exit -1
  fi
}

#get the list of hadoop jars that will be injected based on the hadoop version
function getHadoopJars() {
  version=$1
  if [ "${version}" = "0.20.1" ]; then
    #List is separated by ":"
    hadoopJars="hadoop-core*.jar"
  elif [ "${version}" = "0.20.2" ]; then
    #List is separated by ":"
    hadoopJars="hadoop-core*.jar"
  elif [ "${version}" = "0.20.104" ]; then
    #List is separated by ":"
    hadoopJars="hadoop-core*.jar:jackson-core-asl-*.jar:jackson-mapper-asl-*.jar"
  elif [ "${version}" = "0.20.200" ]; then
    #List is separated by ":"
    hadoopJars="hadoop-core*.jar:jackson-core-asl-*.jar:jackson-mapper-asl-*.jar:commons-configuration-*.jar"
  elif [[ "${version}" =~ .*23 ]]; then
    suffix="-[0-9.]*"
    #List is separated by ":"
    hadoopJars="hadoop-mapreduce-client-core${suffix}.jar:hadoop-mapreduce-client-common${suffix}.jar:hadoop-mapreduce-client-jobclient${suffix}.jar:hadoop-mapreduce-client-app${suffix}.jar:hadoop-yarn-common${suffix}.jar:hadoop-yarn-api${suffix}.jar:hadoop-hdfs${suffix}.jar:hadoop-common${suffix}.jar:hadoop-auth${suffix}.jar:guava*.jar:protobuf-*.jar:avro-ipc-*.jar:jackson-core-asl-*.jar:jackson-mapper-asl-*.jar:commons-configuration-*.jar"
  elif [[ "${version}" =~ 2.* ]]; then
    suffix="-[0-9.]*"
    #List is separated by ":"
    hadoopJars="hadoop-mapreduce-client-core${suffix}.jar:hadoop-mapreduce-client-common${suffix}.jar:hadoop-mapreduce-client-jobclient${suffix}.jar:hadoop-mapreduce-client-app${suffix}.jar:hadoop-yarn-common${suffix}.jar:hadoop-yarn-api${suffix}.jar:hadoop-yarn-client${suffix}.jar:hadoop-hdfs${suffix}.jar:hadoop-common${suffix}.jar:hadoop-auth${suffix}.jar:guava*.jar:protobuf-*.jar:jackson-core-asl-*.jar:jackson-mapper-asl-*.jar:commons-configuration-*.jar:commons-cli-*.jar"
  else
    echo
    echo "Exiting: Unsupported Hadoop version '${hadoopVer}', supported versions: 0.20.1, 0.20.2, 0.20.104, 0.20.200, 0.23.x and 2.x"
    echo
    cleanUp
    exit -1;
  fi
}

function printUsage() {
  echo " Usage  : addtowar.sh <OPTIONS>"
  echo " Options: -inputwar INPUT_OOZIE_WAR"
  echo "          -outputwar OUTPUT_OOZIE_WAR"
  echo "          [-hadoop HADOOP_VERSION HADOOP_PATH]"
  echo "          [-hadoopJarsSNAPSHOT] (if Hadoop jars version on system is SNAPSHOT)"
  echo "          [-extjs EXTJS_PATH] (expanded or ZIP)"
  echo "          [-jars JARS_PATH] (multiple JAR path separated by ':')"
  echo "          [-secureWeb WEB_XML_PATH] (path to secure web.xml)"
  echo
}

if [ $# -eq 0 ]; then
  echo
  echo "Missing options"
  echo
  printUsage
  exit -1
fi

addHadoop=""
addExtjs=""
addJars=""
hadoopVersion=""
hadoopHome=""
hadoopJarsSuffix=""
extjsHome=""
jarsPath=""
inputWar=""
outputWar=""
secureWeb=false
secureWebPath=""

while [ $# -gt 0 ]
do
  if [ "$1" = "-hadoop" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Hadoop version"
      echo
      printUsage
      exit -1
    fi
    hadoopVersion=$1
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Hadoop path"
      echo
      printUsage
      exit -1
    fi
    hadoopHome=$1
    addHadoop=true
  elif [ "$1" = "-hadoopJarsSNAPSHOT" ]; then
    shift
    hadoopJarsSuffix="SNAPSHOT"
  elif [ "$1" = "-extjs" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, ExtJS path"
      echo
      printUsage
      exit -1
    fi
    extjsHome=$1
    addExtjs=true
  elif [ "$1" = "-jars" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, JARs path"
      echo
      printUsage
      exit -1
    fi
    jarsPath=$1
    addJars=true
  elif [ "$1" = "-inputwar" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Input Oozie WAR path"
      echo
      printUsage
      exit -1
    fi
    inputWar=$1
  elif [ "$1" = "-outputwar" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, Output Oozie WAR path"
      echo
      printUsage
      exit -1
    fi
    outputWar=$1
  elif [ "$1" = "-secureWeb" ]; then
    shift
    if [ $# -eq 0 ]; then
      echo
      echo "Missing option value, secure web.xml path"
      echo
      printUsage
      exit -1
      fi
    secureWebPath=$1
    secureWeb=true
  fi
    shift
done

if [ "${addHadoop}${addExtjs}${addJars}" == "" ]; then
  echo
  echo "Nothing to do"
  echo
  printUsage
  exit -1
fi

prepare

checkOption "-inputwar" ${inputWar}
checkOption "-outputwar" ${outputWar} 
checkFileExists ${inputWar}
checkFileDoesNotExist ${outputWar}

if [ "${addHadoop}" = "true" ]; then
  checkFileExists ${hadoopHome}
  getHadoopJars ${hadoopVersion}
fi
  
if [ "${addExtjs}" = "true" ]; then
  checkFileExists ${extjsHome}
fi

if [ "${addJars}" = "true" ]; then
    for jarPath in ${jarsPath//:/$'\n'}
    do
      checkFileExists ${jarPath}
    done
fi

if [ "${secureWeb}" = "true" ]; then
  checkFileExists ${secureWebPath}
  echo
  echo "Using SSL (HTTPS)"
  echo
fi

#Unpacking original war
unzip ${inputWar} -d ${tmpWarDir} > /dev/null
checkExec "unzipping Oozie input WAR"

components=""

if [ "${addHadoop}" = "true" ]; then
  components="Hadoop JARs";
  found=`ls ${tmpWarDir}/WEB-INF/lib/hadoop*core*jar 2> /dev/null | wc -l`
  checkExec "looking for Hadoop JARs in input WAR"
  if [ ! $found = 0 ]; then
    echo
    echo "Specified Oozie WAR '${inputWar}' already contains Hadoop JAR files"
    echo
    cleanUp
    exit -1
  fi  
  ## adding hadoop
    echo "Injecting following Hadoop JARs"
    echo
    for jar in ${hadoopJars//:/$'\n'}
    do
      findFile ${hadoopHome} ${jar}
      jar=${RET}
      echo ${jar}
      cp ${jar} ${tmpWarDir}/WEB-INF/lib/
      checkExec "copying jar ${jar} to staging"
    done
fi

if [ "${addExtjs}" = "true" ]; then
  if [ ! "${components}" = "" ];then
    components="${components}, "
  fi
  components="${components}ExtJS library"
  if [ -e ${tmpWarDir}/ext-2.2 ]; then
    echo
    echo "Specified Oozie WAR '${inputWar}' already contains ExtJS library files"
    echo
    cleanUp
    exit -1
  fi
  #If the extjs path given is a ZIP, expand it and use it from there
  if [ -f ${extjsHome} ]; then
    unzip ${extjsHome} -d ${tmpDir} > /dev/null
    extjsHome=${tmpDir}/ext-2.2
  fi
  #Inject the library in oozie war
  cp -r ${extjsHome} ${tmpWarDir}/ext-2.2
  checkExec "copying ExtJS files into staging"
fi

if [ "${addJars}" = "true" ]; then
  if [ ! "${components}" = "" ];then
    components="${components}, "
  fi
  components="${components}JARs"

  for jarPath in ${jarsPath//:/$'\n'}
  do
    found=`ls ${tmpWarDir}/WEB-INF/lib/${jarPath} 2> /dev/null | wc -l`
    checkExec "looking for JAR ${jarPath} in input WAR"
    if [ ! $found = 0 ]; then
      echo
      echo "Specified Oozie WAR '${inputWar}' already contains JAR ${jarPath}"
      echo
      cleanUp
      exit -1
    fi
    cp ${jarPath} ${tmpWarDir}/WEB-INF/lib/
    checkExec "copying jar ${jarPath} to staging"
  done
fi

if [ "${secureWeb}" = "true" ]; then
  #Inject the SSL version of web.xml in oozie war
  cp ${secureWebPath} ${tmpWarDir}/WEB-INF/web.xml
  checkExec "Injecting secure web.xml file into staging"
fi

#Creating new Oozie WAR
currentDir=`pwd`
cd ${tmpWarDir}
zip -r oozie.war * > /dev/null
checkExec "creating new Oozie WAR"
cd ${currentDir}

#copying new Oozie WAR to asked location
cp ${tmpWarDir}/oozie.war ${outputWar}
checkExec "copying new Oozie WAR"

echo 
echo "New Oozie WAR file with added '${components}' at ${outputWar}"
echo

cleanUp
exit 0
