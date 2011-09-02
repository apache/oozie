#!/bin/sh

# To run build in hudson, the default env variables should be defined in the environment. If not, please do the exports before
# invoking this script.
# export WORKSPACE=PATH_TO_WORKSPACE
# export TOOLS_HOME=PATH_TO_COMMON_LIBRAARY_HOME
# export OOZIE_GIT_REPO=git://github.com/yahoo/oozie.git

# You can also explicitly change JAVA_HOME or M3_HOME below.

export JAVA_HOME=${TOOLS_HOME}/java/latest
export M3_HOME=${TOOLS_HOME}/maven/apache-maven-3.0.2
export PATH=$JAVA_HOME/bin:$M3_HOME/bin:$PATH

#Please uncomment this line if the git repo needs to download
#git clone -o origin $OOZIE_GIT_REPO $WORKSPACE

rm -fr ~/.m2

bin/mkdistro.sh $1