#!/bin/bash -l

# fail hard on errors
set -e

# find this script and establish base directory
SCRIPT_DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd "$SCRIPT_DIR" &> /dev/null
MY_DIR=`pwd`
echo "[INFO] Executing in ${MY_DIR}"

# switch to java 7 for javawizard, enable mvn
echo "CURRENT JAVA_HOME:$JAVA_HOME"
export M2_HOME=/opt/mvn3
export JAVA_HOME=/opt/sapjvm_7
export PATH=$M2_HOME/bin:$JAVA_HOME/bin:$PATH
mvn -version

ALTISCALE_RELEASE=${ALTISCALE_RELEASE:-4.3.0}
HADOOP_VERSION=${HADOOP_VERSION:-2.7.2}
HIVE_VERSION=${HIVE_VERSION:-1.2.1}
OOZIE_VERSION=${SQOOP_VERSION:-4.2.0}
PIG_VERSION=${PIG_VERSION:-0.15.0}
SQOOP_VERSION=${SQOOP_VERSION:-1.4.6}

if [[ -z $HADOOP_VERSION  ||  -z $PIG_VERSION || -z $HIVE_VERSION || -z $SQOOP_VERSION ]]; then
   echo "[ERROR] HADOOP_VERSION, PIG_VERSION and HIVE_VERSION must be explicitly set in the environment"
   exit 1
fi

echo "[INFO] mvn version"
mvn -version
echo ""

echo "Current Directory: $(pwd)"
echo ""

# Extraction and repackaging of nexus tomcat zip into tomcat tar.gz that the distro module requires
export TOMCAT_VERSION=6.0.44
echo "Extraction and repackaging of Nexus Apache Tomcat ${TOMCAT_VERSION} zip into tar.gz file that the distro module requires"
echo ""

DISTRO_DOWNLOADS_DIR=${MY_DIR}/distro/downloads
EXTRACTION_DIR=${DISTRO_DOWNLOADS_DIR}/tomcat

echo "[INFO] mkdir -p --mode=0755 ${EXTRACTION_DIR}"
mkdir --mode=0755 -p ${EXTRACTION_DIR}
echo ""

echo "[INFO] cp /imports/apache-tomcat-${TOMCAT_VERSION}.zip ${EXTRACTION_DIR}"
cp /imports/apache-tomcat-${TOMCAT_VERSION}.zip ${EXTRACTION_DIR}
echo ""

echo "pushd ${EXTRACTION_DIR}"
pushd ${EXTRACTION_DIR}
echo ""

# Extract Apache Tomcat from the Nexus zip file
echo "unzip apache-tomcat-${TOMCAT_VERSION}.zip in $(pwd)"
unzip apache-tomcat-${TOMCAT_VERSION}.zip
rm apache-tomcat-${TOMCAT_VERSION}.zip
echo ""

# Repackage Apache tomcat into the tar.gz file expected by oozie distro build
echo "tar -czvf ${DISTRO_DOWNLOADS_DIR}/tomcat-${TOMCAT_VERSION}.tar.gz ."
echo ""
tar -czvf ${DISTRO_DOWNLOADS_DIR}/tomcat-${TOMCAT_VERSION}.tar.gz .
echo ""

# Remove the extraction directory
echo "rm -rf ${EXTRACTION_DIR}"
rm -rf ${EXTRACTION_DIR}
echo ""

echo "Running popd"
popd
echo ""

echo "Current Directory: $(pwd)"

echo "[INFO] Building oozie ${OOZIE_VERSION} client and server"

mvn package assembly:single versions:set -DnewVersion=${OOZIE_VERSION} -DincludeHadoopJars=true -DskipTests=true -Phadoop-2 -Dhadoop.version=${HADOOP_VERSION} -Dpig.version=${PIG_VERSION} -Dhive.version=${HIVE_VERSION} -Dhadoop.auth.version=${HADOOP_VERSION} -Dhcatalog.version=${HIVE_VERSION} -Dsqoop.version=${SQOOP_VERSION} -Dtomcat.version=$TOMCAT_VERSION

echo "[INFO] Finished building oozie ${OOZIE_VERSION}"
echo ""

echo "Current Directory: $(pwd)"

# Print out the oozie tar.gz files
echo "[INFO] find . -name *.tar.gz -print"
find . -name *.tar.gz -print
echo ""

INSTALL_DIR="$MY_DIR/oozieclientrpmbuild"
echo "[INFO] mkdir -p --mode=0755 ${INSTALL_DIR}"
mkdir --mode=0755 -p ${INSTALL_DIR}

export DEST_DIR=${INSTALL_DIR}/opt
echo "[INFO] mkdir -p --mode=0755 ${DEST_DIR}"
echo ""
mkdir -p --mode=0755 ${DEST_DIR}

cd ${DEST_DIR}
echo "[INFO] tar -xvzpf ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/oozie-client-${OOZIE_VERSION}.tar.gz in $(pwd)"
tar -xvzpf ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/oozie-client-${OOZIE_VERSION}.tar.gz
echo ""

echo "[INFO] cp -r  ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/conf/ oozie-client-${OOZIE_VERSION}"
cp -r  ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/conf/ oozie-client-${OOZIE_VERSION}

echo "[INFO] cp ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/oozie-examples.tar.gz oozie-client-${OOZIE_VERSION}"
cp ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/oozie-examples.tar.gz oozie-client-${OOZIE_VERSION}

DATE_STRING=`date +%Y%m%d%H%M%S`
GIT_REPO="https://github.com/Altiscale/oozie"

export RPM_NAME=`echo alti-oozie-client-${OOZIE_VERSION}`
export RPM_DESCRIPTION="Apache Oozie Client ${OOZIE_VERSION}\n\n${DESCRIPTION}"
export RPM_DIR="${RPM_DIR:-"${INSTALL_DIR}/oozie-client-artifact/"}"
mkdir --mode=0755 -p ${RPM_DIR}

echo ""
echo "Current Directory: $(pwd)"

echo ""
echo "[INFO] Packaging oozie client rpm ${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm" 

# Make the Client RPM
cd ${RPM_DIR}

fpm --verbose \
--maintainer ops@altiscale.com \
--vendor Altiscale \
--provides ${RPM_NAME} \
-s dir \
-t rpm \
-n ${RPM_NAME} \
-v ${ALTISCALE_RELEASE} \
--epoch 1 \
--description "${DESCRIPTION}" \
--iteration ${DATE_STRING} \
--rpm-user root \
--rpm-group root \
-C ${INSTALL_DIR} \
opt

echo "[INFO] Finished packaging oozie client rpm ${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm" 
echo ""

echo "[INFO] find . -name *.rpm -print in $(pwd)"
find . -name *.rpm -print
echo ""

echo "[INFO] cp ${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm ${RPM_DIR}alti-oozie-client-${XMAKE_PROJECT_VERSION}.rpm"
cp "${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm" "${RPM_DIR}alti-oozie-client-${XMAKE_PROJECT_VERSION}.rpm"
echo ""

# Make the Server RPM
echo "[INFO] Making oozie server rpm"
INSTALL_DIR="$MY_DIR/oozieserverrpmbuild"
echo "[INFO] mkdir -p --mode=0755 ${INSTALL_DIR}"
mkdir --mode=0755 -p ${INSTALL_DIR}

export DEST_DIR=${INSTALL_DIR}/opt
rm -rf ${DEST_DIR}
echo "[INFO] mkdir -p --mode=0755 ${DEST_DIR}"
mkdir -p --mode=0755 ${DEST_DIR}
echo ""

cd ${DEST_DIR}
echo "[INFO] tar -xvzpf ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro.tar.gz into ${DEST_DIR}"
tar -xvzpf ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro.tar.gz
echo ""

export RPM_NAME=`echo alti-oozie-server-${OOZIE_VERSION}`
export RPM_DESCRIPTION="Apache Oozie Server ${OOZIE_VERSION}\n\n${DESCRIPTION}"
export RPM_DIR="${INSTALL_DIR}/oozie-server-artifact/"
echo "[INFO] mkdir -p --mode=0755 ${RPM_DIR}"
mkdir -p --mode=0755 ${RPM_DIR}

export RPM_BUILD_DIR="${INSTALL_DIR}/opt/oozie-${OOZIE_VERSION}"
echo "[INFO] mkdir -p --mode=0755 ${RPM_BUILD_DIR}"
mkdir -p  --mode=0755 ${RPM_BUILD_DIR}
echo "[INFO] mkdir -p --mode=0775 ${RPM_BUILD_DIR}/libext"
mkdir -p --mode=0775 ${RPM_BUILD_DIR}/libext
echo ""

echo "[INFO] cp ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/oozie.war ${RPM_BUILD_DIR}"
cp ${MY_DIR}/distro/target/oozie-${OOZIE_VERSION}-distro/oozie-${OOZIE_VERSION}/oozie.war ${RPM_BUILD_DIR} 

cd ${RPM_BUILD_DIR}/libext
echo "[INFO] cp /imports/sap-ext-js-2.2.0.0.zip ${RPM_BUILD_DIR}/libext/ext-2.2.zip"
cp /imports/sap-ext-js-2.2.0.0.zip ./ext-2.2.zip

cd ${RPM_BUILD_DIR}/conf
rm -rf hadoop-conf
ln -s /etc/hadoop hadoop-conf

cd ${RPM_BUILD_DIR}/libtools
ln -s /opt/mysql-connector/mysql-connector.jar mysql-connector.jar

cd ${RPM_BUILD_DIR}/oozie-server/lib
ln -s /opt/mysql-connector/mysql-connector.jar mysql-connector.jar

cd ${RPM_BUILD_DIR}/bin
echo "[INFO] cp /src/ext/oozie-status.sh ${RPM_BUILD_DIR}" 
cp /src/ext/oozie-status.sh .
chmod 755 oozie-status.sh

cd ${INSTALL_DIR}
find opt/oozie-${OOZIE_VERSION} -type d -print | awk '{print "/" $1}' > /tmp/$$.files
export DIRECTORIES=""
for i in `cat /tmp/$$.files`; do DIRECTORIES="--directories $i ${DIRECTORIES} "; done
export DIRECTORIES
rm -f /tmp/$$.files
echo ""

echo "[INFO] mkdir --mode=0755 -p ${INSTALL_DIR}/etc/oozie"
mkdir --mode=0755 -p ${INSTALL_DIR}/etc/oozie
echo ""

echo "Current Directory: $(pwd)"
echo ""

cd ${RPM_DIR}
echo "[INFO] Packaging oozie server rpm ${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm" 
fpm --verbose \
-C ${INSTALL_DIR} \
--maintainer ops@altiscale.com \
--vendor Altiscale \
--provides ${RPM_NAME} \
--depends alti-mysql-connector \
-s dir \
-t rpm \
-n ${RPM_NAME} \
-v ${ALTISCALE_RELEASE} \
${DIRECTORIES} \
--description "${DESCRIPTION}" \
--iteration ${DATE_STRING} \
--rpm-user oozie \
--rpm-group hadoop \
opt

echo "[INFO] Finished packaging oozie server rpm ${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm"
echo ""

# We are unlinking the symbolic links that point to files that are not in the rpm.
# This is because xmake docker tries to do a chown -R on the distribution files.
# This command fails on symbolic links that point at files that are not in the distribution.

echo "[INFO] unlink ${RPM_BUILD_DIR}/conf/hadoop-conf"

unlink ${RPM_BUILD_DIR}/conf/hadoop-conf
echo "[INFO] unlink ${RPM_BUILD_DIR}/libtools/mysql-connector.jar"
unlink ${RPM_BUILD_DIR}/libtools/mysql-connector.jar

echo "[INFO] unlink ${RPM_BUILD_DIR}/oozie-server/lib/mysql-connector.jar"
unlink ${RPM_BUILD_DIR}/oozie-server/lib/mysql-connector.jar
echo ""

echo "[INFO] cp ${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm ${RPM_DIR}alti-oozie-server-${XMAKE_PROJECT_VERSION}.rpm"
cp "${RPM_DIR}${RPM_NAME}-${ALTISCALE_RELEASE}-${DATE_STRING}.x86_64.rpm" "${RPM_DIR}alti-oozie-server-${XMAKE_PROJECT_VERSION}.rpm"
echo ""
