Oozie, Yahoo Workflow Engine for Hadoop.

PLEASE NOTE:

 * Yahoo! does not offer any support for the
   Yahoo! Distribution of Hadoop.

 * This distribution includes cryptographic software that
   is subject to U.S. export control laws and applicable
   export and import laws of other countries. BEFORE using
   any software made available from this site, it is your
   responsibility to understand and comply with these laws.
   This software is being exported in accordance with the
   Export Administration Regulations. As of June 2009, you
   are prohibited from exporting and re-exporting this
   software to Cuba, Iran, North Korea, Sudan, Syria and
   any other countries specified by regulatory update to
   the U.S. export control laws and regulations. Diversion
   contrary to U.S. law is prohibited.

--------------------------------------

Requirements for building and testing Oozie:

* Java 6+
* Apache Maven 2.2.0
* Hadoop 0.20+
* Pig 0.6+

--------------------------------------

Initial Maven setup:

$ build-setup/setup-maven.sh


This script installs a modified Doxia documentation plugin with better twiki support.

This has to be run only once.

--------------------------------------

Building a Oozie distro for Apache Hadoop 0.20.2:

$ bin/mkdistro.sh -DskipTests

This distribution of Oozie uses HSQL as a database.

The options for using MySQL can be used together with this option.

--------------------------------------

Building a Oozie distro for Yahoo Hadoop 0.20.104.1 (Security version):


$ bin/mkdistro.sh -DskipTests -DhadoopGroupId=com.yahoo.hadoop \
  -DhadoopVersion=0.20.104.1 -Doozie.test.hadoop.security=kerberos

This distribution of Oozie uses HSQL as a database.

The options for using MySQL can be used together with this option.

--------------------------------------

Building Oozie distro for MySQL database.

$ bin/mkdistro.sh -DskipTests -dmysql -uroot -llocalhost:3306/oozie

Usage: bin/mkdistro.sh [-ddbtype] [-uusername] [-ppassword] [-lurl]

--------------------------------------

Enabling the Oozie web console

Oozie web console uses ExtJS which is not bundled with Oozie because it is not
Apache License.

The Oozie distro contains a script that installs the ExtJS, refer to the README.txt
in the distribution for more details.

To build Oozie with ExtJS already bundled in the distro do the following:

* Download the ExtJS 2.2 from http://www.extjs.com/learn/Ext_Version_Archives
* Expand the ExtJS ZIP file
* Copy the contents of the ext-2.2 directory into 'webapp/src/main/webapp/ext-2.2'

--------------------------------------

After the distribution is built, detailed documentation, including build options,
is available in the wars/ooziedocs.war file in the distribution, deploy this file
in Tomcat or expand it within a ooziedocs/ directory. To install and setup Oozie,
please refer to this twiki after deploying of Oozie docs:

http://localhost:8080/ooziedocs/DG_QuickStart.html#Install_and_Start_Oozie_server_and_Oozie_Console

--------------------------------------

Eclipse setup

To setup Oozie in Eclipse, you can follow these steps:

1. Untar oozie-*-distro.tar.gz
2. Run Eclipse
3. Use above oozie directory as workspace
4. Go to File -> Import...
5. Select General -> Maven Projects
6. Use workspace as root directory
7. Select all projects to import

Oozie currently supports Hadoop security version and non-security version. If Oozie project
is opened in Eclipse, the classes for security Hadoop will have compilation errors. Please follow
these steps to exclude those files in the settings.

1. Right click on the project
2. Click "Java Build Path"
3. Click "Source" tab
4. Under "oozie-core/src/main/java"
5. Edit "Excluded"
6. Add pattern "**/Kerberos*.java" in Exclusion patterns
7. Click "Finish"

--------------------------------------

If you have any questions/issues, please send an email to: oozie-users@yahoogroups.com
