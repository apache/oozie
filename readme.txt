Oozie v1 is a system that runs workflows of Hadoop Map-Reduce/Pig jobs.

--------------------------------------
Requirements for building and testing Oozie:

* Java 6+
* Maven 2.0.10+
* Hadoop 0.20+
* Pig 0.2+

--------------------------------------
Initial Maven setup:


$ build-setup/setup-maven.sh

$ build-setup/setup-jars.sh


These scripts does 2 things: The first one will install a modified documentation
plugin with better twiki support. The second one will install JARs in the local
Maven repository that are not available in public Maven repositories.

--------------------------------------
Building a Oozie distro:


$ bin/mkdistro.sh -DskipTests

This script will generate a distribution for the current version of Hadoop and
Pig without running the testcases.

After the distribution is built, detailed documentation, including build options,
is available in the wars/ooziedocs.war file in the distribution, deploy this file
in Tomcat or expand it within a docs/ directory.

--------------------------------------
Building with the ExtJS library for the Oozie web console

The Oozie distro contains a script that installs the ExtJS, refer to the README.txt
in the distribution for more details.

To build Oozie with ExtJS already bundled do the following:

* Download the ExtJS 2.2 from http://www.extjs.com/learn/Ext_Version_Archives
* Expand the ExtJS ZIP file
* Copy the contents of the ext-2.2 directory into 'webapp/src/main/webapp/ext-2'
--------------------------------------

