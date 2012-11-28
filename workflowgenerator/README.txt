
Workflow Generator Tool
=========================

-----------------------------------------------
What is Workflow Generator Tool
-----------------------------------------------
Workflow generator tool is a web application where a user can construct Oozie workflow through GUI.
Since it is based on html and javascript, a user needs only browser to access the tool. (major browsers such as Chrome, Firefox, IE supported)
It is developed using Google Web Toolkit ( https://developers.google.com/web-toolkit/ ), which provides functionality to compile java code into javascript.
Therefore, although final product is in javascript, development process is performed in java.


-------------------------------------------------------------------------------------
How to build and launch Workflow Generator Tool as part of entire oozie package build
-------------------------------------------------------------------------------------

1. run mkdistro.sh on top-level directory
---------------------
bin/mkdistro.sh -P wfgen
---------------------
[NOTE]
currently workflow generator is not included in build as default, thus need to specify maven profile (-P wfgen)

2. move to output directory
--------------------
cd distro/target/oozie-<version>-distro/oozie-<version>
--------------------

3-(a). copy the war file to oozie-server/webapps/
--------------------
cp oozie-wfgen.war ./oozie-server/webapps
--------------------

or

3-(b). create /libext and copy the war file to it
--------------------
mkdir libext
cp oozie-wfgen.war ./libext
--------------------
[NOTE]
bin/oozie-setup.sh is implemented such that wfgen.war is automatically picked up and deployed to oozie server

4. start oozie server (using bin/oozie-setup.sh and bin/oozie-start.sh) and check through browser
---------------------
http://localhost:11000/oozie-wfgen
---------------------
[NOTE]
using default port number, which is 11000. tomcat server may fail to start if other application already using the same port. please make sure the port is not used.



----------------------------------------------------------------------------------------
How to build and launch Workflow Generator Tool only (not whole package build)
----------------------------------------------------------------------------------------

There are two possible options
Option-A) launch app using web server that you are already running (without using bundled tomcat)
Option-B) launch app in dev mode using bundled internal web server provided by GWT.

Option-A would be suitable when you want to host this application on the existing tomcat instance. You need to copy war file into webapp directory of existing tomcat.
Option-B would be suitable for development of the tool since you can see error messages for debugging, and don't need to create and copy war file every time

Option-B internally using java class (servlet), not javascript.
On browser UI, therefore, DOM operation (e.g., creating/deleting nodewidget) might be slower than using pure javascript (Option-A)

===============================================
<Option-A>

1. build workflowgenerator and create war file
---------------------
cd workflowgenerator // assuming you are on top directory
mvn clean package
---------------------
[NOTE]
test case not implemented yet

2. copy war file to webapps directory of web server
---------------------
cp target/oozie-wfgen.war <webserver-installed-directory>/webapps/
---------------------

3. start web server
---------------------
<webserver-installed-directory>/bin/startup.sh
---------------------
[NOTE]
name of start script might be different in your web-server, please change accordingly

4. check through browser
---------------------
http://localhost:8080/oozie-wfgen
---------------------
[NOTE]
port number might not be 8080 in your web-server setting (usually it's default in tomcat), please change accordingly

5. stop web server
---------------------
<webserver-installed-directory>/bin/shutdown.sh
---------------------
[NOTE] name of shutdown script might be different in your web-server, please change accordingly

===============================================
<Option-B>

1. compile and start GWT launcher
---------------------
cd workflowgenerator // assuming you are on top directory
mvn clean gwt:run
---------------------
you will see GWT launcher program automatically starts

2. launch app on browser from GWT launcher
---------------------
press button either "Launch Default Browser" or "Copy to Clipboard"

"Launcher Default Browser"
  --> automatically open application on default browser
  --> First time, you will be asked to install plug-in on browser, (also take few mins to launch app, pls be patient)

"Copy to Clipboard"
  --> you can copy this link to browser that you like and open it
  --> First time, you will be asked to install plug-in on browser (also take few mins to launch app, pls be patient)
---------------------

if you want to kill process, Ctrl-C in console


=====================================================================

If you have any questions/issues, please send an email to:

user@oozie.apache.org

Subscribe using the link:

http://oozie.apache.org/mail-lists.html
