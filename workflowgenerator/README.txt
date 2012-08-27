
Workflow Generator Tool
=========================

What is Workflow Generator Tool
----------------------
Workflow generator tool is a web application where a user can construct Oozie workflow through GUI.
Since it is based on html and javascript, a user needs only browser to access the tool. (major browsers such as Chrome, Firefox, IE supported)
It is developed using Google Web Toolkit ( https://developers.google.com/web-toolkit/ ), which provides functionality to compile java code into javascript.
Therefore, although final product is in javascript, development process is performed in java.


How to build and launch Workflow Generator Tool
----------------------

There are three possible options
Option-A) launch app using bundled tomcat
Option-B) launch app using web server that you are already running (without using bundled tomcat)
Option-C) launch app in dev mode using bundled internal web server provided by GWT.

Option-A would be good for easy start since you don't have to install and run tomcat server by yourself.
Option-B would be suitable when you want to host this application on the existing tomcat instance. You need to copy war file into webapp directory of existing tomcat.
Option-C would be suitable for development of the tool since you can see error messages for debugging, and don't need to create and copy war file every time

Option-C internally using java class (servlet), not javascript.
On browser UI, therefore, DOM operation (e.g., creating/deleting nodewidget) might be slower than using pure javascript (Option-A and B)

============================
<Option-A>

1. build project
---------------------
mvn clean package -Dmaven.test.skip=true
---------------------
[NOTE]
test case not implemented yet, so please skip test, otherwise may fail
during the build process, tomcat package is automatically downloaded from apache site. war file is also generated and copied into webapps directory of tomcat.

2. start web server
---------------------
target/workflowgenerator-1.0-SNAPSHOT-tomcat/bin/startup.sh
---------------------

3. check through browser
---------------------
http://localhost:8080/workflowgenerator-1.0-SNAPSHOT/
---------------------
[NOTE]
using default port number, which is 8080. tomcat server may fail to start if other application already using the same port. please make sure the port is not used. If you want to change the port number, please edit target/workflowgenerator-1.0-SNAPSHOT-tomcat/conf/server.xml

4. stop web server
---------------------
target/workflowgenerator-1.0-SNAPSHOT-tomcat/bin/shutdown.sh
---------------------


============================
<Option-B>

1. build project and create war file
---------------------
mvn clean package -Dmaven.test.skip=true
---------------------
[NOTE]
test case not implemented yet, so please skip test, otherwise may fail

2. copy war file to webapps directory of web server
---------------------
cp target/workflowgenerator-1.0-SNAPSHOT.war <webserver-installed-directory>/webapps/
---------------------

3. start web server
---------------------
<webserver-installed-directory>/bin/startup.sh
---------------------
[NOTE]
name of start script might be different in your web-server, please change accordingly

4. check through browser
---------------------
http://localhost:8080/workflowgenerator-1.0-SNAPSHOT/
---------------------
[NOTE]
port number might not be 8080 in your web-server setting, please change accordingly

5. stop web server
---------------------
<webserver-installed-directory>/bin/shutdown.sh
---------------------
[NOTE] name of shutdown script might be different in your web-server, please change accordingly


============================
<Option-C>

1. compile and start GWT launcher
---------------------
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

oozie-users@incubator.apache.org

Subscribe using the link:

http://incubator.apache.org/oozie/MailingLists.html
