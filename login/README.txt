
Login Server Example
====================

---------------------------------
What is the Login Server Example?
---------------------------------
The Login Server Example is a web application that is an example of how to create a login server for Oozie.  It provides two example
servlets: LoginServlet and LDAPLoginServlet.  The LoginServlet example is very primitive and simply authenticates users whose
username and password match (e.g. user=foo and pass=foo).  The LDAPLoginServlet example can be configured against an LDAP server to
authenticate users from that LDAP server.  Onces authenticated, both example servlets write the username to a cookie that Oozie
checks via the ExampleAltAuthenticationHandler (which uses that cookie for authentication for browsers but Kerberos otherwise).

The LoginServlet and LDAPLoginServlet are run from a separate WAR file called oozie-login.war; its web.xml can be used to configure
which servlet is used as well as some additional properties. The ExampleAltAuthenticationHandler is run as part of Oozie.

More details on the Login Server Example and the three classes can be found on the "Creating Custom Authentication" page of the
Oozie Documentation.

ExampleAltAuthenticationHandler, LoginServlet, and LDAPLoginServlet ARE NOT SECURE
                                                                    -- THEY SHOULD NOT BE USED IN A PRODUCTION ENVIRONMENT

--------------------------------------------------------------------------------------
How to build and launch the Login Server Example as part of entire oozie package build
--------------------------------------------------------------------------------------

1. run mkdistro.sh on top-level directory
---------------------
bin/mkdistro.sh -P loginServerExample
---------------------
[NOTE]
The Login Server Example is not included in the build by default, hence the need to specify a maven profile (-P loginServerExample).
This maven profile causes two additional files to be built: oozie-login.war (contains the oozie login server example) and
oozie-login.jar (contains the AuthenticationHandler to use with the oozie login server example)

2. move to output directory
--------------------
cd distro/target/oozie-<version>-distro/oozie-<version>
--------------------

3-(a). copy the war file to oozie-server/webapps/
--------------------
cp oozie-login.war ./oozie-server/webapps
--------------------
[NOTE]
Method (a) only gives you the login server; to also make the AuthenticationHandler available to the Oozie server, use method (b)

or

3-(b). create /libext and copy the war and jar files to it
--------------------
mkdir libext
cp oozie-login.war ./libext
cp oozie-login.jar ./libext
--------------------
[NOTE]
bin/oozie-setup.sh is implemented such that oozie-login.war is automatically picked up and deployed to oozie server

4. start oozie server (using bin/oozie-setup.sh and bin/oozie-start.sh) and check through browser
---------------------
http://localhost:11000/oozie-login
---------------------
[NOTE]
Using default port number, which is 11000. Tomcat server may fail to start if another application already using the same port.
Please make sure the port is not being used.


---------------------------------------------------------------------------
How to build and launch Login Server Example only (not whole package build)
---------------------------------------------------------------------------
This is to launch the Login Server Example using a web server that you are already running (without using bundled tomcat).
This is suitable for when you want to host this application on the existing tomcat instance. You need to copy the war file into
the webapp directory of the existing tomcat.

1. build the Login Server Example and create war file
---------------------
// Assuming you are at the top level directory
mvn clean package -P loginServerExample -Dtest=TestExampleAltAuthenticationHandler,TestLoginServlet,TestLDAPLoginServlet
---------------------
[NOTE]
This must be done from the top level directory because oozie-core is a dependency on the Login Server Example. To skip all tests,
replace the -Dtest=... with -DskipTests.

2. copy war file to webapps directory of web server
---------------------
cp login/target/oozie-login.war <webserver-installed-directory>/webapps/
---------------------

3. start web server
---------------------
<webserver-installed-directory>/bin/startup.sh
---------------------
[NOTE]
name of start script might be different in your web-server, please change accordingly

4. check through browser
---------------------
http://localhost:8080/oozie-login
---------------------
[NOTE]
port number might not be 8080 in your web-server setting (usually it's default in tomcat), please change accordingly

5. stop web server
---------------------
<webserver-installed-directory>/bin/shutdown.sh
---------------------
[NOTE] name of shutdown script might be different in your web-server, please change accordingly


=====================================================================

If you have any questions/issues, please send an email to:

user@oozie.apache.org

Subscribe using the link:

http://oozie.apache.org/mail-lists.html
