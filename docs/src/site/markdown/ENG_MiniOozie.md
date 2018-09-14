

[::Go back to Oozie Documentation Index::](index.html)

# Running MiniOozie Tests

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## System Requirements

   * Unix box (tested on Mac OS X and Linux)
   * Java JDK 1.8+
   * Eclipse (tested on 3.5 and 3.6)
   * [Maven 3.0.1+](http://maven.apache.org/)

The Maven command (mvn) must be in the command path.

## Installing Oozie Jars To Maven Cache

Oozie source tree is at Apache SVN or Apache GIT. MiniOozie sample project is under Oozie source tree.

The following command downloads Oozie trunk to local:


```
$ svn co https://svn.apache.org/repos/asf/incubator/oozie/trunk
```

OR


```
$ git clone git://github.com/apache/oozie.git
```

To run MiniOozie tests, the required jars like oozie-core, oozie-client, oozie-core-tests need to be
available in remote maven repositories or local maven repository. The local maven cache for the above
jars can be created and installed using the command:


```
$ mvn clean install -DskipTests -DtestJarSimple
```

The following properties should be specified to install correct jars for MiniOozie:

   * -DskipTests       : ignore executing Oozie unittests
   * -DtestJarSimple=  : build only required test classes to oozie-core-tests

MiniOozie is a folder named 'minitest' under Oozie source tree. Two sample tests are included in the project.
The following command to execute tests under MiniOozie:


```
$ cd minitest
$ mvn clean test
```

## Create Tests Using MiniOozie

MiniOozie is a JUnit test class to test Oozie applications such as workflow and coordinator. The test case
needs to extend from MiniOozieTestCase and does the same as the example class 'WorkflowTest.java' to create Oozie
workflow application properties and workflow XML. The example file is under Oozie source tree:

   * `minitest/src/test/java/org/apache/oozie/test/WorkflowTest.java`

## IDE Setup

Eclipse and IntelliJ can use directly MiniOozie Maven project files. MiniOozie project can be imported to
Eclipse and IntelliJ as independent project.

The test directories under MiniOozie are:

   * `minitest/src/test/java` : as test-source directory
   * `minitest/src/test/resources` : as test-resource directory


Also asynchronous actions like FS action can be used / tested using `LocalOozie` / `OozieClient` API.
Please see `fs-decision.xml` workflow example.

[::Go back to Oozie Documentation Index::](index.html)


