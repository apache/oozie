

[::Go back to Oozie Documentation Index::](index.html)

# Building Oozie

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## System Requirements

   * Unix box (tested on Mac OS X and Linux)
   * Java JDK 1.8+
   * [Maven 3.0.1+](http://maven.apache.org/)
   * [Hadoop 2.6.0+](http://hadoop.apache.org/core/releases.html)
   * [Pig 0.10.1+](http://hadoop.apache.org/pig/releases.html)

JDK commands (java, javac) must be in the command path.

The Maven command (mvn) must be in the command path.

## Oozie Documentation Generation

To generate the documentation, Oozie uses a Doxia plugin for Maven with markdown support.






```
$ mvn install
```

<a name="SshSetup"></a>
## Passphrase-less SSH Setup

**NOTE: SSH actions are deprecated in Oozie 2.**

To run SSH Testcases and for easier Hadoop start/stop configure SSH to localhost to be passphrase-less.

Create your SSH keys without a passphrase and add the public key to the authorized file:


```
$ ssh-keygen -t dsa
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys2
```

Test that you can ssh without password:


```
$ ssh localhost
```

## Building with different Java Versions

Oozie requires a minimum Java version of 1.8. Any newer version can be used but by default bytecode will be generated
which is compatible with 1.8. This can be changed by specifying the build property **targetJavaVersion**.

## Building and Testing Oozie

The JARs for the specified Hadoop and Pig versions must be available in one of the Maven repositories defined in Oozie
main 'pom.xml' file. Or they must be installed in the local Maven cache.

### Examples Running Oozie Testcases with Different Configurations

**Using embedded Hadoop minicluster with 'simple' authentication:**


```
$ mvn clean test
```

**Using a Hadoop cluster with 'simple' authentication:**


```
$ mvn clean test -Doozie.test.hadoop.minicluster=false
```

**Using embedded Hadoop minicluster with 'simple' authentication and Derby database:**


```
$ mvn clean test -Doozie.test.hadoop.minicluster=false -Doozie.test.db=derby
```

**Using a Hadoop cluster with 'kerberos' authentication:**


```
$ mvn clean test -Doozie.test.hadoop.minicluster=false -Doozie.test.hadoop.security=kerberos
```

NOTE: The embedded minicluster cannot be used when testing with 'kerberos' authentication.

**Using a custom Oozie configuration for testcases:**


```
$ mvn clean test -Doozie.test.config.file=/home/tucu/custom-oozie-sitel.xml
```

**Running the testcases with different databases:**


```
$ mvn clean test -Doozie.test.db=[hsqldb*|derby|mysql|postgres|oracle]
```

Using `mysql` and `oracle` enables profiles that will include their JARs files in the build. If using
 `oracle`, the Oracle JDBC JAR file must be manually installed in the local Maven cache (the JAR is
not available in public Maven repos).

### Build Options Reference

All these options can be set using **-D**.

Except for the options marked with `(*)`, the options can be specified in the `test.properties` in the root
of the Oozie project. The options marked with `(*)` are used in Maven POMs, thus they don't take effect if
specified in the `test.properties` file (which is loaded by the `XTestCase` class at class initialization time).

**hadoop.version** `(*)`: indicates the Hadoop version you wish to build Oozie against specifically. It will
substitute this value in the Oozie POM properties and pull the corresponding Hadoop artifacts from Maven.
The default version is 2.6.0 and that is the minimum supported Hadoop version.

**generateSite** (*): generates Oozie documentation, default is undefined (no documentation is generated)

**skipTests** (*): skips the execution of all testcases, no value required, default is undefined

**test**= (*): runs a single test case, to run a test give the test class name without package and extension, no default

**oozie.test.db**= (*): indicates the database to use for running the testcases, supported values are 'hsqldb', 'derby',
 'mysql', 'postgres' and 'oracle'; default value is 'hsqldb'. For each database there is
 `core/src/test/resources/DATABASE-oozie-site.xml` file preconfigured.

**oozie.test.properties** (*): indicates the file to load the test properties from, by default is `test.properties`.
Having this option allows having different test properties sets, for example: minicluster, simple & kerberos.

**oozie.test.waitfor.ratio**= : multiplication factor for testcases using waitfor, the ratio is used to adjust the
effective time out. For slow machines the ratio should be increased. The default value is `1`.

**oozie.test.config.file**= : indicates a custom Oozie configuration file for running the testcases. The specified file
must be an absolute path. For example, it can be useful to specify different database than HSQL for running the
testcases.

**oozie.test.hadoop.minicluster**= : indicates if Hadoop minicluster should be started for testcases, default value 'true'

**oozie.test.job.tracker**= : indicates the URI of the JobTracker when using a Hadoop cluster for testing, default value
'localhost:8021'

**oozie.test.name.node**= : indicates the URI of the NameNode when using a Hadoop cluster for testing, default value
'`hdfs://localhost:8020`'

**oozie.test.hadoop.security**= : indicates the type of Hadoop authentication for testing, valid values are 'simple' or
'kerberos, default value 'simple'

**oozie.test.kerberos.keytab.file**= : indicates the location of the keytab file, default value
'${user.home}/oozie.keytab'

**oozie.test.kerberos.realm**= : indicates the Kerberos real, default value 'LOCALHOST'

**oozie.test.kerberos.oozie.principal**= : indicates the Kerberos principal for oozie, default value
'${user.name}/localhost'

**oozie.test.kerberos.jobtracker.principal**= : indicates the Kerberos principal for the JobTracker, default value
'mapred/localhost'

**oozie.test.kerberos.namenode.principal**= : indicates the Kerberos principal for the NameNode, default value
'hdfs/localhost'

**oozie.test.user.oozie**= : specifies the user ID used to start Oozie server in testcases, default value
is `${user.name}`.

**oozie.test.user.test**= : specifies primary user ID used as the user submitting jobs to Oozie Server in testcases,
default value is `test`.

**oozie.test.user.test2**= : specifies secondary user ID used as the user submitting jobs to Oozie Server in testcases,
default value is `test2`.

**oozie.test.user.test3**= : specifies secondary user ID used as the user submitting jobs to Oozie Server in testcases,
default value is `test3`.

**oozie.test.group**= : specifies group ID used as group when submitting jobs to Oozie Server in testcases,
default value is `testg`.

NOTE: The users/group specified in **oozie.test.user.test2**, **oozie.test.user.test3** and **oozie.test.user.group**
are used for the authorization testcases only.

**oozie.test.dir**` : specifies the directory where the `oozietests` directory will be created, default value is `/tmp=.
The `oozietests` directory is used by testcases when they need a local filesystem directory.

**hadoop.log.dir**= : specifies the directory where Hadoop minicluster will write its logs during testcases, default
value is `/tmp`.

**test.exclude**` : specifies a testcase class (just the class name) to exclude for the tests run, for example =TestSubmitCommand`.

**test.exclude.pattern**` : specifies one or more patterns for testcases to exclude, for example =**/Test*Command.java`.

### Testing Map Reduce Pipes Action

Pipes testcases require Hadoop's **wordcount-simple** pipes binary example to run. The  **wordcount-simple** pipes binary
should be compiled for the build platform and copied into Oozie's **core/src/test/resources/** directory. The binary file
must be named **wordcount-simple**.

If the  **wordcount-simple** pipes binary file is not available the testcase will do a NOP and it will print to its output
file the following message 'SKIPPING TEST: TestPipesMain, binary 'wordcount-simple' not available in the classpath'.

There are 2 testcases that use the **wordcount-simple** pipes binary, **TestPipesMain** and **TestMapReduceActionExecutor**,
the 'SKIPPING TEST..." message would appear in the testcase log file of both testcases.

### Testing using dist_test and grind

Testing using [dist_test](https://github.com/cloudera/dist_test) framework with
[grind](https://github.com/cloudera/dist_test/blob/master/docs/grind.md) front end might not work using the default 3.0.2
version of the maven dependency plugin. It is necessary to downgrade to version 2.10 using
`-Dmaven-dependency-plugin.version=2.10` .

Maven flags for grind can be specified using `GRIND_MAVEN_FLAGS` environment variable:


```
export GRIND_MAVEN_FLAGS=-Dmaven.dependency.plugin.version=2.10
grind test --java-version 8
```

## Building an Oozie Distribution

An Oozie distribution bundles an embedded Jetty server.

The simplest way to build Oozie is to run the `mkdistro.sh` script:

```
$ bin/mkdistro.sh [-DskipTests]
Running =mkdistro.sh= will create the binary distribution of Oozie. The following options are available to customise
the versions of the dependencies:
-Puber - Bundle required hadoop and hcatalog libraries in oozie war
-Dhadoop.version=<version> - default 2.6.0
-Ptez - Bundle tez jars in hive and pig sharelibs. Useful if you want to use tez
as the execution engine for those applications.
-Dpig.version=<version> - default 0.16.0
-Dpig.classifier=<classifier> - default h2
-Dsqoop.version=<version> - default 1.4.3
-Dsqoop.classifier=<classifier> - default hadoop100
-jetty.version=<version> - default 9.3.20.v20170531
-Dopenjpa.version=<version> - default 2.2.2
-Dxerces.version=<version> - default 2.10.0
-Dcurator.version=<version> - default 2.5.0
-Dhive.version=<version> - default 1.2.0
-Dhbase.version=<version> - default 1.2.3
-Dtez.version=<version> - default 0.8.4
```

**IMPORTANT:** Profile hadoop-3 must be activated if building against Hadoop 3

The following properties should be specified when building a release:

   * -DgenerateDocs : forces the generation of Oozie documentation
   * -Dbuild.time=  : timestamps the distribution
   * -Dvc.revision= : specifies the source control revision number of the distribution
   * -Dvc.url=      : specifies the source control URL of the distribution

The provided `bin/mkdistro.sh` script runs the above Maven invocation setting all these properties to the
right values (the 'vc.*' properties are obtained from the local git repository).

## IDE Setup

Eclipse and IntelliJ can use directly Oozie Maven project files.

The only special consideration is that the following source directories from the `client` module must be added to
the `core` module source path:

   * `client/src/main/java` : as source directory
   * `client/src/main/resources` : as source directory
   * `client/src/test/java` : as test-source directory
   * `client/src/test/resources` : as test-source directory

[::Go back to Oozie Documentation Index::](index.html)


