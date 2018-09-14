

[::Go back to Oozie Documentation Index::](index.html)

# Oozie Quick Start

These instructions install and run Oozie using an embedded Jetty server and an embedded Derby database.

For detailed install and configuration instructions refer to [Oozie Install](AG_Install.html).

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Building Oozie

### System Requirements:
   * Unix box (tested on Mac OS X and Linux)
   * Java JDK 1.8+
   * Maven 3.0.1+
   * Hadoop 2.6.0+
   * Pig 0.10.1+

JDK commands (java, javac) must be in the command path.

The Maven command (mvn) must be in the command path.

### Building Oozie

Download a source distribution of Oozie from the "Releases" drop down menu on the [Oozie site](http://oozie.apache.org).

Expand the source distribution `tar.gz` and change directories into it.

The simplest way to build Oozie is to run the `mkdistro.sh` script:

```
$ bin/mkdistro.sh [-DskipTests]
```


Running `mkdistro.sh` will create the binary distribution of Oozie. By default, oozie war will not contain hadoop and
hcatalog libraries, however they are required for oozie to work. There are 2 options to add these libraries:

1. At install time, copy the hadoop and hcatalog libraries to libext and run oozie-setup.sh to setup Oozie. This is
    suitable when same oozie package needs to be used in multiple set-ups with different hadoop/hcatalog versions.

2. Build with -Puber which will bundle the required libraries in the oozie war. Further, the following options are
    available to customise the versions of the dependencies:
    ```
    -Dhadoop.version=<version> - default 2.6.0
    -Ptez - Bundle tez jars in hive and pig sharelibs. Useful if you want to use tez
    +as the execution engine for those applications.
    -Dpig.version=<version> - default 0.16.0
    -Dpig.classifier=<classifier> - default h2
    -Dsqoop.version=<version> - default 1.4.3
    -Dsqoop.classifier=<classifier> - default hadoop100
    -Djetty.version=<version> - default 9.3.20.v20170531
    -Dopenjpa.version=<version> - default 2.2.2
    -Dxerces.version=<version> - default 2.10.0
    -Dcurator.version=<version> - default 2.5.0
    -Dhive.version=<version - default 1.2.0
    -Dhbase.version=<version> - default 1.2.3
    -Dtez.version=<version> - default 0.8.4

    *IMPORTANT:* Profile hadoop-3 must be activated if building against Hadoop 3
    ```

    More details on building Oozie can be found on the [Building Oozie](ENG_Building.html) page.

## Server Installation

### System Requirements

   * Unix (tested in Linux and Mac OS X)
   * Java 1.8+
   * Hadoop
      * [Apache Hadoop](http://hadoop.apache.org) (tested with 1.2.1 & 2.6.0+)
   * ExtJS library (optional, to enable Oozie webconsole)
      * [ExtJS 2.2](http://archive.cloudera.com/gplextras/misc/ext-2.2.zip)

The Java 1.8+ `bin` directory should be in the command path.

### Server Installation

**IMPORTANT:** Oozie ignores any set value for `OOZIE_HOME`, Oozie computes its home automatically.

   * Build an Oozie binary distribution
   * Download a Hadoop binary distribution
   * Download ExtJS library (it must be version 2.2)

**NOTE:** The ExtJS library is not bundled with Oozie because it uses a different license.

**NOTE:** Oozie UI browser compatibility Chrome (all), Firefox (3.5), Internet Explorer (8.0), Opera (10.5).

**NOTE:** It is recommended to use a Oozie Unix user for the Oozie server.

Expand the Oozie distribution `tar.gz`.

Expand the Hadoop distribution `tar.gz` (as the Oozie Unix user).

<a name="HadoopProxyUser"></a>

**NOTE:** Configure the Hadoop cluster with proxyuser for the Oozie process.

The following two properties are required in Hadoop core-site.xml:


```
  <!-- OOZIE -->
  <property>
    <name>hadoop.proxyuser.[OOZIE_SERVER_USER].hosts</name>
    <value>[OOZIE_SERVER_HOSTNAME]</value>
  </property>
  <property>
    <name>hadoop.proxyuser.[OOZIE_SERVER_USER].groups</name>
    <value>[USER_GROUPS_THAT_ALLOW_IMPERSONATION]</value>
  </property>
```

Replace the capital letter sections with specific values and then restart Hadoop.

The ExtJS library is optional (only required for the Oozie web-console to work)

**IMPORTANT:** all Oozie server scripts (`oozie-setup.sh`, `oozied.sh`, `oozie-start.sh`, `oozie-run.sh`
and `oozie-stop.sh`) run only under the Unix user that owns the Oozie installation directory,
if necessary use `sudo -u OOZIE_USER` when invoking the scripts.

As of Oozie 3.3.2, use of `oozie-start.sh`, `oozie-run.sh`, and `oozie-stop.sh` has
been deprecated and will print a warning. The `oozied.sh` script should be used
instead; passing it `start`, `run`, or `stop` as an argument will perform the
behaviors of `oozie-start.sh`, `oozie-run.sh`, and `oozie-stop.sh` respectively.

Create a **libext/** directory in the directory where Oozie was expanded.

If using the ExtJS library copy the ZIP file to the **libext/** directory. If hadoop and hcatalog libraries are not
already included in the war, add the corresponding libraries to **libext/** directory.

A "sharelib create -fs fs_default_name [-locallib sharelib]" command is available when running oozie-setup.sh
for uploading new sharelib into hdfs where the first argument is the default fs name
and the second argument is the Oozie sharelib to install, it can be a tarball or the expanded version of it.
If the second argument is omitted, the Oozie sharelib tarball from the Oozie installation directory will be used.
Upgrade command is deprecated, one should use create command to create new version of sharelib.
Sharelib files are copied to new `lib_<timestamped>` directory. At start, server picks the sharelib from latest time-stamp directory.
While starting server also purge sharelib directory which is older than sharelib retention days
(defined as oozie.service.ShareLibService.temp.sharelib.retention.days and 7 days is default).

db create|upgrade|postupgrade -run [-sqlfile \<FILE\>] command is for create, upgrade or postupgrade oozie db with an
optional sql file

Run the `oozie-setup.sh` script to configure Oozie with all the components added to the **libext/** directory.


```
$ bin/oozie-setup.sh sharelib create -fs <FS_URI> [-locallib <PATH>]
                     sharelib upgrade -fs <FS_URI> [-locallib <PATH>]
                     db create|upgrade|postupgrade -run [-sqlfile <FILE>]
```

**IMPORTANT**: If the Oozie server needs to establish secure connection with an external server with a self-signed certificate,
make sure you specify the location of a truststore that contains required certificates. It can be done by configuring
`oozie.https.truststore.file` in `oozie-site.xml`, or by setting the `javax.net.ssl.trustStore` system property.
If it is set in both places, the value passed as system property will be used.

Create the Oozie DB using the 'ooziedb.sh' command line tool:


```
$ bin/ooziedb.sh create -sqlfile oozie.sql -run

Validate DB Connection.
DONE
Check DB schema does not exist
DONE
Check OOZIE_SYS table does not exist
DONE
Create SQL schema
DONE
DONE
Create OOZIE_SYS table
DONE

Oozie DB has been created for Oozie version '3.2.0'

$
```

Start Oozie as a daemon process run:


```
$ bin/oozied.sh start
```

To start Oozie as a foreground process run:


```
$ bin/oozied.sh run
```

Check the Oozie log file `logs/oozie.log` to ensure Oozie started properly.

Using the Oozie command line tool check the status of Oozie:


```
$ bin/oozie admin -oozie http://localhost:11000/oozie -status
```

Using a browser go to the [Oozie web console](http://localhost:11000/oozie.html), Oozie status should be **NORMAL**.

Refer to the [Running the Examples](DG_Examples.html) document for details on running the examples.

## Client Installation

### System Requirements

   * Unix (tested in Linux and Mac OS X)
   * Java 1.8+

The Java 1.8+ `bin` directory should be in the command path.

### Client Installation

Copy and expand the `oozie-client` TAR.GZ file bundled with the distribution. Add the `bin/` directory to the `PATH`.

Refer to the [Command Line Interface Utilities](DG_CommandLineTool.html) document for a full reference of the `oozie`
command line tool.

NOTE: The Oozie server installation includes the Oozie client. The Oozie client should be installed in remote machines
only.

<a name="OozieShareLib"></a>
## Oozie Share Lib Installation

Oozie share lib has been installed by oozie-setup.sh create command explained in the earlier section.

See the [Workflow Functional Specification](WorkflowFunctionalSpec.html#ShareLib) and [Installation](AG_Install.html#Oozie_Share_Lib) for more information about the Oozie ShareLib.

[::Go back to Oozie Documentation Index::](index.html)


