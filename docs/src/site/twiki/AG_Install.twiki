

[::Go back to Oozie Documentation Index::](index.html)

# Oozie Installation and Configuration

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Basic Setup

Follow the instructions at [Oozie Quick Start](DG_QuickStart.html).

## Environment Setup

**IMPORTANT:** Oozie ignores any set value for `OOZIE_HOME`, Oozie computes its home automatically.

When running Oozie with its embedded Jetty server, the `conf/oozie-env.sh` file can be
used to configure the following environment variables used by Oozie:

**JETTY_OPTS** : settings for the Embedded Jetty that runs Oozie. Java System properties
for Oozie should be specified in this variable. No default value.

**OOZIE_CONFIG_FILE** : Oozie configuration file to load from Oozie configuration directory.
Default value `oozie-site.xml`.

**OOZIE_LOGS** : Oozie logs directory. Default value `logs/` directory in the Oozie installation
directory.

**OOZIE_LOG4J_FILE** :  Oozie Log4J configuration file to load from Oozie configuration directory.
Default value `oozie-log4j.properties`.

**OOZIE_LOG4J_RELOAD** : Reload interval of the Log4J configuration file, in seconds.
Default value `10`

**OOZIE_CHECK_OWNER** : If set to `true`, Oozie setup/start/run/stop scripts will check that the
owner of the Oozie installation directory matches the user invoking the script. The default
value is undefined and interpreted as a `false`.

**OOZIE_INSTANCE_ID** : The instance id of the Oozie server.  When using HA, each server instance should have a unique instance id.
Default value `${OOZIE_HTTP_HOSTNAME}`

## Oozie Server Setup

The `oozie-setup.sh` script prepares the embedded Jetty server to run Oozie.

The `oozie-setup.sh` script options are:


```
Usage  : oozie-setup.sh <Command and OPTIONS>
          sharelib create -fs FS_URI [-locallib SHARED_LIBRARY] [-extralib EXTRA_SHARED_LIBRARY] [-concurrency CONCURRENCY]
                                                                (create sharelib for oozie,
                                                                FS_URI is the fs.default.name
                                                                for hdfs uri; SHARED_LIBRARY, path to the
                                                                Oozie sharelib to install, it can be a tarball
                                                                or an expanded version of it. If omitted,
                                                                the Oozie sharelib tarball from the Oozie
                                                                installation directory will be used.
                                                                EXTRA_SHARED_LIBRARY represents extra sharelib resources.
                                                                This option requires a pair of sharelibname
                                                                and comma-separated list of pathnames in the following format:
                                                                sharelib-name=path-name-1,path-name-2
                                                                In case of more than one sharelib, this option can be specified
                                                                multiple times.
                                                                CONCURRENCY is a number of threads to be used
                                                                for copy operations.
                                                                By default 1 thread will be used)
                                                                (action fails if sharelib is already installed
                                                                in HDFS)
          sharelib upgrade -fs FS_URI [-locallib SHARED_LIBRARY] ([deprecated][use create command to create new version]
                                                                  upgrade existing sharelib, fails if there
                                                                  is no existing sharelib installed in HDFS)
          db create|upgrade|postupgrade -run [-sqlfile <FILE>] (create, upgrade or postupgrade oozie db with an
                                                                optional sql File)
          export <file>                                         exports the oozie database to the specified
                                                                file in zip format
          import <file>                                         imports the oozie database from the zip file
                                                                created by export
          (without options prints this usage information)
```

If a directory `libext/` is present in Oozie installation directory, the `oozie-setup.sh` script will
include all JARs in Jetty's `webapp/WEB_INF/lib/` directory.

If the ExtJS ZIP file is present in the `libext/` directory, it will be added to the Jetty's `webapp/` directory as well.
The ExtJS library file name be `ext-2.2.zip`.

### Setting Up Oozie with an Alternate Tomcat

Use the `addtowar.sh` script to prepare the Oozie server only if Oozie will run with a different
servlet  container than the embedded Jetty provided with the distribution.

The `addtowar.sh` script adds Hadoop JARs, JDBC JARs and the ExtJS library to the Oozie WAR file.

The `addtowar.sh` script options are:


```
 Usage  : addtowar <OPTIONS>
 Options: -inputwar INPUT_OOZIE_WAR
          -outputwar OUTPUT_OOZIE_WAR
          [-hadoop HADOOP_VERSION HADOOP_PATH]
          [-extjs EXTJS_PATH]
          [-jars JARS_PATH] (multiple JAR path separated by ':')
          [-secureWeb WEB_XML_PATH] (path to secure web.xml)
```

The original `oozie.war` file is in the Oozie server installation directory.

After the Hadoop JARs and the ExtJS library has been added to the `oozie.war` file Oozie is ready to run.

Delete any previous deployment of the `oozie.war` from the servlet container (if using Tomcat, delete
`oozie.war` and `oozie` directory from Tomcat's `webapps/` directory)

Deploy the prepared `oozie.war` file (the one that contains the Hadoop JARs and the ExtJS library) in the
servlet container (if using Tomcat, copy the prepared `oozie.war` file to Tomcat's `webapps/` directory).

**IMPORTANT:** Only one Oozie instance can be deployed per Tomcat instance.

## Database Configuration

Oozie works with HSQL, Derby, MySQL, Oracle, PostgreSQL or SQL Server databases.

By default, Oozie is configured to use Embedded Derby.

Oozie bundles the JDBC drivers for HSQL, Embedded Derby and PostgreSQL.

HSQL is normally used for test cases as it is an in-memory database and all data is lost every time Oozie is stopped.

If using Derby, MySQL, Oracle, PostgreSQL, or SQL Server, the Oozie database schema must be created using the `ooziedb.sh` command
line tool.

If using MySQL, Oracle, or SQL Server, the corresponding JDBC driver JAR file must be copied to Oozie's `libext/` directory and
it must be added to Oozie WAR file using the `bin/addtowar.sh` or the `oozie-setup.sh` scripts using the `-jars` option.

**IMPORTANT:** It is recommended to set the database's timezone to GMT (consult your database's documentation on how to do this).
Databases don't handle Daylight Saving Time shifts correctly, and may cause problems if you run any Coordinators with actions
scheduled to materialize during the 1 hour period where we "fall back".  For Derby, you can add '-Duser.timezone=GMT'
to `JETTY_OPTS` in oozie-env.sh to set this.  Alternatively, if using MySQL, you can have Oozie use GMT with MySQL without
setting MySQL's timezone to GMT by adding 'useLegacyDatetimeCode=false&serverTimezone=GMT' arguments to the JDBC
URL, `oozie.service.JPAService.jdbc.url`.  Be advised that changing the timezone on an existing Oozie database while Coordinators
are already running may cause Coordinators to shift by the offset of their timezone from GMT once after making this change.

The SQL database used by Oozie is configured using the following configuration properties (default values shown):


```
  oozie.db.schema.name=oozie
  oozie.service.JPAService.create.db.schema=false
  oozie.service.JPAService.validate.db.connection=false
  oozie.service.JPAService.jdbc.driver=org.apache.derby.jdbc.EmbeddedDriver
  oozie.service.JPAService.jdbc.url=jdbc:derby:${oozie.data.dir}/${oozie.db.schema.name}-db;create=true
  oozie.service.JPAService.jdbc.username=sa
  oozie.service.JPAService.jdbc.password=
  oozie.service.JPAService.pool.max.active.conn=10
```

**NOTE:** If the `oozie.db.schema.create` property is set to `true` (default value is `false`) the Oozie tables
will be created automatically without having to use the `ooziedb` command line tool. Setting this property to
 `true` it is recommended only for development.

**NOTE:** If the `oozie.db.schema.create` property is set to true, the `oozie.service.JPAService.validate.db.connection`
property value is ignored and Oozie handles it as set to `false`.

Once `oozie-site.xml` has been configured with the database configuration execute the `ooziedb.sh` command line tool to
create the database:


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

The SQL commands have been written to: oozie.sql

$
```

NOTE: If using MySQL, Oracle, or SQL Server, copy the corresponding JDBC driver JAR file to the `libext/` directory before running
the `ooziedb.sh` command line tool.

NOTE: If instead using the '-run' option, the `-sqlfile <FILE>` option is used, then all the
database changes will be written to the specified file and the database won't be modified.

If using HSQL there is no need to use the `ooziedb` command line tool as HSQL is an in-memory database. Use the
following configuration properties in the oozie-site.xml:


```
  oozie.db.schema.name=oozie
  oozie.service.JPAService.create.db.schema=true
  oozie.service.JPAService.validate.db.connection=false
  oozie.service.JPAService.jdbc.driver=org.hsqldb.jdbcDriver
  oozie.service.JPAService.jdbc.url=jdbc:hsqldb:mem:${oozie.db.schema.name}
  oozie.service.JPAService.jdbc.username=sa
  oozie.service.JPAService.jdbc.password=
  oozie.service.JPAService.pool.max.active.conn=10
```

If you are interested in fine tuning how Oozie can retry database operations on failing database connectivity or errors, you can
set following properties to other values. Here are the default ones:


```
  oozie.service.JPAService.retry.initial-wait-time.ms=100
  oozie.service.JPAService.retry.maximum-wait-time.ms=30000
  oozie.service.JPAService.retry.max-retries=10
```

If you set either `oozie.service.JPAService.retry.max-retries` or `oozie.service.JPAService.retry.maximum-wait-time.ms` to `0`,
no retry attempts will be made on any database connectivity issues. Exact settings for these properties depend also on how much load
is on Oozie regarding workflow and coordinator jobs.

The database operation retry functionality kicks in when there is a `javax.persistence.PersistenceException` those root cause is not
part of the normal everyday operation - filtered against a blacklist consisting of descendants like `NoSuchResultException`,
`NonUniqueResultException`, and the like. This way Oozie won't retry database operations on errors that are more related to the
current query, or otherwise part of the everyday life. This way it's ensured that this blacklist is database agnostic.

It has been tested with a MySQL / failing every minute 10 seconds / an Oozie coordinator job of an Oozie workflow consisting of four
workflow actions (some of them are asynchronous). On this setup Oozie was recovering after each and every database outage.

To set up such a failing MySQL scenario following has to be performed:

   * Set `oozie.service.JPAService.connection.data.source` to `org.apache.oozie.util.db.BasicDataSourceWrapper`
   within `oozie-site.xml`
   * Set `oozie.service.JPAService.jdbc.driver` to `org.apache.oozie.util.db.FailingMySQLDriverWrapper` within `oozie-site.xml`
   * Restart Oozie server
   * Submit / start some workflows, coordinators etc.
   * See how Oozie is retrying on injected database errors by looking at the Oozie server logs, grepping `JPAException` instances
   with following message prefix:  `Deliberately failing to prepare statement.`

## Database Migration

Oozie provides an easy way to switch between databases without losing any data. Oozie servers should be stopped during the
database migration process.
The export of the database can be done using the following command:

```
$ bin/oozie-setup.sh export /tmp/oozie_db.zip
1 rows exported from OOZIE_SYS
50 rows exported from WF_JOBS
340 rows exported from WF_ACTIONS
10 rows exported from COORD_JOBS
70 rows exported from COORD_ACTIONS
0 rows exported from BUNDLE_JOBS
0 rows exported from BUNDLE_ACTIONS
0 rows exported from SLA_REGISTRATION
0 rows exported from SLA_SUMMARY
```

The database configuration is read from `oozie-site.xml`. After updating the configuration to point to the new database,
the tables have to be created with ooziedb.sh in the [Database configuration](AG_Install.html#Database_Configuration)
section above.
Once the tables are created, they can be filled with data using the following command:


```
$ bin/oozie-setup.sh import /tmp/oozie_db.zip
Loading to Oozie database version 3
50 rows imported to WF_JOBS
340 rows imported to WF_ACTIONS
10 rows imported to COORD_JOBS
70 rows imported to COORD_ACTIONS
0 rows imported to BUNDLE_JOBS
0 rows imported to BUNDLE_ACTIONS
0 rows imported to SLA_REGISTRATION
0 rows imported to SLA_SUMMARY
```

NOTE: The database version of the zip must match the version of the Oozie database it's imported to.

After starting the Oozie server, the history and the currently running workflows should be available.

**IMPORTANT:** The tool was primarily developed to make the migration from embedded databases (e.g. Derby) to standalone databases
 (e.g. MySQL, PosgreSQL, Oracle, MS SQL Server), though it will work between any supported databases.
It is **not** optimized to handle databases over 1 Gb. If the database size is larger, it should be purged before migration.

## Oozie Configuration

By default, Oozie configuration is read from Oozie's `conf/` directory

The Oozie configuration is distributed in 3 different files:

   * `oozie-site.xml` : Oozie server configuration
   * `oozie-log4j.properties` : Oozie logging configuration
   * `adminusers.txt` : Oozie admin users list

### Oozie Configuration Properties

All Oozie configuration properties and their default values are defined in the `oozie-default.xml` file.

Oozie resolves configuration property values in the following order:

   * If a Java System property is defined, it uses its value
   * Else, if the Oozie configuration file (`oozie-site.xml`) contains the property, it uses its value
   * Else, it uses the default value documented in the `oozie-default.xml` file

**NOTE:** The `oozie-default.xml` file found in Oozie's `conf/` directory is not used by Oozie, it is there
for reference purposes only.

### Precedence of Configuration Properties

For compatibility reasons across Hadoop / Oozie versions, some configuration properties can be defined using multiple keys
in the launcher configuration. Beginning with Oozie 5.0.0, some of them can be overridden, some others will be prepended to default
configuration values.

#### Overriding Configuration Values

Overriding happens for following configuration entries with `oozie.launcher` prefix, by switching `oozie.launcher.override`
(on by default).

For those, following is the general approach:

   * check whether a YARN compatible entry is present. If yes, use it to override default value
   * check whether a MapReduce v2 compatible entry is present. If yes, use it to override default value
   * check whether a MapReduce v1 compatible entry is present. If yes, use it to override default value
   * use default value

Such properties are (legend: YARN / MapReduce v2 / MapReduce v1):

   * max attempts of the MapReduce Application Master:
      * N / A
      * `mapreduce.map.maxattempts`
      * `mapred.map.max.attempts`
   * memory amount in MB of the MapReduce Application Master:
      * `yarn.app.mapreduce.am.resource.mb`
      * `mapreduce.map.memory.mb`
      * `mapred.job.map.memory.mb`
   * CPU vcore count of the MapReduce Application Master:
      * `yarn.app.mapreduce.am.resource.cpu-vcores`
      * `mapreduce.map.cpu.vcores`
      * N / A
   * logging level of the MapReduce Application Master:
      * N / A
      * `mapreduce.map.log.level`
      * `mapred.map.child.log.level`
   * MapReduce Application Master JVM options:
      * `yarn.app.mapreduce.am.command-opts`
      * `mapreduce.map.java.opts`
      * `mapred.child.java.opts`
   * MapReduce Application Master environment variable settings:
      * `yarn.app.mapreduce.am.env`
      * `mapreduce.map.env`
      * `mapred.child.env`
   * MapReduce Application Master job priority:
      * N / A
      * `mapreduce.job.priority`
      * `mapred.job.priority`
   * MapReduce Application Master job queue name:
      * N / A
      * `mapreduce.job.queuename`
      * `mapred.job.queue.name`
   * MapReduce View ACL settings:
      * N / A
      * `mapreduce.job.acl-view-job`
      * N / A
   * MapReduce Modify ACL settings:
      * N / A
      * `mapreduce.job.acl-modify-job`
      * N / A

This list can be extended or modified by adding new configuration entries or updating existing values
beginning with `oozie.launcher.override.` within `oozie-site.xml`. Examples can be found in `oozie-default.xml`.

#### Prepending Configuration Values

Prepending happens for following configuration entries with `oozie.launcher` prefix, by switching `oozie.launcher.prepend`
(on by default).

For those, following is the general approach:

   * check whether a YARN compatible entry is present. If yes, use it to prepend to default value
   * use default value

Such properties are (legend: YARN only):

   * MapReduce Application Master JVM options: `yarn.app.mapreduce.am.admin-command-opts`
   * MapReduce Application Master environment settings: `yarn.app.mapreduce.am.admin.user.env`

This list can be extended or modified by adding new configuration entries or updating existing values
beginning with `oozie.launcher.prepend.` within `oozie-site.xml`. Examples can be found in `oozie-default.xml`.

### Logging Configuration

By default, Oozie log configuration is defined in the `oozie-log4j.properties` configuration file.

If the Oozie log configuration file changes, Oozie reloads the new settings automatically.

By default, Oozie logs to Oozie's `logs/` directory.

Oozie logs in 4 different files:

   * oozie.log: web services log streaming works from this log
   * oozie-ops.log: messages for Admin/Operations to monitor
   * oozie-instrumentation.log: instrumentation data, every 60 seconds (configurable)
   * oozie-audit.log: audit messages, workflow jobs changes

The embedded Jetty and embedded Derby log files are also written to Oozie's `logs/` directory.

### Oozie User Authentication Configuration

Oozie supports Kerberos HTTP SPNEGO authentication, pseudo/simple authentication and anonymous access
for client connections.

Anonymous access (**default**) does not require the user to authenticate and the user ID is obtained from
the job properties on job submission operations, other operations are anonymous.

Pseudo/simple authentication requires the user to specify the user name on the request, this is done by
the PseudoAuthenticator class by injecting the `user.name` parameter in the query string of all requests.
The `user.name` parameter value is taken from the client process Java System property `user.name`.

Kerberos HTTP SPNEGO authentication requires the user to perform a Kerberos HTTP SPNEGO authentication sequence.

If Pseudo/simple or Kerberos HTTP SPNEGO authentication mechanisms are used, Oozie will return the user an
authentication token HTTP Cookie that can be used in later requests as identity proof.

Oozie uses Apache Hadoop-Auth (Java HTTP SPNEGO) library for authentication.
This library can be extended to support other authentication mechanisms.

Oozie user authentication is configured using the following configuration properties (default values shown):


```
  oozie.authentication.type=simple
  oozie.authentication.token.validity=36000
  oozie.authentication.signature.secret=
  oozie.authentication.cookie.domain=
  oozie.authentication.simple.anonymous.allowed=true
  oozie.authentication.kerberos.principal=HTTP/localhost@${local.realm}
  oozie.authentication.kerberos.keytab=${oozie.service.HadoopAccessorService.keytab.file}
```

The `type` defines authentication used for Oozie HTTP endpoint, the supported values are:
simple | kerberos | #AUTHENTICATION_HANDLER_CLASSNAME#.

The `token.validity` indicates how long (in seconds) an authentication token is valid before it has
to be renewed.

The `signature.secret` is the signature secret for signing the authentication tokens. It is recommended to not set this, in which
case Oozie will randomly generate one on startup.

The `oozie.authentication.cookie.domain` The domain to use for the HTTP cookie that stores the
authentication token. In order to authentication to work correctly across all Hadoop nodes web-consoles
the domain must be correctly set.

The `simple.anonymous.allowed` indicates if anonymous requests are allowed. This setting is meaningful
only when using 'simple' authentication.

The `kerberos.principal` indicates the Kerberos principal to be used for HTTP endpoint.
The principal MUST start with 'HTTP/' as per Kerberos HTTP SPNEGO specification.

The `kerberos.keytab` indicates the location of the keytab file with the credentials for the principal.
It should be the same keytab file Oozie uses for its Kerberos credentials for Hadoop.

### Oozie Hadoop Authentication Configuration

Oozie works with Hadoop versions which support Kerberos authentication.

Oozie Hadoop authentication is configured using the following configuration properties (default values shown):


```
  oozie.service.HadoopAccessorService.kerberos.enabled=false
  local.realm=LOCALHOST
  oozie.service.HadoopAccessorService.keytab.file=${user.home}/oozie.keytab
  oozie.service.HadoopAccessorService.kerberos.principal=${user.name}/localhost@{local.realm}
```

The above default values are for a Hadoop 0.20 secure distribution (with support for Kerberos authentication).

To enable Kerberos authentication, the following property must be set:


```
  oozie.service.HadoopAccessorService.kerberos.enabled=true
```

When using Kerberos authentication, the following properties must be set to the correct values (default values shown):


```
  local.realm=LOCALHOST
  oozie.service.HadoopAccessorService.keytab.file=${user.home}/oozie.keytab
  oozie.service.HadoopAccessorService.kerberos.principal=${user.name}/localhost@{local.realm}
```

**IMPORTANT:** When using Oozie with a Hadoop 20 with Security distribution, the Oozie user in Hadoop must be configured
as a proxy user.

### User ProxyUser Configuration

Oozie supports impersonation or proxyuser functionality (identical to Hadoop proxyuser capabilities and conceptually
similar to Unix 'sudo').

Proxyuser enables other systems that are Oozie clients to submit jobs on behalf of other users.

Because proxyuser is a powerful capability, Oozie provides the following restriction capabilities
(similar to Hadoop):

   * Proxyuser is an explicit configuration on per proxyuser user basis.
   * A proxyuser user can be restricted to impersonate other users from a set of hosts.
   * A proxyuser user can be restricted to impersonate users belonging to a set of groups.

There are 2 configuration properties needed to set up a proxyuser:

   * oozie.service.ProxyUserService.proxyuser.#USER#.hosts: hosts from where the user #USER# can impersonate other users.
   * oozie.service.ProxyUserService.proxyuser.#USER#.groups: groups the users being impersonated by user #USER# must belong to.

Both properties support the '*' wildcard as value. Although this is recommended only for testing/development.

### User Authorization Configuration

Oozie has a basic authorization model:

   * Users have read access to all jobs
   * Users have write access to their own jobs
   * Users have write access to jobs based on an Access Control List (list of users and groups)
   * Users have read access to admin operations
   * Admin users have write access to all jobs
   * Admin users have write access to admin operations

If security is disabled all users are admin users.

Oozie security is set via the following configuration property (default value shown):


```
  oozie.service.AuthorizationService.security.enabled=false
```

NOTE: the old ACL model where a group was provided is still supported if the following property is set
in `oozie-site.xml`:


```
  oozie.service.AuthorizationService.default.group.as.acl=true
```

#### Defining Admin Users

Admin users are determined from the list of admin groups, specified in
 `oozie.service.AuthorizationService.admin.groups` property. Use commas to separate multiple groups, spaces, tabs
and ENTER characters are trimmed.

If the above property for admin groups is not set, then defining the admin users can happen in the following manners.
The list of admin users can be in the `conf/adminusers.txt` file. The syntax of this file is:

   * One user name per line
   * Empty lines and lines starting with '#' are ignored

Admin users can also be defined in
`oozie.serviceAuthorizationService.admin.users` property. Use commas to separate multiple admin users, spaces, tabs
and ENTER characters are trimmed.

In case there are admin users defined using both methods, the effective list of admin users will be the union
of the admin users found in the adminusers.txt and those specified with `oozie.serviceAuthorizationService.admin.users`.

#### Defining Access Control Lists

Access Control Lists are defined in the following ways:

   * workflow job submission over CLI: configuration property `group.name` of `job.properties`
   * workflow job submission over HTTP: configuration property `group.name` of the XML submitted over HTTP
   * workflow job re-run: configuration property `oozie.job.acl` (preferred) or configuration property `group.name` of
   `job.properties`
   * coordinator job submission over CLI: configuration property `oozie.job.acl` (preferred) or configuration property `group.name`
   of `job.properties`
   * bundle job submission over CLI: configuration property `oozie.job.acl` (preferred) or configuration property `group.name` of
   `job.properties`

For all other workflow, coordinator, or bundle actions the ACL set in beforehand will be used as basis.

Once the ACL for the job is defined, Oozie will check over HDFS whether the user trying to perform a specific action is part of the
necessary group(s). For implementation details please check out `org.apache.hadoop.security.Groups#getGroups(String user)`.

Note that it's enough that the submitting user be part of at least one group of the ACL. Note also that the ACL can contain user
names as well. If there is an ACL defined and the submitting user isn't part of any group or user name present in the ACL, an
`AuthorizationException` is thrown.

**Example: A typical ACL setup**

Detail of `job.properties` on workflow job submission:

```
user.name=joe
group.name=marketing,admin,qa,root
```

HDFS group membership of HDFS user `joe` is `qa`. That is, the check to `org.apache.hadoop.security.Groups#getGroups("joe")` returns
`qa`. Hence, ACL check will pass inside `AuthorizationService`, because the `user.name` provided belongs to at least of the ACL list
elements provided as `group.name`.

### Oozie System ID Configuration

Oozie has a system ID that is is used to generate the Oozie temporary runtime directory, the workflow job IDs, and the
workflow action IDs.

Two Oozie systems running with the same ID will not have any conflict but in case of troubleshooting it will be easier
to identify resources created/used by the different Oozie systems if they have different system IDs (default value
shown):


```
  oozie.system.id=oozie-${user.name}
```

### Filesystem Configuration

Oozie lets you to configure the allowed Filesystems by using the following configuration property in oozie-site.xml:

```
  <property>
    <name>oozie.service.HadoopAccessorService.supported.filesystems</name>
    <value>hdfs</value>
  </property>
```

The above value, `hdfs`, which is the default, means that Oozie will only allow HDFS filesystems to be used.  Examples of other
filesystems that Oozie is compatible with are: hdfs, hftp, webhdfs, and viewfs.  Multiple filesystems can be specified as
comma-separated values.  Putting a * will allow any filesystem type, effectively disabling this check.

### HCatalog Configuration

Refer to the [Oozie HCatalog Integration](DG_HCatalogIntegration.html) document for a overview of HCatalog and
integration of Oozie with HCatalog. This section explains the various settings to be configured in oozie-site.xml on
the Oozie server to enable Oozie to work with HCatalog.

**Adding HCatalog jars to Oozie war:**

 For Oozie server to talk to HCatalog server, HCatalog and hive jars need to be in the server classpath.
hive-site.xml which has the configuration to talk to the HCatalog server also needs to be in the classpath or specified by the
following configuration property in oozie-site.xml:

```
  <property>
    <name>oozie.service.HCatAccessorService.hcat.configuration</name>
    <value>/local/filesystem/path/to/hive-site.xml</value>
  </property>
```
The hive-site.xml can also be placed in a location on HDFS and the above property can have a value
of `hdfs://HOST:PORT/path/to/hive-site.xml` to point there instead of the local file system.

The oozie-[version]-hcataloglibs.tar.gz in the oozie distribution bundles the required hcatalog and hive jars that
needs to be placed in the Oozie server classpath. If using a version of HCatalog bundled in
Oozie hcataloglibs/, copy the corresponding HCatalog jars from hcataloglibs/ to the libext/ directory. If using a
different version of HCatalog, copy the required HCatalog jars from such version in the libext/ directory.
This needs to be done before running the `oozie-setup.sh` script so that these jars get added for Oozie.

**Configure HCatalog URI Handling:**


```
  <property>
    <name>oozie.service.URIHandlerService.uri.handlers</name>
    <value>org.apache.oozie.dependency.FSURIHandler,org.apache.oozie.dependency.HCatURIHandler</value>
    <description>
        Enlist the different uri handlers supported for data availability checks.
    </description>
  </property>
```

The above configuration defines the different uri handlers which check for existence of data dependencies defined in a
Coordinator. The default value is `org.apache.oozie.dependency.FSURIHandler`. FSURIHandler supports uris with
schemes defined in the configuration `oozie.service.HadoopAccessorService.supported.filesystems` which are hdfs, hftp
and webhcat by default. HCatURIHandler supports uris with the scheme as hcat.

**Configure HCatalog services:**


```
  <property>
    <name>oozie.services.ext</name>
    <value>
        org.apache.oozie.service.JMSAccessorService,
        org.apache.oozie.service.PartitionDependencyManagerService,
        org.apache.oozie.service.HCatAccessorService
      </value>
    <description>
          To add/replace services defined in 'oozie.services' with custom implementations.
          Class names must be separated by commas.
    </description>
  </property>
```

PartitionDependencyManagerService and HCatAccessorService are required to work with HCatalog and support Coordinators
having HCatalog uris as data dependency. If the HCatalog server is configured to publish partition availability
notifications to a JMS compliant messaging provider like ActiveMQ, then JMSAccessorService needs to be added
to `oozie.services.ext` to handle those notifications.

**Configure JMS Provider JNDI connection mapping for HCatalog:**


```
  <property>
    <name>oozie.service.HCatAccessorService.jmsconnections</name>
    <value>
      hcat://hcatserver.colo1.com:8020=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.colo1.com:61616,
      default=java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#tcp://broker.colo.com:61616;connectionFactoryNames#ConnectionFactory
    </value>
    <description>
        Specify the map  of endpoints to JMS configuration properties. In general, endpoint
        identifies the HCatalog server URL. "default" is used if no endpoint is mentioned
        in the query. If some JMS property is not defined, the system will use the property
        defined jndi.properties. jndi.properties files is retrieved from the application classpath.
        Mapping rules can also be provided for mapping Hcatalog servers to corresponding JMS providers.
        hcat://${1}.${2}.com:8020=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.${2}.com:61616
    </description>
  </property>
```

  Currently HCatalog does not provide APIs to get the connection details to connect to the JMS Provider it publishes
notifications to. It only has APIs which provide the topic name in the JMS Provider to which the notifications are
published for a given database table. So the JMS Provider's connection properties needs to be manually configured
in Oozie using the above setting. You can either provide a `default` JNDI configuration which will be used as the
JMS Provider for all HCatalog servers, or can specify a configuration per HCatalog server URL or provide a
configuration based on a rule matching multiple HCatalog server URLs. For example: With the configuration of
`hcat://${1}.${2}.com:8020=java.naming.factory.initial#Dummy.Factory;java.naming.provider.url#tcp://broker.${2}.com:61616`,
request URL of `hcat://server1.colo1.com:8020 `will map to`tcp://broker.colo1.com:61616`, `hcat://server2.colo2.com:8020`
will map to`tcp://broker.colo2.com:61616` and so on.

**Configure HCatalog Polling Frequency:**


```
  <property>
    <name>oozie.service.coord.push.check.requeue.interval
        </name>
    <value>600000</value>
    <description>Command re-queue interval for push dependencies (in millisecond).
    </description>
  </property>
```

  If there is no JMS Provider configured for a HCatalog Server, then oozie polls HCatalog based on the frequency defined
in `oozie.service.coord.input.check.requeue.interval`. This config also applies to HDFS polling.
If there is a JMS provider configured for a HCatalog Server, then oozie polls HCatalog based on the frequency defined
in `oozie.service.coord.push.check.requeue.interval` as a fallback.
The defaults for `oozie.service.coord.input.check.requeue.interval` and `oozie.service.coord.push.check.requeue.interval`
are 1 minute and 10 minutes respectively.

### Notifications Configuration

Oozie supports publishing notifications to a JMS Provider for job status changes and SLA met and miss events. For
more information on the feature, refer [JMS Notifications](DG_JMSNotifications.html) documentation. Oozie can also send email
notifications on SLA misses.

   * **Message Broker Installation**: <br/>
For Oozie to send/receive messages, a JMS-compliant broker should be installed. Apache ActiveMQ is a popular JMS-compliant
broker usable for this purpose. See [here](http://activemq.apache.org/getting-started.html) for instructions on
installing and running ActiveMQ.

   * **Services**: <br/>
Add/modify `oozie.services.ext` property in `oozie-site.xml` to include the following services.

```
     <property>
        <name>oozie.services.ext</name>
        <value>
            org.apache.oozie.service.JMSAccessorService,
            org.apache.oozie.service.JMSTopicService,
            org.apache.oozie.service.EventHandlerService,
            org.apache.oozie.sla.service.SLAService
        </value>
     </property>
```

   * **Event Handlers**: <br/>

```
     <property>
        <name>oozie.service.EventHandlerService.event.listeners</name>
        <value>
            org.apache.oozie.jms.JMSJobEventListener,
            org.apache.oozie.sla.listener.SLAJobEventListener,
            org.apache.oozie.jms.JMSSLAEventListener,
            org.apache.oozie.sla.listener.SLAEmailEventListener
        </value>
     </property>
```
It is also recommended to increase `oozie.service.SchedulerService.threads` to 15 for faster event processing and sending notifications. The services and their functions are as follows: <br/>
      JMSJobEventListener - Sends JMS job notifications <br/>
      JMSSLAEventListener - Sends JMS SLA notifications <br/>
      SLAEmailEventListener - Sends Email SLA notifications <br/>
      SLAJobEventListener - Processes job events and calculates SLA. Does not send any notifications

   * **JMS properties**:  <br/>
Add `oozie.jms.producer.connection.properties` property in `oozie-site.xml`. Its value corresponds to an
identifier (e.g. default) assigned to a semi-colon separated key#value list of properties from your JMS broker's
`jndi.properties` file. The important properties are `java.naming.factory.initial` and `java.naming.provider.url`.

As an example, if using ActiveMQ in local env, the property can be set to

```
     <property>
        <name>oozie.jms.producer.connection.properties</name>
        <value>
            java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#tcp://localhost:61616;connectionFactoryNames#ConnectionFactory
        </value>
     </property>
```
   * **JMS Topic name**: <br/>
JMS consumers listen on a particular "topic". Hence Oozie needs to define a topic variable with which to publish messages
about the various jobs.

```
     <property>
        <name>oozie.service.JMSTopicService.topic.name</name>
        <value>
            default=${username}
        </value>
        <description>
            Topic options are ${username}, ${jobId}, or a fixed string which can be specified as default or for a
            particular job type.
            For e.g To have a fixed string topic for workflows, coordinators and bundles,
            specify in the following comma-separated format: {jobtype1}={some_string1}, {jobtype2}={some_string2}
            where job type can be WORKFLOW, COORDINATOR or BUNDLE.
            Following example defines topic for workflow job, workflow action, coordinator job, coordinator action,
            bundle job and bundle action
            WORKFLOW=workflow,
            COORDINATOR=coordinator,
            BUNDLE=bundle
            For jobs with no defined topic, default topic will be ${username}
        </description>
     </property>
```

Another related property is the topic prefix.

```
     <property>
        <name>oozie.service.JMSTopicService.topic.prefix</name>
        <value></value>
        <description>
            This can be used to append a prefix to the topic in oozie.service.JMSTopicService.topic.name. For eg: oozie.
        </description>
     </property>
```


### Setting Up Oozie with HTTPS (SSL)

**IMPORTANT**:
The default HTTPS configuration will cause all Oozie URLs to use HTTPS except for the JobTracker callback URLs. This is to simplify
configuration (no changes needed outside of Oozie), but this is okay because Oozie doesn't inherently trust the callbacks anyway;
they are used as hints.

The related environment variables are explained at [Environment Setup](AG_Install.html#Environment_Setup).

You can use either a certificate from a Certificate Authority or a Self-Signed Certificate.  Using a self-signed certificate
requires some additional configuration on each Oozie client machine.  If possible, a certificate from a Certificate Authority is
recommended because it's simpler to configure.

There's also some additional considerations when using Oozie HA with HTTPS.

#### To use a Self-Signed Certificate
There are many ways to create a Self-Signed Certificate, this is just one way.  We will be using
the [keytool](http://docs.oracle.com/javase/6/docs/technotes/tools/solaris/keytool.html) program, which is
included with your JRE. If it's not on your path, you should be able to find it in $JAVA_HOME/bin.

1. Run the following command (as the Oozie user) to create the keystore file, which will be named `.keystore` and located in the
    Oozie user's home directory.

    ```
    keytool -genkeypair -alias jetty -keyalg RSA -dname "CN=hostname" -storepass password -keypass password
    ```
    The `hostname` should be the host name of the Oozie Server or a wildcard on the subdomain it belongs to.  Make sure to include
    the "CN=" part.  You can change `storepass` and `keypass` values, but they should be the same.  If you do want to use something
    other than password, you'll also need to change the value of the `oozie.https.keystore.pass` property in `oozie-site.xml` to
    match; `password` is the default.

    For example, if your Oozie server was at oozie.int.example.com, then you would do this:

    ```
    keytool -genkeypair -alias jetty -keyalg RSA -dname "CN=oozie.int.example.com" -storepass password -keypass password
    ```
    If you're going to be using Oozie HA, it's simplest if you have a single certificate that all Oozie servers in the HA group can use.
    To do that, you'll need to use a wildcard on the subdomain it belongs to:

    ```
    keytool -genkeypair -alias jetty -keyalg RSA -dname "CN=*.int.example.com" -storepass password -keypass password
    ```
    The above would work on any server in the int.example.com domain.

2. Run the following command (as the Oozie user) to export a certificate file from the keystore file:

    ```
    keytool -exportcert -alias jetty -file path/to/anywhere/certificate.cert -storepass password
    ```

3. Run the following command (as any user) to create a truststore containing the certificate we just exported:

    ```
    keytool -import -alias jetty -file path/to/certificate.cert -keystore /path/to/anywhere/oozie.truststore -storepass password2
    ```
    You'll need the `oozie.truststore` later if you're using the Oozie client (or other Java-based client); otherwise, you can skip
    this step.  The `storepass` value here is only used to verify or change the truststore and isn't typically required when only
    reading from it; so it does not have to be given to users only using the client.

#### To use a Certificate from a Certificate Authority

1. You will need to make a request to a Certificate Authority in order to obtain a proper Certificate; please consult a Certificate
    Authority on this procedure.  If you're going to be using Oozie HA, it's simplest if you have a single certificate that all Oozie
    servers in the HA group can use.  To do that, you'll need to use a wild on the subdomain it belongs to (e.g. "*.int.example.com").

2. Once you have your .cert file, run the following command (as the Oozie user) to create a keystore file from your certificate:

    ```
    keytool -import -alias jetty -file path/to/certificate.cert
    ```
    The keystore file will be named `.keystore` and located in the Oozie user's home directory.

#### Configure the Oozie Server to use SSL (HTTPS)

1. Make sure the Oozie server isn't running

2. Configure settings necessary for enabling SSL/TLS support in `oozie-site.xml`.

    2a. Set `oozie.https.enabled` to `true`. To revert back to HTTP, set `oozie.https.enabled` to `false`.

    2b. Set location and password for the keystore and location for truststore by setting `oozie.https.keystore.file`,
    `oozie.https.keystore.pass`, `oozie.https.truststore.file`.

    **Note:** `oozie.https.truststore.file` can be overridden by setting `javax.net.ssl.trustStore` system property.

    The default HTTPS port Oozie listens on for secure connections is 11443; it can be changed via `oozie.https.port`.

    It is possible to specify other HTTPS settings via `oozie-site.xml`:
    - To include / exclude cipher suites, set `oozie.https.include.cipher.suites` / `oozie.https.exclude.cipher.suites`.
    - To include / exclude TLS protocols, set `oozie.https.include.protocols` / `oozie.https.exclude.protocols`.
    **Note:** Exclude is always preferred over include (i.e. if you both include and exclude an entity, it will be excluded).

3. Start the Oozie server

    **Note:** If using Oozie HA, make sure that each Oozie server has a copy of the .keystore file.

#### Configure the Oozie Client to connect using SSL (HTTPS)

The first two steps are only necessary if you are using a Self-Signed Certificate; the third is required either way.
Also, these steps must be done on every machine where you intend to use the Oozie Client.

1. Copy or download the oozie.truststore file onto the client machine

2. When using any Java-based program, you'll need to pass `-Djavax.net.ssl.trustStore` to the JVM.  To
    do this for the Oozie client:

    ```
    export OOZIE_CLIENT_OPTS='-Djavax.net.ssl.trustStore=/path/to/oozie.truststore'
    ```

3. When using the Oozie Client, you will need to use `https://oozie.server.hostname:11443/oozie` instead of
    `http://oozie.server.hostname:11000/oozie` -- Java will not automatically redirect from the http address to the https address.

#### Connect to the Oozie Web UI using SSL (HTTPS)

1. Use `https://oozie.server.hostname:11443/oozie`
    though most browsers should automatically redirect you if you use `http://oozie.server.hostname:11000/oozie`

    **IMPORTANT**: If using a Self-Signed Certificate, your browser will warn you that it can't verify the certificate or something
    similar. You will probably have to add your certificate as an exception.

#### Additional considerations for Oozie HA with SSL

You'll need to configure the load balancer to do SSL pass-through.  This will allow the clients talking to Oozie to use the
SSL certificate provided by the Oozie servers (so the load balancer does not need one).  Please consult your load balancer's
documentation on how to configure this.  Make sure to point the load balancer at the `https://HOST:HTTPS_PORT` addresses for your
Oozie servers.  Clients can then connect to the load balancer at `https://LOAD_BALANCER_HOST:PORT`.

**Important:** Callbacks from the ApplicationMaster are done via http or https depending on what you enter for the
`OOZIE_BASE_URL` property.  If you are using a Certificate from a Certificate Authority, you can simply put the https address here.
If you are using a self-signed certificate, you have to do one of the following options (Option 1 is recommended):

Option 1) You'll need to follow the steps in
the [Configure the Oozie Client to connect using SSL (HTTPS)](AG_Install.html#Configure_the_Oozie_Client_to_connect_using_SSL_HTTPS)
section, but on the host of the ApplicationMaster.  You can then set `OOZIE_BASE_URL` to the load balancer https address.
This will allow the ApplicationMaster to contact the Oozie server with https (like the Oozie client, they are also Java
programs).

Option 2) You'll need setup another load balancer, or another "pool" on the existing load balancer, with the http addresses of the
Oozie servers.  You can then set `OOZIE_BASE_URL` to the load balancer http address.  Clients should use the https load balancer
address.  This will allow clients to use https while the ApplicationMaster uses http for callbacks.

### Fine Tuning an Oozie Server

Refer to the [oozie-default.xml](./oozie-default.xml) for details.

### Using Instrumentation instead of Metrics

As of version 4.1.0, Oozie includes a replacement for the Instrumentation based on Codahale's Metrics library.  It includes a
number of improvements over the original Instrumentation included in Oozie.  They both report most of the same information, though
the formatting is slightly different and there's some additional information in the Metrics version; the format of the output to the
oozie-instrumentation log is also different.

As of version 5.0.0, `MetricsInstrumentationService` is the default one, it's enlisted in `oozie.services`:

```
    <property>
        <name>oozie.services</name>
        <value>
            ...
            org.apache.oozie.service.MetricsInstrumentationService,
            ...
        </value>
     </property>
```

The deprecated `InstrumentationService` can be enabled by adding `InstrumentationService` reference to the list of
`oozie.services.ext`:

```
    <property>
        <name>oozie.services.ext</name>
        <value>
            ...
            org.apache.oozie.service.InstrumentationService,
            ...
        </value>
     </property>
```

By default the `admin/instrumentation` REST endpoint is no longer be available and instead the `admin/metrics` endpoint can
be used (see the [Web Services API](WebServicesAPI.html#Oozie_Metrics) documentation for more details); the Oozie Web UI also replaces
the "Instrumentation" tab with a "Metrics" tab.

If the deprecated `InstrumentationService` is used, the `admin/instrumentation` REST endpoint gets enabled, the `admin/metrics`
REST endpoint is no longer available (see the [Web Services API](WebServicesAPI.html#Oozie_Metrics) documentation for more details);
the Oozie Web UI also replaces the "Metrics" tab with the "Instrumentation" tab.

We can also publish the instrumentation metrics to the external server graphite or ganglia. For this the following
properties should be specified in oozie-site.xml :

```
    <property>
        <name>oozie.external_monitoring.enable</name>
        <value>false</value>
        <description>
            If the oozie functional metrics needs to be exposed to the metrics-server backend, set it to true
            If set to true, the following properties has to be specified : oozie.metrics.server.name,
            oozie.metrics.host, oozie.metrics.prefix, oozie.metrics.report.interval.sec, oozie.metrics.port
        </description>
    </property>

    <property>
        <name>oozie.external_monitoring.type</name>
        <value>graphite</value>
        <description>
            The name of the server to which we want to send the metrics, would be graphite or ganglia.
        </description>
    </property>

    <property>
        <name>oozie.external_monitoring.address</name>
        <value>http://localhost:2020</value>
    </property>

    <property>
        <name>oozie.external_monitoring.metricPrefix</name>
        <value>oozie</value>
    </property>

    <property>
        <name>oozie.external_monitoring.reporterIntervalSecs</name>
        <value>60</value>
    </property>
```

We can also publish the instrumentation metrics via JMX interface. For this the following property should be specified
in oozie-site.xml :

```
    <property>
         <name>oozie.jmx_monitoring.enable</name>
         <value>false</value>
         <description>
             If the oozie functional metrics needs to be exposed via JMX interface, set it to true.
         </description>
     </property>>
```

<a name="HA"></a>
### High Availability (HA)

Multiple Oozie Servers can be configured against the same database to provide High Availability (HA) of the Oozie service.

#### Pre-requisites

1. A database that supports multiple concurrent connections.  In order to have full HA, the database should also have HA support, or
    it becomes a single point of failure.

    **NOTE:** The default derby database does not support this

2. A ZooKeeper ensemble.

    Apache ZooKeeper is a distributed, open-source coordination service for distributed applications; the Oozie servers use it for
    coordinating access to the database and communicating with each other.  In order to have full HA, there should be at least 3
    ZooKeeper servers.
    More information on ZooKeeper can be found [here](http://zookeeper.apache.org).

3. Multiple Oozie servers.

    **IMPORTANT:** While not strictly required for all configuration properties, all of the servers should ideally have exactly the same
    configuration for consistency's sake.

4. A Loadbalancer, Virtual IP, or Round-Robin DNS.

    This is used to provide a single entry-point for users and for callbacks from the JobTracker/ResourceManager.  The load balancer
    should be configured for round-robin between the Oozie servers to distribute the requests.  Users (using either the Oozie client, a
    web browser, or the REST API) should connect through the load balancer.  In order to have full HA, the load balancer should also
    have HA support, or it becomes a single point of failure.

#### Installation/Configuration Steps

1. Install identically configured Oozie servers normally.  Make sure they are all configured against the same database and make sure
    that you DO NOT start them yet.

2. Add the following services to the extension services configuration property in oozie-site.xml in all Oozie servers.  This will
    make Oozie use the ZooKeeper versions of these services instead of the default implementations.


    ```
    <property>
        <name>oozie.services.ext</name>
        <value>
            org.apache.oozie.service.ZKLocksService,
            org.apache.oozie.service.ZKXLogStreamingService,
            org.apache.oozie.service.ZKJobsConcurrencyService,
            org.apache.oozie.service.ZKUUIDService
        </value>
    </property>
    ```

3. Add the following property to oozie-site.xml in all Oozie servers.  It should be a comma-separated list of host:port pairs of the
    ZooKeeper servers.  The default value is shown below.


    ```
    <property>
       <name>oozie.zookeeper.connection.string</name>
       <value>localhost:2181</value>
    </property>
    ```

4. (Optional) Add the following property to oozie-site.xml in all Oozie servers to specify the namespace to use.  All of the Oozie
    Servers that are planning on talking to each other should have the same namespace.  If there are multiple Oozie setups each doing
    their own HA, they should have their own namespace.  The default value is shown below.


    ```
    <property>
        <name>oozie.zookeeper.namespace</name>
        <value>oozie</value>
    </property>
    ```

5. Change the value of `OOZIE_BASE_URL` in oozie-site.xml to point to the loadbalancer or virtual IP, for example:


    ```
    <property>
        <name>oozie.base.url</name>
        <value>http://my.loadbalancer.hostname:11000/oozie</value>
    </property>
    ```

6. (Optional) If using a secure cluster, see [Security](AG_Install.html#Security) below on configuring Kerberos with Oozie HA.

7. Start the ZooKeeper servers.

8. Start the Oozie servers.

    Note: If one of the Oozie servers becomes unavailable, querying Oozie for the logs from a job in the Web UI, REST API, or client may
    be missing information until that server comes back up.

#### Security

Oozie HA works with the existing Oozie security framework and settings. For HA features (log streaming, share lib, etc) to work
properly in a secure setup, following property can be set on each server. If `oozie.server.authentication.type` is not set, then
server-server authentication will fall back on `oozie.authentication.type`.


```
<property>
    <name>oozie.server.authentication.type</name>
    <value>kerberos</value>
</property>
```

Below are some additional steps and information specific to Oozie HA:

1. (Optional) To prevent unauthorized users or programs from interacting with or reading the znodes used by Oozie in ZooKeeper,
    you can tell Oozie to use Kerberos-backed ACLs.  To enforce this for all of the Oozie-related znodes, simply add the following
    property to oozie-site.xml in all Oozie servers and set it to `true`.  The default is `false`.


    ```
    <property>
        <name>oozie.zookeeper.secure</name>
        <value>true</value>
    </property>
    ```

    Note: The Kerberos principals of each of the Oozie servers should have the same primary name (i.e. in `primary/instance@REALM`, each
    server should have the same value for `primary`).

    **Important:** Once this property is set to `true`, it will set the ACLs on all existing Oozie-related znodes to only allow Kerberos
    authenticated users with a principal that has the same primary as described above (also for any subsequently created new znodes).
    This means that if you ever want to turn this feature off, you will have to manually connect to ZooKeeper using a Kerberos principal
    with the same primary and either delete all znodes under and including the namespace (i.e. if `oozie.zookeeper.namespace` ` `oozie=
    then that would be `/oozie`); alternatively, instead of deleting them all, you can manually set all of their ACLs to `world:anyone`.
    In either case, make sure that no Oozie servers are running while this is being done.

    Also, in your zoo.cfg for ZooKeeper, make sure to set the following properties:

    ```
    authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
    kerberos.removeHostFromPrincipal=true
    kerberos.removeRealmFromPrincipal=true
    ```

2. Until Hadoop 2.5.0 and later, there is a known limitation where each Oozie server can only use one HTTP principal.  However,
    for Oozie HA, we need to use two HTTP principals: `HTTP/oozie-server-host@realm` and `HTTP/load-balancer-host@realm`.  This
    allows access to each Oozie server directly and through the load balancer.  While users should always go through the load balancer,
    certain features (e.g. log streaming) require the Oozie servers to talk to each other directly; it can also be helpful for an
    administrator to talk directly to an Oozie server.  So, if using a Hadoop version prior to 2.5.0, you will have to choose which
    HTTP principal to use as you cannot use both; it is recommended to choose `HTTP/load-balancer-host@realm` so users can connect
    through the load balancer.  This will prevent Oozie servers from talking to each other directly, which will effectively disable
    log streaming.

    For Hadoop 2.5.0 and later:

    2a. When creating the keytab used by Oozie, make sure to include Oozie's principal and the two HTTP principals mentioned above.


    2b. Set `oozie.authentication.kerberos.principal` to * (that is, an asterisks) so it will use both HTTP principals.

    For earlier versions of Hadoop:

    2a. When creating the keytab used by Oozie, make sure to include Oozie's principal and the load balancer HTTP principal


    2b. Set `oozie.authentication.kerberos.principal` to `HTTP/load-balancer-host@realm`.

3. With Hadoop 2.6.0 and later, a rolling random secret that is synchronized across all Oozie servers will be used for signing the
    Oozie auth tokens.  This is done automatically when HA is enabled; no additional configuration is needed.

    For earlier versions of Hadoop, each server will have a different random secret.  This will still work but will likely result in
    additional calls to the KDC to authenticate users to the Oozie server (because the auth tokens will not be accepted by other
    servers, which will cause a fallback to Kerberos).

4. If you'd like to use HTTPS (SSL) with Oozie HA, there's some additional considerations that need to be made.
    See the [Setting Up Oozie with HTTPS (SSL)](AG_Install.html#Setting_Up_Oozie_with_HTTPS_SSL) section for more information.

#### JobId sequence
Oozie in HA mode, uses ZK to generate job id sequence. Job Ids are of following format.
`<Id sequence>-<yyMMddHHmmss(server start time)>-<system_id>-<W/C/B>`

Where, `<systemId>` is configured as `oozie.system.id` (default is "oozie-" + "user.name")
W/C/B is suffix to job id indicating that generated job is a type of workflow or coordinator or bundle.

Maximum allowed character for job id sequence is 40. "Id sequence" is stored in ZK and reset to 0 once maximum job id sequence is
reached. Maximum job id sequence is configured as `oozie.service.ZKUUIDService.jobid.sequence.max`, default value is 99999999990.


```
<property>
    <name>oozie.service.ZKUUIDService.jobid.sequence.max</name>
    <value>99999999990</value>
</property>
```

## Starting and Stopping Oozie

Use the standard commands to start and stop Oozie.

## Oozie Command Line Installation

Copy and expand the `oozie-client` TAR.GZ file bundled with the distribution. Add the `bin/` directory to the `PATH`.

Refer to the [Command Line Interface Utilities](DG_CommandLineTool.html) document for a full reference of the `oozie`
command line tool.

## Oozie Share Lib

The Oozie sharelib TAR.GZ file bundled with the distribution contains the necessary files to run Oozie map-reduce streaming, pig,
hive, sqooop, and distcp actions.  There is also a sharelib for HCatalog.  The sharelib is required for these actions to work; any
other actions (mapreduce, shell, ssh, and java) do not require the sharelib to be installed.

As of Oozie 4.0, the following property is included.  If true, Oozie will create and ship a "launcher jar" to hdfs that contains
classes necessary for the launcher job.  If false, Oozie will not do this, and it is assumed that the necessary classes are in their
respective sharelib jars or the "oozie" sharelib instead.  When false, the sharelib is required for ALL actions; when true, the
sharelib is only required for actions that need additional jars (the original list from above).


```
<property>
    <name>oozie.action.ship.launcher.jar</name>
    <value>true</value>
</property>
```

Using sharelib CLI, sharelib files are copied to new lib_`<timestamped>` directory. At start, server picks the sharelib from latest
time-stamp directory. While starting, server also purges sharelib directory which are older than sharelib retention days
(defined as oozie.service.ShareLibService.temp.sharelib.retention.days and 7 days is default).

Sharelib mapping file can be also configured. Configured file is a key value mapping, where key will be the sharelib name for the
action and value is a comma separated list of DFS or local filesystem directories or jar files. Local filesystem refers to the local
filesystem of the node where the Oozie launcher is running. This can be configured in oozie-site.xml as :

```
  <!-- OOZIE -->
    <property>
        <name>oozie.service.ShareLibService.mapping.file</name>
        <value></value>
        <description>
            Sharelib mapping files contains list of key=value,
            where key will be the sharelib name for the action and value is a comma separated list of
            DFS or local filesystem directories or jar files.
            Example.
            oozie.pig_10=hdfs:///share/lib/pig/pig-0.10.1/lib/
            oozie.pig=hdfs:///share/lib/pig/pig-0.11.1/lib/
            oozie.distcp=hdfs:///share/lib/hadoop-2.2.0/share/hadoop/tools/lib/hadoop-distcp-2.2.0.jar
            oozie.spark=hdfs:///share/lib/spark/lib/,hdfs:///share/lib/spark/python/lib/pyspark.zip,hdfs:///share/lib/spark/python/lib/py4j-0-9-src.zip
            oozie.hive=file:///usr/local/oozie/share/lib/hive/
        </description>
    </property>
```

Example mapping file with local filesystem resources:


```
    <property>
        <name>oozie.service.ShareLibService.mapping.file</name>
        <value>
            oozie.distcp=file:///usr/local/oozie/share/lib/distcp
            oozie.hcatalog=file:///usr/local/oozie/share/lib/hcatalog
            oozie.hive=file:///usr/local/oozie/share/lib/hive
            oozie.hive2=file:///usr/local/oozie/share/lib/hive2
            oozie.mapreduce-streaming=file:///usr/local/oozie/share/lib/mapreduce-streaming
            oozie.oozie=file://usr/local/oozie/share/lib/oozie
            oozie.pig=file:///usr/local/oozie/share/lib/pig
            oozie.spark=file:///usr/local/oozie/share/lib/spark
            oozie.sqoop=file:///usr/localoozie/share/lib/sqoop
        </value>
    </property>
```

If you are using local filesystem resources in the mapping file, make sure corresponding jars are already deployed to
all the nodes where Oozie launcher jobs will be executed, and the files are readable by the launchers. To do this, you
can extract Oozie sharelib TAR.GZ file in the directory of your choice on the nodes, and set permission of the files.

Oozie sharelib TAR.GZ file bundled with the distribution does not contain pyspark and py4j zip files since they vary
with Apache Spark version. Therefore, to run pySpark using Spark Action, user need to specify pyspark and py4j zip
files. These files can be added either to workflow's lib/ directory, to the sharelib or in sharelib mapping file.


## Oozie Coordinators/Bundles Processing Timezone

By default Oozie runs coordinator and bundle jobs using `UTC` timezone for datetime values specified in the application
XML and in the job parameter properties. This includes coordinator applications start and end times of jobs, coordinator
datasets initial-instance, and bundle applications kickoff times. In addition, coordinator dataset instance URI templates
will be resolved using datetime values of the Oozie processing timezone.

It is possible to set the Oozie processing timezone to a timezone that is an offset of UTC, alternate timezones must
expressed in using a GMT offset ( `GMT+/-#### ` ). For example: `GMT+0530` (India timezone).

To change the default `UTC` timezone, use the `oozie.processing.timezone` property in the `oozie-site.xml`. For example:


```
<configuration>
    <property>
        <name>oozie.processing.timezone</name>
        <value>GMT+0530</value>
    </property>
</configuration>
```

**IMPORTANT:** If using a processing timezone other than `UTC`, all datetime values in coordinator and bundle jobs must
be expressed in the corresponding timezone, for example `2012-08-08T12:42+0530`.

**NOTE:** It is strongly encouraged to use `UTC`, the default Oozie processing timezone.

For more details on using an alternate Oozie processing timezone, please refer to the
[Coordinator Functional Specification, section '4. Datetime'](CoordinatorFunctionalSpec.html#datetime)

<a name="UberJar"></a>
## MapReduce Workflow Uber Jars
For Map-Reduce jobs (not including streaming or pipes), additional jar files can also be included via an uber jar. An uber jar is a
jar file that contains additional jar files within a "lib" folder (see
[Workflow Functional Specification](WorkflowFunctionalSpec.html#AppDeployment) for more information). Submitting a workflow with an uber jar
requires at least Hadoop 2.2.0 or 1.2.0. As such, using uber jars in a workflow is disabled by default. To enable this feature, use
the `oozie.action.mapreduce.uber.jar.enable` property in the `oozie-site.xml` (and make sure to use a supported version of Hadoop).


```
<configuration>
    <property>
        <name>oozie.action.mapreduce.uber.jar.enable</name>
        <value>true</value>
    </property>
</configuration>
```

## Advanced/Custom Environment Settings

Oozie can be configured to use Unix standard filesystem hierarchy for its different files
(configuration, logs, data and temporary files).

These settings must be done in the `bin/oozie-env.sh` script.

This script is sourced before the configuration `oozie-env.sh` and supports additional
environment variables (shown with their default values):


```
export OOZIE_CONFIG=${OOZIE_HOME}/conf
export OOZIE_DATA={OOZIE_HOME}/data
export OOZIE_LOG={OOZIE_HOME}/logs
export JETTY_OUT=${OOZIE_LOGS}/jetty.out
export JETTY_PID=/tmp/oozie.pid
```

Sample values to make Oozie follow Unix standard filesystem hierarchy:


```
export OOZIE_CONFIG=/etc/oozie
export OOZIE_DATA=/var/lib/oozie
export OOZIE_LOG=/var/log/oozie
export JETTY_PID=/tmp/oozie.pid
```

[::Go back to Oozie Documentation Index::](index.html)


