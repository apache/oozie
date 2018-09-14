

[::Go back to Oozie Documentation Index::](index.html)

# HCatalog Integration (Since Oozie 4.x)

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## HCatalog Overview
HCatalog is a table and storage management layer for Hadoop that enables users with different data processing
tools - Pig, MapReduce, and Hive - to more easily read and write data on the grid. HCatalog's table abstraction presents
users with a relational view of data in the Hadoop distributed file system (HDFS).

Read [HCatalog Documentation](http://incubator.apache.org/hcatalog/docs/r0.5.0/index.html) to know more about HCatalog.
Working with HCatalog using pig is detailed in
[HCatLoader and HCatStorer](http://incubator.apache.org/hcatalog/docs/r0.5.0/loadstore.html).
Working with HCatalog using MapReduce directly is detailed in
[HCatInputFormat and HCatOutputFormat](http://incubator.apache.org/hcatalog/docs/r0.5.0/inputoutput.html).

### HCatalog notifications
   HCatalog provides notifications through a JMS provider like ActiveMQ when a new partition is added to a table in the
database. This allows applications to consume those events and schedule the work that depends on them. In case of Oozie,
the notifications are used to determine the availability of HCatalog partitions defined as data dependencies in the
Coordinator and trigger workflows.

Read [HCatalog Notification](http://incubator.apache.org/hcatalog/docs/r0.5.0/notification.html) to know more about
notifications in HCatalog.

## Oozie HCatalog Integration
   Oozie's Coordinators so far have been supporting HDFS directories as a input data dependency. When a HDFS URI
template is specified as a dataset and input events are defined in Coordinator for the dataset, Oozie performs data
availability checks by polling the HDFS directory URIs resolved based on the nominal time. When all the data
dependencies are met, the Coordinator's workflow is triggered which then consumes the available HDFS data.

With addition of HCatalog support, Coordinators also support specifying a set of HCatalog tables or table partitions as a dataset.
The workflow is triggered when the HCatalog table partitions are available and the workflow actions can then read the
partition data. A mix of HDFS and HCatalog dependencies can be specified as input data dependencies.
Similar to HDFS directories, HCatalog table partitions can also be specified as output dataset events.

With HDFS data dependencies, Oozie has to poll HDFS every time to determine the availability of a directory.
If the HCatalog server is configured to publish partition availability notifications to a JMS provider, Oozie can be
configured to subscribe to it and trigger jobs immediately. This pub-sub model reduces pressure on Namenode and also
cuts down on delays caused by polling intervals.

In the absence of a message bus in the deployment, Oozie will always
poll the HCatalog server directly for partition availability with the same frequency as the HDFS polling. Even when
subscribed to notifications, Oozie falls back to polling HCatalog server for partitions that were available before the
coordinator action was materialized and to deal with missed notifications due to system downtimes. The frequency of the
fallback polling is usually lower than the constant polling. Defaults are 10 minutes and 1 minute respectively.


### Oozie Server Configuration
   Refer to [HCatalog Configuration](AG_Install.html#HCatalog_Configuration) section of [Oozie Install](AG_Install.html)
documentation for the Oozie server side configuration required to support HCatalog table partitions as a data dependency.

### HCatalog URI Format

Oozie supports specifying HCatalog partitions as a data dependency through a URI notation. The HCatalog partition URI is
used to identify a set of table partitions: `hcat://bar:8020/logsDB/logsTable/dt=20090415;region=US`

The format to specify a HCatalog table URI is:

hcat://[metastore server]:[port]/[database name]/[table name]

The format to specify a HCatalog table partition URI is:

hcat://[metastore server]:[port]/[database name]/[table name]/[partkey1]=[value];[partkey2]=[value];...

For example,

```
  <dataset name="logs" frequency="${coord:days(1)}"
           initial-instance="2009-02-15T08:15Z" timezone="America/Los_Angeles">
    <uri-template>
      hcat://myhcatmetastore:9080/database1/table1/datestamp=${YEAR}${MONTH}${DAY}${HOUR};region=USA
    </uri-template>
  </dataset>
```

Post Oozie-4.3.0 release, Oozie also supports the multiple HCatalog servers in the URI. Each of the server needs to be
separated by single comma (,).

The format to specify a HCatalog table partition URI with multiple HCatalog server is:

hcat://[metastore_server]:[port],[metastore_server]:[port]/[database_name]/[table_name]/[partkey1]=[value];[partkey2]=[value];...

For example,

```
  <dataset name="logs" frequency="${coord:days(1)}"
           initial-instance="2009-02-15T08:15Z" timezone="America/Los_Angeles">
    <uri-template>
      hcat://myhcatmetastore:9080,myhcatmetastore:9080/database1/table1/datestamp=${YEAR}${MONTH}${DAY}${HOUR};region=USA
    </uri-template>
  </dataset>
```

The regex for parsing the multiple HCatalog URI is exposed via oozie-site.xml, So Users can modify if there is any
requirement. Key for the regex is: `oozie.hcat.uri.regex.pattern`

For example, following has multiple HCatalog URI with multiple HCatalog servers. To understand this, Oozie will split them into
two HCatalog URIs. For splitting the URIs, above mentioned regex is used.

`hcat://hostname1:1000,hcat://hostname2:2000/mydb/clicks/datastamp=12;region=us,scheme://hostname3:3000,scheme://hostname4:4000,scheme://hostname5:5000/db/table/p1=12;p2=us`

After split: (This is internal Oozie mechanism)

`hcat://hostname1:1000,hcat://hostname2:2000/mydb/clicks/datastamp=12;region=us`

`scheme://hostname3:3000,scheme://hostname4:4000,scheme://hostname5:5000/db/table/p1=12;p2=us`

<a name="HCatalogLibraries"></a>
### HCatalog Libraries

A workflow action interacting with HCatalog requires the following jars in the classpath:
hcatalog-core.jar, hcatalog-pig-adapter.jar, webhcat-java-client.jar, hive-common.jar, hive-exec.jar,
hive-metastore.jar, hive-serde.jar and libfb303.jar.
hive-site.xml which has the configuration to talk to the HCatalog server also needs to be in the classpath. The correct
version of HCatalog and hive jars should be placed in classpath based on the version of HCatalog installed on the cluster.

The jars can be added to the classpath of the action using one of the below ways.

   * You can place the jars and hive-site.xml in the system shared library. The shared library for a pig, hive or java action can be overridden to include hcatalog shared libraries along with the action's shared library. Refer to [Shared Libraries](WorkflowFunctionalSpec.html#a17_HDFS_Share_Libraries_for_Workflow_Applications_since_Oozie_2.3) for more information. The oozie-sharelib-[version].tar.gz in the oozie distribution bundles the required HCatalog jars in a hcatalog sharelib. If using a different version of HCatalog than the one bundled in the sharelib, copy the required HCatalog jars from such version into the sharelib.
   * You can place the jars and hive-site.xml in the workflow application lib/ path.
   * You can specify the location of the jar files in `archive` tag and the hive-site.xml in `file` tag in the corresponding pig, hive or java action.

### Coordinator

Refer to [Coordinator Functional Specification](CoordinatorFunctionalSpec.html) for more information about

   * how to specify HCatalog partitions as a data dependency using input dataset events
   * how to specify HCatalog partitions as output dataset events
   * the various EL functions available to work with HCatalog dataset events and how to use them to access HCatalog partitions in pig, hive or java actions in a workflow.

### Workflow
Refer to [Workflow Functional Specification](WorkflowFunctionalSpec.html) for more information about

   * how to drop HCatalog table/partitions in the prepare block of a action
   * the HCatalog EL functions available to use in workflows

Refer to [Action Authentication](DG_ActionAuthentication.html) for more information about

   * how to access a secure HCatalog from any action (e.g. hive, pig, etc) in a workflow

### Known Issues
   * When rerunning a coordinator action without specifying -nocleanup option if the 'output-event' are hdfs directories, then they are deleted. But if the 'output-event' is a hcatalog partition, currently the partition is not dropped.

