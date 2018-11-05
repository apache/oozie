

[::Go back to Oozie Documentation Index::](index.html)

-----

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Oozie Web Services API, V1 (Workflow, Coordinator, And Bundle)

The Oozie Web Services API is a HTTP REST JSON API.

All responses are in `UTF-8`.

Assuming Oozie is running at `OOZIE_URL`, the following web services end points are supported:

   * \<OOZIE_URL\>/versions
   * \<OOZIE_URL\>/v1/admin
   * \<OOZIE_URL\>/v1/job
   * \<OOZIE_URL\>/v1/jobs
   * \<OOZIE_URL\>/v2/job
   * \<OOZIE_URL\>/v2/jobs
   * \<OOZIE_URL\>/v2/admin
   * \<OOZIE_URL\>/v2/sla

Documentation on the API is below; in some cases, looking at the corresponding command in the
[Command Line Documentation](DG_CommandLineTool.html) page will provide additional details and examples.  Most of the functionality
offered by the Oozie CLI is using the WS API. If you export `OOZIE_DEBUG` then the Oozie CLI will output the WS API
details used by any commands you execute. This is useful for debugging purposes to or see how the Oozie CLI works with the WS API.

### Versions End-Point

_Identical to the corresponding Oozie v0 WS API_

This endpoint is for clients to perform protocol negotiation.

It support only HTTP GET request and not sub-resources.

It returns the supported Oozie protocol versions by the server.

Current returned values are `0, 1, 2`.

**Request:**


```
GET /oozie/versions
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
[0,1]
```

### Admin End-Point

This endpoint is for obtaining Oozie system status and configuration information.

It supports the following sub-resources: `status, os-env, sys-props, configuration, instrumentation, systems, available-timezones`.

#### System Status

_Identical to the corresponding Oozie v0 WS API_

A HTTP GET request returns the system status.

**Request:**


```
GET /oozie/v1/admin/status
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{"systemMode":NORMAL}
```

With a HTTP PUT request it is possible to change the system status between `NORMAL`, `NOWEBSERVICE`, and `SAFEMODE`.

**Request:**


```
PUT /oozie/v1/admin/status?systemmode=SAFEMODE
```

**Response:**


```
HTTP/1.1 200 OK
```

#### OS Environment

_Identical to the corresponding Oozie v0 WS API_

A HTTP GET request returns the Oozie system OS environment.

**Request:**


```
GET /oozie/v1/admin/os-env
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  TERM: "xterm",
  JAVA_HOME: "/usr/java/latest",
  XCURSOR_SIZE: "",
  SSH_CLIENT: "::ffff:127.0.0.1 49082 22",
  XCURSOR_THEME: "default",
  INPUTRC: "/etc/inputrc",
  HISTSIZE: "1000",
  PATH: "/usr/java/latest/bin"
  KDE_FULL_SESSION: "true",
  LANG: "en_US.UTF-8",
  ...
}
```

#### Java System Properties

_Identical to the corresponding Oozie v0 WS API_

A HTTP GET request returns the Oozie Java system properties.

**Request:**


```
GET /oozie/v1/admin/java-sys-properties
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  java.vm.version: "11.0-b15",
  sun.jnu.encoding: "UTF-8",
  java.vendor.url: "http://java.sun.com/",
  java.vm.info: "mixed mode",
  ...
}
```

#### Oozie Configuration

_Identical to the corresponding Oozie v0 WS API_

A HTTP GET request returns the Oozie system configuration.

**Request:**


```
GET /oozie/v1/admin/configuration
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  oozie.service.SchedulerService.threads: "5",
  oozie.service.ActionService.executor.classes: "
            org.apache.oozie.dag.action.decision.DecisionActionExecutor,
            org.apache.oozie.dag.action.hadoop.HadoopActionExecutor,
            org.apache.oozie.dag.action.hadoop.FsActionExecutor
        ",
  oozie.service.CallableQueueService.threads.min: "10",
  oozie.service.DBLiteWorkflowStoreService.oozie.autoinstall: "true",
  ...
}
```

#### Oozie Instrumentation

_Identical to the corresponding Oozie v0 WS API_

Deprecated and by default disabled since 5.0.0.

A HTTP GET request returns the Oozie instrumentation information.  Keep in mind that timers and counters that the Oozie server
hasn't incremented yet will not show up.

**Note:** If Instrumentation is enabled, then Metrics is unavailable.

**Request:**


```
GET /oozie/v1/admin/instrumentation
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  timers: [
    {
      group: "db",
      data: [
        {
          ownMinTime: 2,
          ownTimeStdDev: 0,
          totalTimeStdDev: 0,
          ownTimeAvg: 3,
          ticks: 117,
          name: "update-workflow",
          ownMaxTime: 32,
          totalMinTime: 2,
          totalMaxTime: 32,
          totalTimeAvg: 3
        },
        ...
      ]
    },
    ...
  ],
  samplers: [
    {
      group: "callablequeue",
      data: [
        {
          name: "threads.active",
          value: 1.8333333333333333
        },
        {
          name: "delayed.queue.size",
          value: 0
        },
        {
          name: "queue.size",
          value: 0
        }
      ]
    },
    ...
  ],
  variables: [
    {
      group: "jvm",
      data: [
        {
          name: "max.memory",
          value: 506920960
        },
        {
          name: "total.memory",
          value: 56492032
        },
        {
          name: "free.memory",
          value: 45776800
        }
      ]
    },
    ...
  ]
}
```

#### Oozie Metrics

_Available in the Oozie v2 WS API and later_

A HTTP GET request returns the Oozie metrics information.  Keep in mind that timers and counters that the Oozie server
hasn't incremented yet will not show up.


**Note:** If Metrics is enabled, then Instrumentation is unavailable.

**Note:** by default enabled since 5.0.0.

**Request:**


```
GET /oozie/v2/admin/metrics
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
   "gauges" : {
        "jvm.memory.non-heap.committed" : {
          "value" : 62590976
        },
        "oozie.mode" : {
          "value" : "NORMAL"
        },
        ...
    },
    "timers" : {
        "commands.action.end.call.timer" : {
          "mean" : 108.5,
          "p50" : 111.5,
          "p75" : 165.5,
          "p999" : 169,
          "count" : 4,
          "p95" : 169,
          "max" : 169,
          "mean_rate" : 0,
          "duration_units" : "milliseconds",
          "p98" : 169,
          "m1_rate" : 0,
          "rate_units" : "calls/millisecond",
          "m15_rate" : 0,
          "stddev" : 62.9417720330995,
          "m5_rate" : 0,
          "p99" : 169,
          "min" : 42
        },
        ...
    },
    "histograms" : {
        "callablequeue.threads.active.histogram" : {
          "p999" : 1,
          "mean" : 0.0625,
          "min" : 0,
          "p75" : 0,
          "p95" : 1,
          "count" : 48,
          "p98" : 1,
          "stddev" : 0.24462302739504083,
          "max" : 1,
          "p99" : 1,
          "p50" : 0
        },
        ...
    },
    "counters" : {
        "commands.job.info.executions" : {
          "count" : 9
        },
        "jpa.CoordJobsGetPendingJPAExecutor" : {
          "count" : 1
        },
        "jpa.GET_WORKFLOW" : {
          "count" : 10
        },
        ...
    }
}
```

#### Version

_Identical to the corresponding Oozie v0 WS API_

A HTTP GET request returns the Oozie build version.

**Request:**


```
GET /oozie/v1/admin/build-version
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{buildVersion: "3.0.0-SNAPSHOT" }
```

#### Available Time Zones

A HTTP GET request returns the available time zones.

**Request:**


```
GET /oozie/v1/admin/available-timezones
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  "available-timezones":[
    {
      "timezoneDisplayName":"SST (Pacific\/Midway)",
      "timezoneId":"Pacific\/Midway"
    },
    {
      "timezoneDisplayName":"NUT (Pacific\/Niue)",
      "timezoneId":"Pacific\/Niue"
    },
    {
      "timezoneDisplayName":"SST (Pacific\/Pago_Pago)",
      "timezoneId":"Pacific\/Pago_Pago"
    },
    {
      "timezoneDisplayName":"SST (Pacific\/Samoa)",
      "timezoneId":"Pacific\/Samoa"
    },
    {
      "timezoneDisplayName":"SST (US\/Samoa)",
      "timezoneId":"US\/Samoa"
    },
    {
      "timezoneDisplayName":"HAST (America\/Adak)",
      "timezoneId":"America\/Adak"
    },
    {
      "timezoneDisplayName":"HAST (America\/Atka)",
      "timezoneId":"America\/Atka"
    },
    {
      "timezoneDisplayName":"HST (HST)",
      "timezoneId":"HST"
    },
    ...
  ]
}
```

#### Queue Dump

A HTTP GET request returns the queue dump of the Oozie system.  This is an administrator debugging feature.

**Request:**


```

GET /oozie/v1/admin/queue-dump
```

#### Available Oozie Servers

A HTTP GET request returns the list of available Oozie Servers.  This is useful when Oozie is configured
for [High Availability](AG_Install.html#HA); if not, it will simply return the one Oozie Server.

**Request:**


```
GET /oozie/v2/admin/available-oozie-servers
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
    "hostA": "http://hostA:11000/oozie",
    "hostB": "http://hostB:11000/oozie",
    "hostC": "http://hostC:11000/oozie",
}
```

#### List available sharelib
A HTTP GET request to get list of available sharelib.
If the name of the sharelib is passed as an argument (regex supported) then all corresponding files are also listed.

**Request:**


```
GET /oozie/v2/admin/list_sharelib
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
{
    "sharelib":
        [
            "oozie",
            "hive",
            "distcp",
            "hcatalog",
            "sqoop",
            "mapreduce-streaming",
            "pig"
        ]
}
```

**Request:**


```
GET /oozie/v2/admin/list_sharelib?lib=pig*
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8

{
    "sharelib":
            [
                {
                    "pig":
                        {
                            "sharelibFiles":
                                [
                                    hdfs://localhost:9000/user/purushah/share/lib/lib_20131114095729/pig/pig.jar
                                    hdfs://localhost:9000/user/purushah/share/lib/lib_20131114095729/pig/piggybank.jar
                                ]
                        }
                }
            ]
}
```


#### Update system sharelib
This webservice call makes the oozie server(s) to pick up the latest version of sharelib present
under oozie.service.WorkflowAppService.system.libpath directory based on the sharelib directory timestamp or reloads
the sharelib metafile if one is configured. The main purpose is to update the sharelib on the oozie server without restarting.


**Request:**


```
GET /oozie/v2/admin/update_sharelib
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
[
    {
       "sharelibUpdate":{
          "host":"server1",
          "status":"Server not found"
       }
    },
    {
       "sharelibUpdate":{
          "host":"server2",
          "status":"Successful",
          "sharelibDirOld":"hdfs://localhost:51951/user/purushah/share/lib/lib_20140107181218",
          "sharelibDirNew":"hdfs://localhost:51951/user/purushah/share/lib/lib_20140107181218"
       }
    },
    {
       "sharelibUpdate":{
          "host":"server3",
          "status":"Successful",
          "sharelibDirOld":"hdfs://localhost:51951/user/purushah/share/lib/lib_20140107181218",
          "sharelibDirNew":"hdfs://localhost:51951/user/purushah/share/lib/lib_20140107181218"
       }
    }
]
```

#### Purge Command

Oozie admin purge command cleans up the Oozie Workflow/Coordinator/Bundle records based on the parameters.
The unit for parameters is day.

Purge command will delete the workflow records (wf=30) older than 30 days, coordinator records (coord=7) older than 7 days and
bundle records (bundle=7) older than 7 days. The limit (limit=10) defines, number of records to be fetch at a time. Turn
(oldCoordAction`true/false) `on/off= coordinator action record purging for long running coordinators. If any of the parameter is
not provided, then it will be taken from the `oozie-default/oozie-site` configuration.

**Request:**


```

GET /oozie/v2/admin/purge?wf=30&coord=7&bundle=7&limit=10&oldCoordAction=true

```

**Response:**


```

{
  "purge": "Purge command executed successfully"
}

```

### Job and Jobs End-Points

_Modified in Oozie v1 WS API_

These endpoints are for submitting, managing and retrieving information of workflow, coordinator, and bundle jobs.

#### Job Submission

#### Standard Job Submission

An HTTP POST request with an XML configuration as payload creates a job.

The type of job is determined by the presence of one of the following 3 properties:

   * `oozie.wf.application.path` : path to a workflow application directory, creates a workflow job
   * `oozie.coord.application.path` : path to a coordinator application file, creates a coordinator job
   * `oozie.bundle.application.path` : path to a bundle application file, creates a bundle job

Or, if none of those are present, the jobtype parameter determines the type of job to run. It can either be mapreduce or pig.

**Request:**


```
POST /oozie/v1/jobs
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>user.name</name>
        <value>bansalm</value>
    </property>
    <property>
        <name>oozie.wf.application.path</name>
        <value>hdfs://foo:8020/user/bansalm/myapp/</value>
    </property>
    ...
</configuration>
```

**Response:**


```
HTTP/1.1 201 CREATED
Content-Type: application/json;charset=UTF-8
.
{
  id: "job-3"
}
```

A created job will be in `PREP` status. If the query string parameter 'action=start' is provided in
the POST URL, the job will be started immediately and its status will be `RUNNING`.

Coordinator jobs with start time in the future they will not create any action until the start time
happens.

A coordinator job will remain in `PREP` status until it's triggered, in which case it will change to `RUNNING` status.
The 'action=start' parameter is not valid for coordinator jobs.

#### Proxy MapReduce Job Submission

You can submit a Workflow that contains a single MapReduce action without writing a workflow.xml. Any required Jars or other files
must already exist in HDFS.

The following properties are required; any additional parameters needed by the MapReduce job can also be specified here:

   * `fs.default.name`: The NameNode
   * `mapred.job.tracker`: The JobTracker
   * `mapred.mapper.class`: The map-task classname
   * `mapred.reducer.class`: The reducer-task classname
   * `mapred.input.dir`: The map-task input directory
   * `mapred.output.dir`: The reduce-task output directory
   * `user.name`: The username of the user submitting the job
   * `oozie.libpath`: A directory in HDFS that contains necessary Jars for your job
   * `oozie.proxysubmission`: Must be set to `true`

**Request:**


```
POST /oozie/v1/jobs?jobtype=mapreduce
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:8020</value>
    </property>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:8021</value>
    </property>
    <property>
        <name>mapred.mapper.class</name>
        <value>org.apache.oozie.example.SampleMapper</value>
    </property>
    <property>
        <name>mapred.reducer.class</name>
        <value>org.apache.oozie.example.SampleReducer</value>
    </property>
    <property>
        <name>mapred.input.dir</name>
        <value>hdfs://localhost:8020/user/rkanter/examples/input-data/text</value>
    </property>
    <property>
        <name>mapred.output.dir</name>
        <value>hdfs://localhost:8020/user/rkanter/examples/output-data/map-reduce</value>
    </property>
    <property>
        <name>user.name</name>
        <value>rkanter</value>
    </property>
    <property>
        <name>oozie.libpath</name>
        <value>hdfs://localhost:8020/user/rkanter/examples/apps/map-reduce/lib</value>
    </property>
    <property>
        <name>oozie.proxysubmission</name>
        <value>true</value>
    </property>
</configuration>
```

**Response:**


```
HTTP/1.1 201 CREATED
Content-Type: application/json;charset=UTF-8
.
{
  id: "job-3"
}
```

#### Proxy Pig Job Submission

You can submit a Workflow that contains a single Pig action without writing a workflow.xml.  Any required Jars or other files must
already exist in HDFS.

The following properties are required:

   * `fs.default.name`: The NameNode
   * `mapred.job.tracker`: The JobTracker
   * `user.name`: The username of the user submitting the job
   * `oozie.pig.script`: Contains the pig script you want to run (the actual script, not a file path)
   * `oozie.libpath`: A directory in HDFS that contains necessary Jars for your job
   * `oozie.proxysubmission`: Must be set to `true`

The following properties are optional:

   * `oozie.pig.script.params.size`: The number of parameters you'll be passing to Pig
   required
  *  `oozie.pig.script.params.n`: A parameter (variable definition for the script) in 'key=value' format, the 'n' should be an integer starting with 0 to indicate the parameter number
   * `oozie.pig.options.size`: The number of options you'll be passing to Pig
   * `oozie.pig.options.n`: An argument to pass to Pig, the 'n' should be an integer starting with 0 to indicate the option number

The `oozie.pig.options.n` parameters are sent directly to Pig without any modification unless they start with `-D`, in which case
they are put into the `<configuration>` element of the action.

In addition to passing parameters to Pig with `oozie.pig.script.params.n`, you can also create a properties file on HDFS and
reference it with the `-param_file` option in `oozie.pig.script.options.n`; both are shown in the following example.


```
$ hadoop fs -cat /user/rkanter/pig_params.properties
INPUT=/user/rkanter/examples/input-data/text
```

**Request:**


```
POST /oozie/v1/jobs?jobtype=pig
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:8020</value>
    </property>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:8021</value>
    </property>
    <property>
        <name>user.name</name>
        <value>rkanter</value>
    </property>
    <property>
        <name>oozie.pig.script</name>
        <value>
            A = load '$INPUT' using PigStorage(':');
            B = foreach A generate $0 as id;
            store B into '$OUTPUT' USING PigStorage();
        </value>
    </property>
    <property>
        <name>oozie.pig.script.params.size</name>
        <value>1</value>
    </property>
    <property>
        <name>oozie.pig.script.params.0</name>
        <value>OUTPUT=/user/rkanter/examples/output-data/pig</value>
    </property>
    <property>
        <name>oozie.pig.options.size</name>
        <value>2</value>
    </property>
    <property>
        <name>oozie.pig.options.0</name>
        <value>-param_file</value>
    </property>
    <property>
        <name>oozie.pig.options.1</name>
        <value>hdfs://localhost:8020/user/rkanter/pig_params.properties</value>
    </property>
    <property>
        <name>oozie.libpath</name>
        <value>hdfs://localhost:8020/user/rkanter/share/lib/pig</value>
    </property>
    <property>
        <name>oozie.proxysubmission</name>
        <value>true</value>
    </property>
</configuration>
```

**Response:**


```
HTTP/1.1 201 CREATED
Content-Type: application/json;charset=UTF-8
.
{
  id: "job-3"
}
```

#### Proxy Hive Job Submission

You can submit a Workflow that contains a single Hive action without writing a workflow.xml.  Any required Jars or other files must
already exist in HDFS.

The following properties are required:

   * `fs.default.name`: The NameNode
   * `mapred.job.tracker`: The JobTracker
   * `user.name`: The username of the user submitting the job
   * `oozie.hive.script`: Contains the hive script you want to run (the actual script, not a file path)
   * `oozie.libpath`: A directory in HDFS that contains necessary Jars for your job
   * `oozie.proxysubmission`: Must be set to `true`

The following properties are optional:

   * `oozie.hive.script.params.size`: The number of parameters you'll be passing to Hive
   * `oozie.hive.script.params.n`: A parameter (variable definition for the script) in 'key=value' format, the 'n' should be an integer starting with 0 to indicate the parameter number
   * `oozie.hive.options.size`: The number of options you'll be passing to Hive
   * `oozie.hive.options.n`: An argument to pass to Hive, the 'n' should be an integer starting with 0 to indicate the option number

The `oozie.hive.options.n` parameters are sent directly to Hive without any modification unless they start with `-D`, in which case
they are put into the `<configuration>` element of the action.

**Request:**


```
POST /oozie/v1/jobs?jobtype=hive
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:8020</value>
    </property>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:8021</value>
    </property>
    <property>
        <name>user.name</name>
        <value>rkanter</value>
    </property>
    <property>
        <name>oozie.hive.script</name>
        <value>
            CREATE EXTERNAL TABLE test (a INT) STORED AS TEXTFILE LOCATION '${INPUT}';
            INSERT OVERWRITE DIRECTORY '${OUTPUT}' SELECT * FROM test;
        </value>
    </property>
    <property>
        <name>oozie.hive.script.params.size</name>
        <value>2</value>
    </property>
    <property>
        <name>oozie.hive.script.params.0</name>
        <value>OUTPUT=/user/rkanter/examples/output-data/hive</value>
    </property>
    <property>
        <name>oozie.hive.script.params.1</name>
        <value>INPUT=/user/rkanter/examples/input-data/table</value>
    </property>
    <property>
        <name>oozie.libpath</name>
        <value>hdfs://localhost:8020/user/rkanter/share/lib/hive</value>
    </property>
    <property>
        <name>oozie.proxysubmission</name>
        <value>true</value>
    </property>
</configuration>
```

**Response:**


```
HTTP/1.1 201 CREATED
Content-Type: application/json;charset=UTF-8
.
{
  id: "job-3"
}
```

#### Proxy Sqoop Job Submission

You can submit a Workflow that contains a single Sqoop command without writing a workflow.xml. Any required Jars or other
 files must already exist in HDFS.

The following properties are required:

   * `fs.default.name`: The NameNode
   * `mapred.job.tracker`: The JobTracker
   * `user.name`: The username of the user submitting the job
   * `oozie.sqoop.command`: The sqoop command you want to run where each argument occupies one line or separated by "\n"
   * `oozie.libpath`: A directory in HDFS that contains necessary Jars for your job
   * `oozie.proxysubmission`: Must be set to `true`

The following properties are optional:

   * `oozie.sqoop.options.size`: The number of options you'll be passing to Sqoop Hadoop job
   * `oozie.sqoop.options.n`: An argument to pass to Sqoop hadoop job conf, the 'n' should be an integer starting with 0 to indicate the option number

**Request:**


```
POST /oozie/v1/jobs?jobtype=sqoop
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:8020</value>
    </property>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:8021</value>
    </property>
    <property>
        <name>user.name</name>
        <value>bzhang</value>
    </property>
    <property>
        <name>oozie.sqoop.command</name>
        <value>
            import
            --connect
            jdbc:mysql://localhost:3306/oozie
            --username
            oozie
            --password
            oozie
            --table
            WF_JOBS
            --target-dir
            /user/${wf:user()}/${examplesRoot}/output-data/sqoop
        </value>
    </property>
        <name>oozie.libpath</name>
        <value>hdfs://localhost:8020/user/bzhang/share/lib/sqoop</value>
    </property>
    <property>
        <name>oozie.proxysubmission</name>
        <value>true</value>
    </property>
</configuration>
```

**Response:**


```
HTTP/1.1 201 CREATED
Content-Type: application/json;charset=UTF-8
.
{
  id: "job-3"
}
```

#### Managing a Job

A HTTP PUT request starts, suspends, resumes, kills, update or dryruns a job.

**Request:**


```
PUT /oozie/v1/job/job-3?action=start
```

**Response:**


```
HTTP/1.1 200 OK
```

Valid values for the 'action' parameter are 'start', 'suspend', 'resume', 'kill', 'dryrun', 'rerun', and 'change'.

Rerunning and changing a job require additional parameters, and are described below:

#####  Re-Running a Workflow Job

A workflow job in `SUCCEEDED`, `KILLED` or `FAILED` status can be partially rerun specifying a list
of workflow nodes to skip during the rerun. All the nodes in the skip list must have complete its
execution.

The rerun job will have the same job ID.

A rerun request is done with a HTTP PUT request with a `rerun` action.

**Request:**


```
PUT /oozie/v1/job/job-3?action=rerun
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>user.name</name>
        <value>tucu</value>
    </property>
    <property>
        <name>oozie.wf.application.path</name>
        <value>hdfs://foo:8020/user/tucu/myapp/</value>
    </property>
    <property>
        <name>oozie.wf.rerun.skip.nodes</name>
        <value>firstAction,secondAction</value>
    </property>
    ...
</configuration>
```

**Response:**


```
HTTP/1.1 200 OK
```

##### Re-Running a coordinator job

A coordinator job in `RUNNING` `SUCCEEDED`, `KILLED` or `FAILED` status can be partially rerun by specifying the coordinator actions
to re-execute.

A rerun request is done with an HTTP PUT request with a `coord-rerun` `action`.

The `type` of the rerun can be `date` or `action`.

The `scope` of the rerun depends on the type:
* `date`: a comma-separated list of date ranges. Each date range element is specified with dates separated by `::`
* `action`: a comma-separated list of action ranges. Each action range is specified with two action numbers separated by `-`

The `refresh` parameter can be `true` or `false` to specify if the user wants to refresh an action's input and output events.

The `nocleanup` parameter can be `true` or `false` to specify is the user wants to cleanup output events for the rerun actions.

**Request:**


```
PUT /oozie/v1/job/job-3?action=coord-rerun&type=action&scope=1-2&refresh=false&nocleanup=false
.
```

or


```
PUT /oozie/v1/job/job-3?action=coord-rerun&type=date2009-02-01T00:10Z::2009-03-01T00:10Z&scope=&refresh=false&nocleanup=false
.
```

**Response:**


```
HTTP/1.1 200 OK
```

##### Re-Running a bundle job

A coordinator job in `RUNNING` `SUCCEEDED`, `KILLED` or `FAILED` status can be partially rerun by specifying the coordinators to
re-execute.

A rerun request is done with an HTTP PUT request with a `bundle-rerun` `action`.

A comma separated list of coordinator job names (not IDs) can be specified in the `coord-scope` parameter.

The `date-scope` parameter is a comma-separated list of date ranges. Each date range element is specified with dates separated
by `::`. If empty or not included, Oozie will figure this out for you

The `refresh` parameter can be `true` or `false` to specify if the user wants to refresh the coordinator's input and output events.

The `nocleanup` parameter can be `true` or `false` to specify is the user wants to cleanup output events for the rerun coordinators.

**Request:**


```
PUT /oozie/v1/job/job-3?action=bundle-rerun&coord-scope=coord-1&refresh=false&nocleanup=false
.
```

**Response:**


```
HTTP/1.1 200 OK
```


##### Changing endtime/concurrency/pausetime of a Coordinator Job

A coordinator job not in `KILLED` status can have it's endtime, concurrency, or pausetime changed.

A change request is done with an HTTP PUT request with a `change` `action`.

The `value` parameter can contain any of the following:
* endtime: the end time of the coordinator job.
* concurrency: the concurrency of the coordinator job.
* pausetime: the pause time of the coordinator job.

Multiple arguments can be passed to the `value` parameter by separating them with a ';' character.

If an already-succeeded job changes its end time, its status will become running.

**Request:**


```
PUT /oozie/v1/job/job-3?action=change&value=endtime=2011-12-01T05:00Z
.
```

or


```
PUT /oozie/v1/job/job-3?action=change&value=concurrency=100
.
```

or


```
PUT /oozie/v1/job/job-3?action=change&value=pausetime=2011-12-01T05:00Z
.
```

or


```
PUT /oozie/v1/job/job-3?action=change&value=endtime=2011-12-01T05:00Z;concurrency=100;pausetime=2011-12-01T05:00Z
.
```

**Response:**


```
HTTP/1.1 200 OK
```

##### Updating coordinator definition and properties
Existing coordinator definition and properties will be replaced by new definition and properties. Refer [Updating coordinator definition and properties](DG_CommandLineTool.html#Updating_coordinator_definition_and_properties)


```
PUT oozie/v2/job/0000000-140414102048137-oozie-puru-C?action=update
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
{"update":
     {"diff":"**********Job definition changes**********\n******************************************\n**********Job conf changes****************\n@@ -8,16 +8,12 @@\n
          <value>hdfs:\/\/localhost:9000\/user\/purushah\/examples\/apps\/aggregator\/coordinator.xml<\/value>\r\n   <\/property>\r\n   <property>\r\n
          -    <name>user.name<\/name>\r\n
          -    <value>purushah<\/value>\r\n
          -    <\/property>\r\n
          -  <property>\r\n     <name>start<\/name>\r\n
               <value>2010-01-01T01:00Z<\/value>\r\n   <\/property>\r\n   <property>\r\n
          -    <name>newproperty<\/name>\r\n
          -    <value>new<\/value>\r\n
          +    <name>user.name<\/name>\r\n
          +    <value>purushah<\/value>\r\n   <\/property>\r\n   <property>\r\n
               <name>queueName<\/name>\r\n******************************************\n"
      }
}
```


#### Job Information

A HTTP GET request retrieves the job information.

**Request:**


```
GET /oozie/v1/job/job-3?show=info&timezone=GMT
```

**Response for a workflow job:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  id: "0-200905191240-oozie-W",
  appName: "indexer-workflow",
  appPath: "hdfs://user/bansalm/indexer.wf",
  externalId: "0-200905191230-oozie-pepe",
  user: "bansalm",
  status: "RUNNING",
  conf: "<configuration> ... </configuration>",
  createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
  startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
  endTime: null,
  run: 0,
  actions: [
    {
      id: "0-200905191240-oozie-W@indexer",
      name: "indexer",
      type: "map-reduce",
      conf: "<configuration> ...</configuration>",
      startTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      endTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      status: "OK",
      externalId: "job-123-200903101010",
      externalStatus: "SUCCEEDED",
      trackerUri: "foo:8021",
      consoleUrl: "http://foo:50040/jobdetailshistory.jsp?jobId=...",
      transition: "reporter",
      data: null,
      errorCode: null,
      errorMessage: null,
      retries: 0
    },
    ...
  ]
}
```

**Response for a coordinator job:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  id: "0-200905191240-oozie-C",
  appName: "indexer-Coord",
  appPath: "hdfs://user/bansalm/myapp/logprocessor-coord.xml",
  externalId: "0-200905191230-oozie-pepe",
  user: "bansalm",
  status: "RUNNING",
  conf: "<configuration> ... </configuration>",
  createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
  startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
  endTime: "Fri, 31 Dec 2009 00:00:00 GMT",
  frequency: "${days(1)}"
  actions: [
    {
      id: "0000010-130426111815091-oozie-bansalm-C@1",
      createdTime: "Fri, 26 Apr 2013 20:57:07 GMT",
      externalId: "",
      missingDependencies: "",
      runConf: null,
      createdConf: null,
      consoleUrl: null,
      nominalTime: "Fri, 01 Jan 2010 01:00:00 GMT",
  ...
}
```

**Response for a bundle job:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  jobType: "bundle",
  id: "0-200905191240-oozie-B",
  appName: "new-bundle",
  appPath: "hdfs://user/bansalm/myapp/logprocessor-bundle.xml",
  externalId: "0-200905191230-oozie-pepe",
  user: "bansalm",
  status: "RUNNING",
  conf: "<configuration> ... </configuration>",
  createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
  startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
  endTime: "Fri, 31 Dec 2009 00:00:00 GMT"
  bundleCoordJobs: [
    {
      status: "RUNNING",
      concurrency: 1,
      conf: "<configuration> ... </configuration>",
      executionPolicy: "FIFO",
      toString: "Coordinator application id[0000010-130426111815091-oozie-bansalm-C] status[RUNNING]",
      coordJobName: "coord-1",
      endTime: "Fri, 01 Jan 2010 03:00:00 GMT",
      ...
    }
  ...
}
```

**Getting all the Workflows corresponding to a Coordinator Action:**

A coordinator action kicks off different workflows for its original run and all subsequent reruns.
Getting a list of those workflow ids is a useful tool to keep track of your actions' runs and
to go debug the workflow job logs if required. Along with ids, it also lists their statuses,
and start and end times for quick reference.

Both v1 and v2 API are supported. v0 is not supported.


```
GET /oozie/v2/job/0000001-111219170928042-oozie-joe-C@1?show=allruns
```

**Response**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{"workflows":[
    {
        "startTime":"Mon, 24 Mar 2014 23:40:53 GMT",
        "id":"0000001-140324163709596-oozie-chit-W",
        "status":"SUCCEEDED",
        "endTime":"Mon, 24 Mar 2014 23:40:54 GMT"
    },
    {
        "startTime":"Mon, 24 Mar 2014 23:44:01 GMT",
        "id":"0000000-140324164318985-oozie-chit-W",
        "status":"SUCCEEDED",
        "endTime":"Mon, 24 Mar 2014 23:44:01 GMT"
    },
    {
        "startTime":"Mon, 24 Mar 2014 23:44:24 GMT",
        "id":"0000001-140324164318985-oozie-chit-W",
        "status":"SUCCEEDED",
        "endTime":"Mon, 24 Mar 2014 23:44:24 GMT"
    }
]}
```

An alternate API is also available for the same output. With this API, one can pass the coordinator **JOB** Id
followed by query params - `type=action` and `scope=<action-number>`. One single action number can be passed at a time.


```
GET /oozie/v2/job/0000001-111219170928042-oozie-joe-C?show=allruns&type=action&scope=1
```

**Retrieve a subset of actions**

Query parameters, `offset` and `length` can be specified with a workflow job to retrieve specific actions. Default is offset=0, len=1000

```
GET /oozie/v1/job/0000002-130507145349661-oozie-joe-W?show=info&offset=5&len=10
```
Query parameters, `offset`, `length`, `filter` can be specified with a coordinator job to retrieve specific actions.
Query parameter, `order` with value "desc" can be used to retrieve the latest coordinator actions materialized instead of actions from @1.
Query parameters `filter` can be used to retrieve coordinator actions matching specific status.
Default is offset=0, len=0 for v2/job (i.e., does not return any coordinator actions) and offset=0, len=1000 with v1/job and v0/job.
So if you need actions to be returned with v2 API, specifying `len` parameter is necessary.
Default `order` is "asc".

```
GET /oozie/v1/job/0000001-111219170928042-oozie-joe-C?show=info&offset=5&len=10&filter=status%3DKILLED&order=desc
```
Note that the filter is URL encoded, its decoded value is `status=KILLED`.

```
GET /oozie/v1/job/0000001-111219170928042-oozie-joe-C?show=info&filter=status%21%3DSUCCEEDED&order=desc
```
This retrieves coordinator actions except for SUCCEEDED status, which is useful for debugging.

**Retrieve information of the retry attempts of the workflow action:**


```
GET oozie/v2/job/0000000-161212175234862-oozie-puru-W@pig-node?show=retries
```

**Response**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
     "retries":
     [
         {
             "startTime": "Tue, 13 Dec 2016 01:54:13 GMT",
             "consoleUrl": "http://localhost:50030/jobdetails.jsp?jobid=job_201612051339_2648",
             "endTime": "Tue, 13 Dec 2016 01:54:20 GMT",
             "attempt": "1"
         },
         {
             "startTime": "Tue, 13 Dec 2016 01:55:20 GMT",
             "consoleUrl": "http://localhost:50030/jobdetails.jsp?jobid=job_201612051339_2649",
             "endTime": "Tue, 13 Dec 2016 01:55:24 GMT",
             "attempt": "2"
         }
    ]
}
```

#### Job Application Definition

A HTTP GET request retrieves the workflow or a coordinator job definition file.

**Request:**


```
GET /oozie/v1/job/job-3?show=definition
```

**Response for a workflow job:**


```
HTTP/1.1 200 OK
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<workflow-app name='xyz-app' xmlns="uri:oozie:workflow:0.1">
    <start to='firstaction' />
    ...
    <end name='end' />
</workflow-app>
```

**Response for a coordinator job:**


```
HTTP/1.1 200 OK
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<coordinator-app name='abc-app' xmlns="uri:oozie:coordinator:0.1" frequency="${days(1)}
                 start="2009-01-01T00:00Z" end="2009-12-31T00:00Z" timezone="America/Los_Angeles">
    <datasets>
    ...
    </datasets>
    ...
</coordinator-app>
```

**Response for a bundle job:**


```
HTTP/1.1 200 OK
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8"?>
<bundle-app name='abc-app' xmlns="uri:oozie:coordinator:0.1"
                 start="2009-01-01T00:00Z" end="2009-12-31T00:00Z"">
    <datasets>
    ...
    </datasets>
    ...
</bundle-app>
```

#### Job Log

An HTTP GET request retrieves the job log.

**Request:**


```
GET /oozie/v1/job/job-3?show=log
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: text/plain;charset=UTF-8
.
...
23:21:31,272 TRACE oozieapp:526 - USER[bansalm] GROUP[other] TOKEN[-] APP[test-wf] JOB[0-20090518232130-oozie-tucu] ACTION[mr-1] Start
23:21:31,305 TRACE oozieapp:526 - USER[bansalm] GROUP[other] TOKEN[-] APP[test-wf] JOB[0-20090518232130-oozie-tucu] ACTION[mr-1] End
...
```

#### Job Error Log

An HTTP GET request retrieves the job error log.

**Request:**


```
GET /oozie/v2/job/0000000-150121110331712-oozie-puru-B?show=errorlog
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: text/plain;charset=UTF-8
2015-01-21 11:33:29,090  WARN CoordSubmitXCommand:523 - SERVER[-] USER[-] GROUP[-] TOKEN[-] APP[-] JOB[0000000-150121110331712-oozie-puru-B] ACTION[] SAXException :
org.xml.sax.SAXParseException; lineNumber: 20; columnNumber: 22; cvc-complex-type.2.4.a: Invalid content was found starting with element 'concurrency'. One of '{"uri:oozie:coordinator:0.2":controls, "uri:oozie:coordinator:0.2":datasets, "uri:oozie:coordinator:0.2":input-events, "uri:oozie:coordinator:0.2":output-events, "uri:oozie:coordinator:0.2":action}' is expected.
        at org.apache.xerces.util.ErrorHandlerWrapper.createSAXParseException(Unknown Source)
        at org.apache.xerces.util.ErrorHandlerWrapper.error(Unknown Source)
...
```


#### Job Audit Log

An HTTP GET request retrieves the job audit log.

**Request:**


```
GET /oozie/v2/job/0000000-150322000230582-oozie-puru-C?show=auditlog
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: text/plain;charset=UTF-8
2015-03-22 00:04:35,494  INFO oozieaudit:520 - IP [-], USER [purushah], GROUP [null], APP [-], JOBID [0000000-150322000230582-oozie-puru-C], OPERATION [start], PARAMETER [null], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [null], ERRORMESSAGE [null]
2015-03-22 00:05:13,823  INFO oozieaudit:520 - IP [-], USER [purushah], GROUP [null], APP [-], JOBID [0000000-150322000230582-oozie-puru-C], OPERATION [suspend], PARAMETER [0000000-150322000230582-oozie-puru-C], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [null], ERRORMESSAGE [null]
2015-03-22 00:06:59,561  INFO oozieaudit:520 - IP [-], USER [purushah], GROUP [null], APP [-], JOBID [0000000-150322000230582-oozie-puru-C], OPERATION [suspend], PARAMETER [0000000-150322000230582-oozie-puru-C], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [null], ERRORMESSAGE [null]
2015-03-22 23:22:20,012  INFO oozieaudit:520 - IP [-], USER [purushah], GROUP [null], APP [-], JOBID [0000000-150322000230582-oozie-puru-C], OPERATION [suspend], PARAMETER [0000000-150322000230582-oozie-puru-C], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [null], ERRORMESSAGE [null]
2015-03-22 23:28:48,218  INFO oozieaudit:520 - IP [-], USER [purushah], GROUP [null], APP [-], JOBID [0000000-150322000230582-oozie-puru-C], OPERATION [resume], PARAMETER [0000000-150322000230582-oozie-puru-C], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [null], ERRORMESSAGE [null]
```


#### Filtering the server logs with logfilter options
User can provide multiple option to filter logs using -logfilter opt1=val1;opt2=val1;opt3=val1. This can be used to fetch only just logs of interest faster as fetching Oozie server logs is slow due to the overhead of pattern matching.


```
GET /oozie/v1/job/0000003-140319184715726-oozie-puru-C?show=log&logfilter=limit=3;loglevel=WARN
```


Refer to the [Filtering the server logs with logfilter options](DG_CommandLineTool.html#Filtering_the_server_logs_with_logfilter_options) for more details.


#### Job graph

An `HTTP GET` request returns the image of the workflow DAG (rendered as a PNG or SVG image, or as a DOT string).

   * The nodes that are being executed are painted yellow
   * The nodes that have successfully executed are painted green
   * The nodes that have failed execution are painted red
   * The nodes that are yet to be executed are pained gray
   * An arc painted green marks the successful path taken so far
   * An arc painted red marks the failure of the node and highlights the _error_ action
   * An arc painted gray marks a path not taken yet

**PNG request:**

```
GET /oozie/v1/job/job-3?show=graph[&show-kill=true][&format=png]
```

**PNG response:**

```
HTTP/1.1 200 OK
Content-Type: image/png
Content-Length: {image_size_in_bytes}

{image_bits}
```

**SVG request:**

```
GET /oozie/v1/job/job-3?show=graph[&show-kill=true]&format=svg
```

**SVG response:**

```
HTTP/1.1 200 OK
Content-Type: image/svg+xml
Content-Length: {image_size_in_bytes}

{image_bits}
```

**DOT request:**

```
GET /oozie/v1/job/job-3?show=graph[&show-kill=true]&format=dot
```

**DOT response:**

```
HTTP/1.1 200 OK
Content-Type: text/plain
Content-Length: {dot_size_in_bytes}

{dot_bytes}
```

The optional `show-kill` parameter shows `kill` node in the graph. Valid values for this parameter are `1`, `yes`, and `true`.
This parameter has no effect when workflow fails and the failure node leads to the `kill` node; in  that case `kill` node is shown
always.

The optional `format` parameter describes whether the response has to be rendered as a PNG image, or an SVG image, or a DOT string.
When omitted, `format` is considered as `png` for backwards compatibility. Oozie Web UI uses the `svg` `format`.

The node labels are the node names provided in the workflow XML.

This API returns `HTTP 400` when run on a resource other than a workflow, viz. bundle and coordinator.

Note that when running on JDK8 the supported minimum minor version for this feature is `1.8.0_u40`.

#### Job Status

An `HTTP GET` request that returns the current status (e.g. `SUCCEEDED`, `KILLED`, etc) of a given job.  If you are only interested
in the status, and don't want the rest of the information that the `info` query provides, it is recommended to use this call
as it is more efficient.

**Request**

```
GET /oozie/v2/job/0000000-140908152307821-oozie-rkan-C?show=status
```

**Response**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  "status" : "SUCCEEDED"
}
```

It accepts any valid Workflow Job ID, Coordinator Job ID, Coordinator Action ID, or Bundle Job ID.

#### Changing job SLA definition and alerting
An `HTTP PUT` request to change job SLA alert status/SLA definition.

   * All sla commands takes actions-list or date parameter.
   * `date`: a comma-separated list of date ranges. Each date range element is specified with dates separated by `::`
   * `action-list`: a comma-separated list of action ranges. Each action range is specified with two action numbers separated by `-`
   * For bundle jobs additional `coordinators` (coord_name/id) parameter can be passed.
   * Sla change command need extra parameter `value` to specify new sla definition.
   * Changing SLA definition

   SLA definition of should-start, should-end, nominal-time and max-duration can be changed.


```
PUT /oozie/v2/job/0000003-140319184715726-oozie-puru-C?action=sla-change&value=<key>=<value>;...;<key>=<value>
```

   * Disabling SLA alert


```
PUT /oozie/v2/job/0000003-140319184715726-oozie-puru-C?action=sla-disable&action-list=3-4
```
Will disable SLA alert for actions 3 and 4.


```
PUT /oozie/v1/job/0000003-140319184715726-oozie-puru-C?action=sla-disable&date=2009-02-01T00:10Z::2009-03-01T00:10Z
```
Will disable SLA alert for actions whose nominal time is in-between 2009-02-01T00:10Z 2009-03-01T00:10Z (inclusive).



```
PUT /oozie/v1/job/0000004-140319184715726-oozie-puru-B?action=sla-disable&date=2009-02-01T00:10Z::2009-03-01T00:10Z&coordinators=abc
```
For bundle jobs additional coordinators (list of comma separated coord_name/id) parameter can be passed.

   * Enabling SLA alert


```
PUT /oozie/v2/job/0000003-140319184715726-oozie-puru-C?action=sla-enable&action-list=1,14,17-20
```
Will enable SLA alert for actions 1,14,17,18,19,20.

### Getting missing dependencies of coordinator action(s)


```
GET oozie/v2/job/0000000-170104115137443-oozie-puru-C?show=missing-dependencies&action-list=1,20
```

**Response**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8

{
"missingDependencies":
[{
        "blockedOn": "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/00/00/_SUCCESS",
        "dataSets":
        [
            {
                "missingDependencies":
                [
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/00/00/_SUCCESS"
                ],
                "dataSet": "input-2"
            }
        ],
        "id": 1
    },
    {
        "blockedOn": "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/20/00/_SUCCESS",
        "dataSets":
        [
            {
                "missingDependencies":
                [
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/20/00/_SUCCESS",
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/19/40/_SUCCESS",
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/19/20/_SUCCESS",
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/19/00/_SUCCESS",
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/18/40/_SUCCESS",
                    "hdfs://localhost:9000/user/purushah/examples/input-data/rawLogs/2010/01/01/18/20/_SUCCESS"
                ],
                "dataSet": "input-2"
            }
        ],
        "id": 20
    }]
}
```
#### Jobs Information

A HTTP GET request retrieves workflow and coordinator jobs information.

**Request:**


```
GET /oozie/v1/jobs?filter=user%3Dbansalm&offset=1&len=50&timezone=GMT
```

Note that the filter is URL encoded, its decoded value is `user=bansalm`.

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  offset: 1,
  len: 50,
  total: 1002,
**jobs: [
    {
**    jobType: "workflow"
      id: "0-200905191240-oozie-W",
      appName: "indexer-workflow",
      appPath: "hdfs://user/tucu/indexer-wf",
      user: "bansalm",
      group: "other",
      status: "RUNNING",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: null,
      info: "run=0",
    },
    {
**    jobType: "coordinator"
      id: "0-200905191240-oozie-C",
      appName: "logprocessor-coord",
      appPath: "hdfs://user/bansalm/myapp/logprocessor-coord.xml",
      user: "bansalm",
      group: "other",
      status: "RUNNING",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: "Fri, 31 Dec 2009 00:00:00 GMT",
      info: "nextAction=5",
    },
    {
**    jobType: "bundle"
      id: "0-200905191240-oozie-B",
      appName: "logprocessor-bundle",
      appPath: "hdfs://user/bansalm/myapp/logprocessor-bundle.xml",
      user: "bansalm",
      group: "other",
      status: "RUNNING",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: "Fri, 31 Dec 2009 00:00:00 GMT",
    },
    ...
  ]
}
```

No action information is returned when querying for multiple jobs.


The syntax for the filter is  `[NAME=VALUE][;NAME=VALUE]*`

Valid filter names are:

   * text: any text that might be a part of application name or a part of user name or a complete job ID
   * name: the application name from the workflow/coordinator/bundle definition
   * user: the user who submitted the job
   * group: the group for the job (support for the group filter is discontinued. version: 3.2.0 OOZIE-228).
   * id: the id of the workflow/coordinator/bundle job
   * status: the status of the job
   * startCreatedTime : the start of the window about workflow job's created time
   * endCreatedTime : the end of above window
   * sortby: order the results. Supported values for `sortby` are: `createdTime` and `lastModifiedTime`

The query will do an AND among all the filter names.

The query will do an OR among all the filter values for the same name. Multiple values must be specified as different
name value pairs.

Additionally the `offset` and `len` parameters can be used for pagination. The start parameter is base 1.

Moreover, the `jobtype` parameter could be used to determine what type of job is looking for.
The valid values of job type are: `wf`, `coordinator` or `bundle`.

startCreatedTime and endCreatedTime should be specified either in **ISO8601 (UTC)** format **(yyyy-MM-dd'T'HH:mm'Z')** or
a offset value in days or hours or minutes from the current time. For example, -2d means the (current time - 2 days),
-3h means the (current time - 3 hours), -5m means the (current time - 5 minutes).

#### Bulk modify jobs

A HTTP PUT request can kill, suspend, or resume all jobs that satisfy the url encoded parameters.

**Request:**


```
PUT /oozie/v1/jobs?action=kill&filter=name%3Dcron-coord&offset=1&len=50&jobtype=coordinator
```

This request will kill all the coordinators with name=cron-coord up to 50 of them.

Note that the filter is URL encoded, its decoded value is `name=cron-coord`.

The syntax for the filter is  `[NAME=VALUE][;NAME=VALUE]*`

Valid filter names are:

   * name: the application name from the workflow/coordinator/bundle definition
   * user: the user that submitted the job
   * group: the group for the job
   * status: the status of the job

The query will do an AND among all the filter names.

The query will do an OR among all the filter values for the same name. Multiple values must be specified as different
name value pairs.

Additionally the `offset` and `len` parameters can be used for pagination. The start parameter is base 1.

Moreover, the `jobtype` parameter could be used to determine what type of job is looking for.
The valid values of job type are: `wf`, `coordinator` or `bundle`

**Response:**

```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  offset: 1,
  len: 50,
  total: 2,
**jobs: [
    {
**    jobType: "coordinator"
      id: "0-200905191240-oozie-C",
      appName: "cron-coord",
      appPath: "hdfs://user/bansalm/app/cron-coord.xml",
      user: "bansalm",
      group: "other",
      status: "KILLED",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: "Fri, 31 Dec 2009 00:00:00 GMT",
      info: "nextAction=5",
    },
    {
**    jobType: "coordinator"
      id: "0-200905191240-oozie-C",
      appName: "cron-coord",
      appPath: "hdfs://user/bansalm/myapp/cron-coord.xml",
      user: "bansalm",
      group: "other",
      status: "KILLED",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: "Fri, 31 Dec 2009 00:00:00 GMT",
    },
    ...
  ]
}
```


```
PUT /oozie/v1/jobs?action=suspend&filter=status%3Drunning&offset=1&len=50&jobtype=wf
```

This request will suspend all the workflows with status=running up to 50 of them.
Note that the filter is URL encoded, its decoded value is `status=running`.

**Response:**

```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  offset: 1,
  len: 50,
  total: 50,
**jobs: [
    {
**    jobType: "workflow"
      id: "0-200905191240-oozie-W",
      appName: "indexer-workflow",
      appPath: "hdfs://user/tucu/indexer-wf",
      user: "bansalm",
      group: "other",
      status: "SUSPENDED",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: null,
      info: "run=0",
    },
    {
**    jobType: "workflow"
      id: "0-200905191240-oozie-W",
      appName: "logprocessor-wf",
      appPath: "hdfs://user/bansalm/myapp/workflow.xml",
      user: "bansalm",
      group: "other",
      status: "SUSPENDED",
      createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      endTime: null,
      info: "run=0",
    },
    ...
  ]
}
```

#### Jobs information using Bulk API

A HTTP GET request retrieves a bulk response for all actions, corresponding to a particular bundle, that satisfy user specified criteria.
This is useful for monitoring purposes, where user can find out about the status of downstream jobs with a single bulk request.
The criteria are used for filtering the actions returned. Valid options (_case insensitive_) for these request criteria are:

   * **bundle**: the application name from the bundle definition
   * **coordinators**: the application name(s) from the coordinator definition.
   * **actionStatus**: the status of coordinator action (Valid values are WAITING, READY, SUBMITTED, RUNNING, SUSPENDED, TIMEDOUT, SUCCEEDED, KILLED, FAILED)
   * **startCreatedTime**: the start of the window you want to look at, of the actions' created time
   * **endCreatedTime**: the end of above window
   * **startScheduledTime**: the start of the window you want to look at, of the actions' scheduled i.e. nominal time.
   * **endScheduledTime**: the end of above window

Specifying 'bundle' is REQUIRED. All the rest are OPTIONAL but that might result in thousands of results depending on the size of your job. (pagination comes into play then)

If no 'actionStatus' values provided, by default KILLED,FAILED will be used.
For e.g if the query string is only "bundle=MyBundle", the response will have all actions (across all coordinators) whose status is KILLED or FAILED

The query will do an AND among all the filter names, and OR among each filter name's values.


The syntax for the request criteria is  `[NAME=VALUE][;NAME=VALUE]*`

For 'coordinators' and 'actionStatus', if user wants to check for multiple values, they can be passed in a comma-separated manner.
**Note**: The query will do an OR among them. Hence no need to repeat the criteria name

All the time values should be specified in **ISO8601 (UTC)** format i.e. **yyyy-MM-dd'T'HH:mm'Z'**

Additionally the `offset` and `len` parameters can be used as usual for pagination. The start parameter is base 1.

If you specify a coordinator in the list, that does not exist, no error is thrown; simply the response will be empty or pertaining to the other valid coordinators.
However, if bundle name provided does not exist, an error is thrown.

**Request:**


```
GET /oozie/v1/jobs?bulk=bundle%3Dmy-bundle-app;coordinators%3Dmy-coord-1,my-coord-5;actionStatus%3DKILLED&offset=1&len=50
```

Note that the filter is URL encoded, its decoded value is `user=chitnis`. If typing in browser URL, one can type decoded value itself i.e. using '='

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  offset: 1,
  len: 50,
  total: 1002,
**  bulkresponses: [
**  {
      bulkbundle:
      {
        bundleJobName: "my-bundle-app",
        bundleJobId: "0-200905191240-oozie-B",
        status: "SUSPENDED",
      },
      bulkcoord:
      {
        coordJobName: "my-coord-1",
        status: "SUSPENDED",
      },
      bulkaction:
      {
        id: "0-200905191240-oozie-C@21",
        coordJobId: "0-200905191240-oozie-C",
        actionNumber: 21,
        externalId: "job_00076_0009",
        status: "KILLED",
        externalStatus: "FAILED",
        errorCode: "E0902",
        errorMessage: "Input file corrupt",
        createdTime: "Fri, 02 Jan 2009 00:00:00 GMT",
        nominalTime: "Thu, 01 Jan 2009 00:00:00 GMT",
        missingDependencies: "hdfs://nn:port/user/joe/file.txt"
      },
    },
**  {
      bulkbundle:
      {
        bundleJobName: "my-bundle-app",
        bundleJobId: "0-200905191240-oozie-B",
        status: "SUSPENDED",
      }
      bulkcoord:
      {
        coordJobName: "my-coord-5",
        status: "SUSPENDED",
      }
      bulkaction:
      {
        id: "0-200905191245-oozie-C@114",
        coordJobId: "0-200905191245-oozie-C",
        actionNumber: 114,
        externalId: "job_00076_0265",
        status: "KILLED",
        externalStatus: "KILLED",
        errorCode: "E0603",
        errorMessage: "SQL error in operation ...",
        createdTime: "Fri, 02 Jan 2009 00:00:00 GMT",
        nominalTime: "Thu, 01 Jan 2009 00:00:00 GMT",
        missingDependencies:
      }
    }
    ...
  ]
}
```

## Oozie Web Services API, V2 (Workflow , Coordinator And Bundle)

The Oozie Web Services API is a HTTP REST JSON API.

All responses are in `UTF-8`.

Assuming Oozie is running at `OOZIE_URL`, the following web services end points are supported:

   * \<OOZIE_URL\>/versions
   * \<OOZIE_URL\>/v2/admin
   * \<OOZIE_URL\>/v2/job
   * \<OOZIE_URL\>/v2/jobs

**Changes in v2 job API:**

There is a difference in the JSON format of Job Information API (*/job) particularly for map-reduce action.
No change for other actions.
In v1, externalId and consoleUrl point to spawned child job ID, and externalChildIDs is null in map-reduce action.
In v2, externalId and consoleUrl point to launcher job ID, and externalChildIDs is spawned child job ID in map-reduce action.

v2 supports retrieving of JMS topic on which job notifications are sent

**REST API URL:**


```
GET http://localhost:11000/oozie/v2/job/0000002-130507145349661-oozie-vira-W?show=jmstopic
```

**Changes in v2 admin API:**

v2 adds support for retrieving JMS connection information related to JMS notifications.

**REST API URL:**


```
GET http://localhost:11000/oozie/v2/admin/jmsinfo
```

v2/jobs remain the same as v1/jobs

### Job and Jobs End-Points

#### Job Information

A HTTP GET request retrieves the job information.

**Request:**


```
GET /oozie/v2/job/job-3?show=info&timezone=GMT
```

**Response for a workflow job:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
**jobType: "workflow",
  id: "0-200905191240-oozie-W",
  appName: "indexer-workflow",
  appPath: "hdfs://user/bansalm/indexer.wf",
  externalId: "0-200905191230-oozie-pepe",
  user: "bansalm",
  group: "other",
  status: "RUNNING",
  conf: "<configuration> ... </configuration>",
  createdTime: "Thu, 01 Jan 2009 00:00:00 GMT",
  startTime: "Fri, 02 Jan 2009 00:00:00 GMT",
  endTime: null,
  run: 0,
  actions: [
    {
      id: "0-200905191240-oozie-W@indexer",
      name: "indexer",
      type: "map-reduce",
      conf: "<configuration> ...</configuration>",
      startTime: "Thu, 01 Jan 2009 00:00:00 GMT",
      endTime: "Fri, 02 Jan 2009 00:00:00 GMT",
      status: "OK",
      externalId: "job-123-200903101010",
      externalStatus: "SUCCEEDED",
      trackerUri: "foo:8021",
      consoleUrl: "http://foo:50040/jobdetailshistory.jsp?jobId=job-123-200903101010",
      transition: "reporter",
      data: null,
      stats: null,
      externalChildIDs: "job-123-200903101011"
      errorCode: null,
      errorMessage: null,
      retries: 0
    },
    ...
  ]
}
```

#### Managing a Job
##### Ignore a Coordinator Job or Action

A ignore request is done with an HTTP PUT request with a `ignore`

The `type` parameter supports `action` only.
The `scope` parameter can contain coordinator action id(s) to be ignored.
Multiple action ids can be passed to the `scope` parameter

**Request:**

Ignore a coordinator job

```
PUT /oozie/v2/job/job-3?action=ignore
```

Ignore coordinator actions

```
PUT /oozie/v2/job/job-3?action=ignore&type=action&scope=3-4
```

### Validate End-Point

This endpoint is to validate a workflow, coordinator, bundle XML file.

#### Validate a local file

**Request:**


```
POST /oozie/v2/validate?file=/home/test/myApp/workflow.xml
Content-Type: application/xml;charset=UTF-8
.
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workflow-app xmlns="uri:oozie:workflow:0.3" name="test">
    <start to="shell"/>
    <action name="shell">
        <shell xmlns="uri:oozie:shell-action:0.3">
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <exec>script.sh</exec>
            <argument></argument>
            <file>script.sh</file>
            <capture-output/>
        </shell>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  validate: "Valid workflow-app"
}
```

#### Validate a file in HDFS

You can validate a workflow, coordinator, bundle XML file in HDFS. The XML file must already exist in HDFS.

**Request:**


```
POST /oozie/v2/validate?file=hdfs://localhost:8020/user/test/myApp/workflow.xml
Content-Type: application/xml;charset=UTF-8
.
```

**Response:**


```
HTTP/1.1 200 OK
Content-Type: application/json;charset=UTF-8
.
{
  validate: "Valid workflow-app"
}
```








