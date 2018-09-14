

[::Go back to Oozie Documentation Index::](index.html)

# Oozie Examples

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Command Line Examples

### Setting Up the Examples

Oozie examples are bundled within the Oozie distribution in the `oozie-examples.tar.gz` file.

Expanding this file will create an `examples/` directory in the local file system.

The `examples/` directory must be copied to the user HOME directory in HDFS:


```
$ hadoop fs -put examples examples
```

**NOTE:** If an examples directory already exists in HDFS, it must be deleted before copying it again. Otherwise files may not be
copied.

### Running the Examples

For the Streaming and Pig example, the [Oozie Share Library](DG_QuickStart.html#OozieShareLib) must be installed in HDFS.

Add Oozie `bin/` to the environment PATH.

The examples assume the ResourceManager is `localhost:8032` and the NameNode is `hdfs://localhost:8020`. If the actual
values are different, the job properties files in the examples directory must be edited to the correct values.

The example applications are under the examples/app directory, one directory per example. The directory contains the
application XML file (workflow, or workflow and coordinator), the `job.properties` file to submit the job and any JAR
files the example may need.

The inputs for all examples are in the `examples/input-data/` directory.

The examples create output under the `examples/output-data/${EXAMPLE_NAME}` directory.

**Note**: The `job.properties` file needs to be a local file during submissions, and not a HDFS path.

**How to run an example application:**


```
$ oozie job -oozie http://localhost:11000/oozie -config examples/apps/map-reduce/job.properties -run
.
job: 14-20090525161321-oozie-tucu
```

Check the workflow job status:


```
$ oozie job -oozie http://localhost:11000/oozie -info 14-20090525161321-oozie-tucu
.
.----------------------------------------------------------------------------------------------------------------------------------------------------------------
Workflow Name :  map-reduce-wf
App Path      :  hdfs://localhost:8020/user/tucu/examples/apps/map-reduce
Status        :  SUCCEEDED
Run           :  0
User          :  tucu
Group         :  users
Created       :  2009-05-26 05:01 +0000
Started       :  2009-05-26 05:01 +0000
Ended         :  2009-05-26 05:01 +0000
Actions
.----------------------------------------------------------------------------------------------------------------------------------------------------------------
Action Name             Type        Status     Transition  External Id            External Status  Error Code    Start Time              End Time
.----------------------------------------------------------------------------------------------------------------------------------------------------------------
mr-node                 map-reduce  OK         end         job_200904281535_0254  SUCCEEDED        -             2009-05-26 05:01 +0000  2009-05-26 05:01 +0000
.----------------------------------------------------------------------------------------------------------------------------------------------------------------
```

To check the workflow job status via the Oozie web console, with a browser go to `http://localhost:11000/oozie`.

To avoid having to provide the `-oozie` option with the Oozie URL with every `oozie` command, set `OOZIE_URL` env
variable to the Oozie URL in the shell environment. For example:


```
$ export OOZIE_URL="http://localhost:11000/oozie"
$
$ oozie job -info 14-20090525161321-oozie-tucu
```

## Java API Example

Oozie provides a [Java Client API](./apidocs/org/org/apache/oozie/client/package-summary.html) that simplifies
integrating Oozie with Java applications. This Java Client API is a convenience API to interact with Oozie Web-Services
API.

The following code snippet shows how to submit an Oozie job using the Java Client API.


```
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
.
import java.util.Properties;
.
    ...
.
    // get a OozieClient for local Oozie
    OozieClient wc = new OozieClient("http://bar:11000/oozie");
.
    // create a workflow job configuration and set the workflow application path
    Properties conf = wc.createConfiguration();
    conf.setProperty(OozieClient.APP_PATH, "hdfs://foo:8020/usr/tucu/my-wf-app");
.
    // setting workflow parameters
    conf.setProperty("resourceManager", "foo:8032");
    conf.setProperty("inputDir", "/usr/tucu/inputdir");
    conf.setProperty("outputDir", "/usr/tucu/outputdir");
    ...
.
    // submit and start the workflow job
    String jobId = wc.run(conf);
    System.out.println("Workflow job submitted");
.
    // wait until the workflow job finishes printing the status every 10 seconds
    while (wc.getJobInfo(jobId).getStatus() == Workflow.Status.RUNNING) {
        System.out.println("Workflow job running ...");
        Thread.sleep(10 * 1000);
    }
.
    // print the final status of the workflow job
    System.out.println("Workflow job completed ...");
    System.out.println(wf.getJobInfo(jobId));
    ...
```

## Local Oozie Example

Oozie provides an embedded Oozie implementation,  [LocalOozie](./apidocs/org/apache/oozie/local/LocalOozie.html) ,
which is useful for development, debugging and testing of workflow applications within the convenience of an IDE.

The code snippet below shows the usage of the `LocalOozie` class. All the interaction with Oozie is done using Oozie
 `OozieClient` Java API, as shown in the previous section.

The examples bundled with Oozie include the complete and running class, `LocalOozieExample` from where this snippet was
taken.


```
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
.
import java.util.Properties;
.
    ...
    // start local Oozie
    LocalOozie.start();
.
    // get a OozieClient for local Oozie
    OozieClient wc = LocalOozie.getClient();
.
    // create a workflow job configuration and set the workflow application path
    Properties conf = wc.createConfiguration();
    conf.setProperty(OozieClient.APP_PATH, "hdfs://foo:8020/usr/tucu/my-wf-app");
.
    // setting workflow parameters
    conf.setProperty("resourceManager", "foo:8032");
    conf.setProperty("inputDir", "/usr/tucu/inputdir");
    conf.setProperty("outputDir", "/usr/tucu/outputdir");
    ...
.
    // submit and start the workflow job
    String jobId = wc.run(conf);
    System.out.println("Workflow job submitted");
.
    // wait until the workflow job finishes printing the status every 10 seconds
    while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
        System.out.println("Workflow job running ...");
        Thread.sleep(10 * 1000);
    }
.
    // print the final status of the workflow job
    System.out.println("Workflow job completed ...");
    System.out.println(wc.getJobInfo(jobId));
.
    // stop local Oozie
    LocalOozie.stop();
    ...
```

Also asynchronous actions like FS action can be used / tested using `LocalOozie` / `OozieClient` API. Please see the module
`oozie-mini` for details like `fs-decision.xml` workflow example.


## Fluent Job API Examples

There are some elaborate examples how to use the [Fluent Job API](DG_FluentJobAPI.html), under `examples/fluentjob/`. There are two
simple examples covered under [Fluent Job API :: A Simple Example](DG_FluentJobAPI.html#A_Simple_Example) and
[Fluent Job API :: A More Verbose Example](DG_FluentJobAPI.html#A_More_Verbose_Example).

[::Go back to Oozie Documentation Index::](index.html)


