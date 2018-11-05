

[::Go back to Oozie Documentation Index::](index.html)

-----

# Oozie Specification, a Hadoop Workflow System
**<center>(v5.0)</center>**

The goal of this document is to define a workflow engine system specialized in coordinating the execution of Hadoop
Map/Reduce and Pig jobs.


<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Changelog


**2016FEB19**

   * 3.2.7 Updated notes on System.exit(int n) behavior

**2015APR29**

   * 3.2.1.4 Added notes about Java action retries
   * 3.2.7 Added notes about Java action retries

**2014MAY08**

   * 3.2.2.4 Added support for fully qualified job-xml path

**2013JUL03**

   * Appendix A, Added new workflow schema 0.5 and SLA schema 0.2

**2012AUG30**

   * 4.2.2 Added two EL functions (replaceAll and appendAll)

**2012JUL26**

   * Appendix A, updated XML schema 0.4 to include `parameters` element
   * 4.1 Updated to mention about `parameters` element as of schema 0.4

**2012JUL23**

   * Appendix A, updated XML schema 0.4 (Fs action)
   * 3.2.4 Updated to mention that a `name-node`, a `job-xml`, and a `configuration` element are allowed in the Fs action as of
schema 0.4

**2012JUN19**

   * Appendix A, added XML schema 0.4
   * 3.2.2.4 Updated to mention that multiple `job-xml` elements are allowed as of schema 0.4
   * 3.2.3 Updated to mention that multiple `job-xml` elements are allowed as of schema 0.4

**2011AUG17**

   * 3.2.4 fs 'chmod' xml closing element typo in Example corrected

**2011AUG12**

   * 3.2.4 fs 'move' action characteristics updated, to allow for consistent source and target paths and existing target path only if directory
   * 18, Update the doc for user-retry of workflow action.

**2011FEB19**

   * 10, Update the doc to rerun from the failed node.

**2010OCT31**

   * 17, Added new section on Shared Libraries

**2010APR27**

   * 3.2.3 Added new "arguments" tag to PIG actions
   * 3.2.5 SSH actions are deprecated in Oozie schema 0.1 and removed in Oozie schema 0.2
   * Appendix A, Added schema version 0.2


**2009OCT20**

   * Appendix A, updated XML schema


**2009SEP15**

   * 3.2.6 Removing support for sub-workflow in a different Oozie instance (removing the 'oozie' element)


**2009SEP07**

   * 3.2.2.3 Added Map Reduce Pipes specifications.
   * 3.2.2.4 Map-Reduce Examples. Previously was 3.2.2.3.


**2009SEP02**

   * 10 Added missing skip nodes property name.
   * 3.2.1.4 Reworded action recovery explanation.


**2009AUG26**

   * 3.2.9 Added `java` action type
   * 3.1.4 Example uses EL constant to refer to counter group/name


**2009JUN09**

   * 12.2.4 Added build version resource to admin end-point
   * 3.2.6 Added flag to propagate workflow configuration to sub-workflows
   * 10 Added behavior for workflow job parameters given in the rerun
   * 11.3.4 workflows info returns pagination information


**2009MAY18**

   * 3.1.4 decision node, 'default' element, 'name' attribute changed to 'to'
   * 3.1.5 fork node, 'transition' element changed to 'start', 'to' attribute change to 'path'
   * 3.1.5 join node, 'transition' element remove, added 'to' attribute to 'join' element
   * 3.2.1.4 Rewording on action recovery section
   * 3.2.2 map-reduce action, added 'job-tracker', 'name-node' actions, 'file', 'file' and 'archive' elements
   * 3.2.2.1 map-reduce action, remove from 'streaming' element 'file', 'file' and 'archive' elements
   * 3.2.2.2 map-reduce action, reorganized streaming section
   * 3.2.3 pig action, removed information about implementation (SSH), changed elements names
   * 3.2.4 fs action, removed 'fs-uri' and 'user-name' elements, file system URI is now specified in path, user is propagated
   * 3.2.6 sub-workflow action, renamed elements 'oozie-url' to 'oozie' and 'workflow-app' to 'app-path'
   * 4 Properties that are valid Java identifiers can be used as ${NAME}
   * 4.1 Renamed default properties file from 'configuration.xml' to 'default-configuration.xml'
   * 4.2 Changes in EL Constants and Functions
   * 5 Updated notification behavior and tokens
   * 6 Changed user propagation behavior
   * 7 Changed application packaging from ZIP to HDFS directory
   * Removed application lifecycle and self containment model sections
   * 10 Changed workflow job recovery, simplified recovery behavior
   * 11 Detailed Web Services API
   * 12 Updated  Client API section
   * 15 Updated  Action Executor API section
   * Appendix A XML namespace updated to 'uri:oozie:workflow:0.1'
   * Appendix A Updated XML schema to changes in map-reduce/pig/fs/ssh actions
   * Appendix B Updated workflow example to schema changes


**2009MAR25**

   * Changing all references of HWS to Oozie (project name)
   * Typos, XML Formatting
   * XML Schema URI correction


**2009MAR09**

   * Changed `CREATED` job state to `PREP` to have same states as Hadoop
   * Renamed 'hadoop-workflow' element to 'workflow-app'
   * Decision syntax changed to be 'switch/case' with no transition indirection
   * Action nodes common root element 'action', with the action type as sub-element (using a single built-in XML schema)
   * Action nodes have 2 explicit transitions 'ok to' and 'error to' enforced by XML schema
   * Renamed 'fail' action element to 'kill'
   * Renamed 'hadoop' action element to 'map-reduce'
   * Renamed 'hdfs' action element to 'fs'
   * Updated all XML snippets and examples
   * Made user propagation simpler and consistent
   * Added Oozie XML schema to Appendix A
   * Added workflow example to Appendix B


**2009FEB22**

   * Opened [JIRA HADOOP-5303](https://issues.apache.org/jira/browse/HADOOP-5303)


**27/DEC/2012:**

   * Added information on dropping hcatalog table partitions in prepare block
   * Added hcatalog EL functions section

## 0 Definitions

**Action:** An execution/computation task (Map-Reduce job, Pig job, a shell command). It can also be referred as task or
'action node'.

**Workflow:** A collection of actions arranged in a control dependency DAG (Directed Acyclic Graph). "control dependency"
from one action to another means that the second action can't run until the first action has completed.

**Workflow Definition:** A programmatic description of a workflow that can be executed.

**Workflow Definition Language:** The language used to define a Workflow Definition.

**Workflow Job:** An executable instance of a workflow definition.

**Workflow Engine:** A system that executes workflows jobs. It can also be referred as a DAG engine.

## 1 Specification Highlights

A Workflow application is DAG that coordinates the following types of actions: Hadoop, Pig, and
sub-workflows.

Flow control operations within the workflow applications can be done using decision, fork and join nodes. Cycles in
workflows are not supported.

Actions and decisions can be parameterized with job properties, actions output (i.e. Hadoop counters) and file information (file exists, file size, etc). Formal parameters are expressed in the workflow
definition as `${VAR}` variables.

A Workflow application is a ZIP file that contains the workflow definition (an XML file), all the necessary files to
run all the actions: JAR files for Map/Reduce jobs, shells for streaming Map/Reduce jobs, native libraries, Pig
scripts, and other resource files.

Before running a workflow job, the corresponding workflow application must be deployed in Oozie.

Deploying workflow application and running workflow jobs can be done via command line tools, a WS API and a Java API.

Monitoring the system and workflow jobs can be done via a web console, command line tools, a WS API and a Java API.

When submitting a workflow job, a set of properties resolving all the formal parameters in the workflow definitions
must be provided. This set of properties is a Hadoop configuration.

Possible states for a workflow jobs are: `PREP`, `RUNNING`, `SUSPENDED`, `SUCCEEDED`, `KILLED` and `FAILED`.

In the case of a action start failure in a workflow job, depending on the type of failure, Oozie will attempt automatic
retries, it will request a manual retry or it will fail the workflow job.

Oozie can make HTTP callback notifications on action start/end/failure events and workflow end/failure events.

In the case of workflow job failure, the workflow job can be resubmitted skipping previously completed actions.
Before doing a resubmission the workflow application could be updated with a patch to fix a problem in the workflow
application code.

<a name="WorkflowDefinition"></a>
## 2 Workflow Definition

A workflow definition is a DAG with control flow nodes (start, end, decision, fork, join, kill) or action nodes
(map-reduce, pig, etc.), nodes are connected by transitions arrows.

The workflow definition language is XML based and it is called hPDL (Hadoop Process Definition Language).

Refer to the Appendix A for the[Oozie Workflow Definition XML Schema](WorkflowFunctionalSpec.html#OozieWFSchema). Appendix
B has [Workflow Definition Examples](WorkflowFunctionalSpec.html#OozieWFExamples).

### 2.1 Cycles in Workflow Definitions

Oozie does not support cycles in workflow definitions, workflow definitions must be a strict DAG.

At workflow application deployment time, if Oozie detects a cycle in the workflow definition it must fail the
deployment.

## 3 Workflow Nodes

Workflow nodes are classified in control flow nodes and action nodes:

   * **Control flow nodes:** nodes that control the start and end of the workflow and workflow job execution path.
   * **Action nodes:** nodes that trigger the execution of a computation/processing task.

Node names and transitions must be conform to the following pattern `[a-zA-Z][\-_a-zA-Z0-0]*`, of up to 20 characters
long.

### 3.1 Control Flow Nodes

Control flow nodes define the beginning and the end of a workflow (the `start`, `end` and `kill` nodes) and provide a
mechanism to control the workflow execution path (the `decision`, `fork` and `join` nodes).

<a name="StartNode"></a>
#### 3.1.1 Start Control Node

The `start` node is the entry point for a workflow job, it indicates the first workflow node the workflow job must
transition to.

When a workflow is started, it automatically transitions to the node specified in the `start`.

A workflow definition must have one `start` node.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
  ...
  <start to="[NODE-NAME]"/>
  ...
</workflow-app>
```

The `to` attribute is the name of first workflow node to execute.

**Example:**


```
<workflow-app name="foo-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <start to="firstHadoopJob"/>
    ...
</workflow-app>
```

<a name="EndNode"></a>
#### 3.1.2 End Control Node

The `end` node is the end for a workflow job, it indicates that the workflow job has completed successfully.

When a workflow job reaches the `end` it finishes successfully (SUCCEEDED).

If one or more actions started by the workflow job are executing when the `end` node is reached, the actions will be
killed. In this scenario the workflow job is still considered as successfully run.

A workflow definition must have one `end` node.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <end name="[NODE-NAME]"/>
    ...
</workflow-app>
```

The `name` attribute is the name of the transition to do to end the workflow job.

**Example:**


```
<workflow-app name="foo-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <end name="end"/>
</workflow-app>
```

<a name="KillNode"></a>
#### 3.1.3 Kill Control Node

The `kill` node allows a workflow job to kill itself.

When a workflow job reaches the `kill` it finishes in error (KILLED).

If one or more actions started by the workflow job are executing when the `kill` node is reached, the actions will be
killed.

A workflow definition may have zero or more `kill` nodes.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <kill name="[NODE-NAME]">
        <message>[MESSAGE-TO-LOG]</message>
    </kill>
    ...
</workflow-app>
```

The `name` attribute in the `kill` node is the name of the Kill action node.

The content of the `message` element will be logged as the kill reason for the workflow job.

A `kill` node does not have transition elements because it ends the workflow job, as `KILLED`.

**Example:**


```
<workflow-app name="foo-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <kill name="killBecauseNoInput">
        <message>Input unavailable</message>
    </kill>
    ...
</workflow-app>
```

<a name="DecisionNode"></a>
#### 3.1.4 Decision Control Node

A `decision` node enables a workflow to make a selection on the execution path to follow.

The behavior of a `decision` node can be seen as a switch-case statement.

A `decision` node consists of a list of predicates-transition pairs plus a default transition. Predicates are evaluated
in order or appearance until one of them evaluates to `true` and the corresponding transition is taken. If none of the
predicates evaluates to `true` the `default` transition is taken.

Predicates are JSP Expression Language (EL) expressions (refer to section 4.2 of this document) that resolve into a
boolean value, `true` or `false`. For example:


```
    ${fs:fileSize('/usr/foo/myinputdir') gt 10 * GB}
```

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <decision name="[NODE-NAME]">
        <switch>
            <case to="[NODE_NAME]">[PREDICATE]</case>
            ...
            <case to="[NODE_NAME]">[PREDICATE]</case>
            <default to="[NODE_NAME]"/>
        </switch>
    </decision>
    ...
</workflow-app>
```

The `name` attribute in the `decision` node is the name of the decision node.

Each `case` elements contains a predicate and a transition name. The predicate ELs are evaluated
in order until one returns `true` and the corresponding transition is taken.

The `default` element indicates the transition to take if none of the predicates evaluates
to `true`.

All decision nodes must have a `default` element to avoid bringing the workflow into an error
state if none of the predicates evaluates to true.

**Example:**


```
<workflow-app name="foo-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <decision name="mydecision">
        <switch>
            <case to="reconsolidatejob">
              ${fs:fileSize(secondjobOutputDir) gt 10 * GB}
            </case> <case to="rexpandjob">
              ${fs:fileSize(secondjobOutputDir) lt 100 * MB}
            </case>
            <case to="recomputejob">
              ${ hadoop:counters('secondjob')[RECORDS][REDUCE_OUT] lt 1000000 }
            </case>
            <default to="end"/>
        </switch>
    </decision>
    ...
</workflow-app>
```

<a name="ForkJoinNodes"></a>
#### 3.1.5 Fork and Join Control Nodes

A `fork` node splits one path of execution into multiple concurrent paths of execution.

A `join` node waits until every concurrent execution path of a previous `fork` node arrives to it.

The `fork` and `join` nodes must be used in pairs. The `join` node assumes concurrent execution paths are children of
the same `fork` node.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <fork name="[FORK-NODE-NAME]">
        <path start="[NODE-NAME]" />
        ...
        <path start="[NODE-NAME]" />
    </fork>
    ...
    <join name="[JOIN-NODE-NAME]" to="[NODE-NAME]" />
    ...
</workflow-app>
```

The `name` attribute in the `fork` node is the name of the workflow fork node. The `start` attribute in the `path`
elements in the `fork` node indicate the name of the workflow node that will be part of the concurrent execution paths.

The `name` attribute in the `join` node is the name of the workflow join node. The `to` attribute in the `join` node
indicates the name of the workflow node that will executed after all concurrent execution paths of the corresponding
fork arrive to the join node.

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <fork name="forking">
        <path start="firstparalleljob"/>
        <path start="secondparalleljob"/>
    </fork>
    <action name="firstparallejob">
        <map-reduce>
            <resource-manager>foo:8032</resource-manager>
            <name-node>bar:8020</name-node>
            <job-xml>job1.xml</job-xml>
        </map-reduce>
        <ok to="joining"/>
        <error to="kill"/>
    </action>
    <action name="secondparalleljob">
        <map-reduce>
            <resource-manager>foo:8032</resource-manager>
            <name-node>bar:8020</name-node>
            <job-xml>job2.xml</job-xml>
        </map-reduce>
        <ok to="joining"/>
        <error to="kill"/>
    </action>
    <join name="joining" to="nextaction"/>
    ...
</workflow-app>
```

By default, Oozie performs some validation that any forking in a workflow is valid and won't lead to any incorrect behavior or
instability.  However, if Oozie is preventing a workflow from being submitted and you are very certain that it should work, you can
disable forkjoin validation so that Oozie will accept the workflow.  To disable this validation just for a specific workflow, simply
set `oozie.wf.validate.ForkJoin` to `false` in the job.properties file.  To disable this validation for all workflows, simply set
`oozie.validate.ForkJoin` to `false` in the oozie-site.xml file.  Disabling this validation is determined by the AND of both of
these properties, so it will be disabled if either or both are set to false and only enabled if both are set to true (or not
specified).

<a name="ActionNodes"></a>
### 3.2 Workflow Action Nodes

Action nodes are the mechanism by which a workflow triggers the execution of a computation/processing task.

#### 3.2.1 Action Basis

The following sub-sections define common behavior and capabilities for all action types.

##### 3.2.1.1 Action Computation/Processing Is Always Remote

All computation/processing tasks triggered by an action node are remote to Oozie. No workflow application specific
computation/processing task is executed within Oozie.

##### 3.2.1.2 Actions Are Asynchronous

All computation/processing tasks triggered by an action node are executed asynchronously by Oozie. For most types of
computation/processing tasks triggered by workflow action, the workflow job has to wait until the
computation/processing task completes before transitioning to the following node in the workflow.

The exception is the `fs` action that is handled as a synchronous action.

Oozie can detect completion of computation/processing tasks by two different means, callbacks and polling.

When a computation/processing tasks is started by Oozie, Oozie provides a unique callback URL to the task, the task
should invoke the given URL to notify its completion.

For cases that the task failed to invoke the callback URL for any reason (i.e. a transient network failure) or when
the type of task cannot invoke the callback URL upon completion, Oozie has a mechanism to poll computation/processing
tasks for completion.

##### 3.2.1.3 Actions Have 2 Transitions, `ok` and `error`

If a computation/processing task -triggered by a workflow- completes successfully, it transitions to `ok`.

If a computation/processing task -triggered by a workflow- fails to complete successfully, its transitions to `error`.

If a computation/processing task exits in error, there computation/processing task must provide `error-code` and
 `error-message` information to Oozie. This information can be used from `decision` nodes to implement a fine grain
error handling at workflow application level.

Each action type must clearly define all the error codes it can produce.

##### 3.2.1.4 Action Recovery

Oozie provides recovery capabilities when starting or ending actions.

Once an action starts successfully Oozie will not retry starting the action if the action fails during its execution.
The assumption is that the external system (i.e. Hadoop) executing the action has enough resilience to recover jobs
once it has started (i.e. Hadoop task retries).

Java actions are a special case with regard to retries.  Although Oozie itself does not retry Java actions
should they fail after they have successfully started, Hadoop itself can cause the action to be restarted due to a
map task retry on the map task running the Java application.  See the Java Action section below for more detail.

For failures that occur prior to the start of the job, Oozie will have different recovery strategies depending on the
nature of the failure.

If the failure is of transient nature, Oozie will perform retries after a pre-defined time interval. The number of
retries and timer interval for a type of action must be pre-configured at Oozie level. Workflow jobs can override such
configuration.

Examples of a transient failures are network problems or a remote system temporary unavailable.

If the failure is of non-transient nature, Oozie will suspend the workflow job until an manual or programmatic
intervention resumes the workflow job and the action start or end is retried. It is the responsibility of an
administrator or an external managing system to perform any necessary cleanup before resuming the workflow job.

If the failure is an error and a retry will not resolve the problem, Oozie will perform the error transition for the
action.

<a name="MapReduceAction"></a>
#### 3.2.2 Map-Reduce Action

The `map-reduce` action starts a Hadoop map/reduce job from a workflow. Hadoop jobs can be Java Map/Reduce jobs or
streaming jobs.

A `map-reduce` action can be configured to perform file system cleanup and directory creation before starting the
map reduce job. This capability enables Oozie to retry a Hadoop job in the situation of a transient failure (Hadoop
checks the non-existence of the job output directory and then creates it when the Hadoop job is starting, thus a retry
without cleanup of the job output directory would fail).

The workflow job will wait until the Hadoop map/reduce job completes before continuing to the next action in the
workflow execution path.

The counters of the Hadoop job and job exit status (`FAILED`, `KILLED` or `SUCCEEDED`) must be available to the
workflow job after the Hadoop jobs ends. This information can be used from within decision nodes and other actions
configurations.

The `map-reduce` action has to be configured with all the necessary Hadoop JobConf properties to run the Hadoop
map/reduce job.

Hadoop JobConf properties can be specified as part of

   * the `config-default.xml` or
   * JobConf XML file bundled with the workflow application or
   * \<global\> tag in workflow definition or
   * Inline `map-reduce` action configuration or
   * An implementation of OozieActionConfigurator specified by the \<config-class\> tag in workflow definition.

The configuration properties are loaded in the following above order i.e. `streaming`, `job-xml`, `configuration`,
and `config-class`, and the precedence order is later values override earlier values.

Streaming and inline property values can be parameterized (templatized) using EL expressions.

The Hadoop `mapred.job.tracker` and `fs.default.name` properties must not be present in the job-xml and inline
configuration.

<a name="FilesArchives"></a>
##### 3.2.2.1 Adding Files and Archives for the Job

The `file`, `archive` elements make available, to map-reduce jobs, files and archives. If the specified path is
relative, it is assumed the file or archiver are within the application directory, in the corresponding sub-path.
If the path is absolute, the file or archive it is expected in the given absolute path.

Files specified with the `file` element, will be symbolic links in the home directory of the task.

If a file is a native library (an '.so' or a '.so.#' file), it will be symlinked as and '.so' file in the task running
directory, thus available to the task JVM.

To force a symlink for a file on the task running directory, use a '#' followed by the symlink name. For example
'mycat.sh#cat'.

Refer to Hadoop distributed cache documentation for details more details on files and archives.

##### 3.2.2.2 Configuring the MapReduce action with Java code

Java code can be used to further configure the MapReduce action.  This can be useful if you already have "driver" code for your
MapReduce action, if you're more familiar with MapReduce's Java API, if there's some configuration that requires logic, or some
configuration that's difficult to do in straight XML (e.g. Avro).

Create a class that implements the org.apache.oozie.action.hadoop.OozieActionConfigurator interface from the "oozie-sharelib-oozie"
artifact.  It contains a single method that receives a `JobConf` as an argument.  Any configuration properties set on this `JobConf`
will be used by the MapReduce action.

The OozieActionConfigurator has this signature:

```
public interface OozieActionConfigurator {
    public void configure(JobConf actionConf) throws OozieActionConfiguratorException;
}
```
where `actionConf` is the `JobConf` you can update.  If you need to throw an Exception, you can wrap it in
an `OozieActionConfiguratorException`, also in the "oozie-sharelib-oozie" artifact.

For example:

```
package com.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.action.hadoop.OozieActionConfigurator;
import org.apache.oozie.action.hadoop.OozieActionConfiguratorException;
import org.apache.oozie.example.SampleMapper;
import org.apache.oozie.example.SampleReducer;

public class MyConfigClass implements OozieActionConfigurator {

    @Override
    public void configure(JobConf actionConf) throws OozieActionConfiguratorException {
        if (actionConf.getUser() == null) {
            throw new OozieActionConfiguratorException("No user set");
        }
        actionConf.setMapperClass(SampleMapper.class);
        actionConf.setReducerClass(SampleReducer.class);
        FileInputFormat.setInputPaths(actionConf, new Path("/user/" + actionConf.getUser() + "/input-data"));
        FileOutputFormat.setOutputPath(actionConf, new Path("/user/" + actionConf.getUser() + "/output"));
        ...
    }
}
```

To use your config class in your MapReduce action, simply compile it into a jar, make the jar available to your action, and specify
the class name in the `config-class` element (this requires at least schema 0.5):

```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <map-reduce>
            ...
            <job-xml>[JOB-XML-FILE]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <config-class>com.example.MyConfigClass</config-class>
            ...
        </map-reduce>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

Another example of this can be found in the "map-reduce" example that comes with Oozie.

A useful tip: The initial `JobConf` passed to the `configure` method includes all of the properties listed in the `configuration`
section of the MR action in a workflow.  If you need to pass any information to your OozieActionConfigurator, you can simply put
them here.

<a name="StreamingMapReduceAction"></a>
##### 3.2.2.3 Streaming

Streaming information can be specified in the `streaming` element.

The `mapper` and `reducer` elements are used to specify the executable/script to be used as mapper and reducer.

User defined scripts must be bundled with the workflow application and they must be declared in the `files` element of
the streaming configuration. If the are not declared in the `files` element of the configuration it is assumed they
will be available (and in the command PATH) of the Hadoop slave machines.

Some streaming jobs require Files found on HDFS to be available to the mapper/reducer scripts. This is done using
the `file` and `archive` elements described in the previous section.

The Mapper/Reducer can be overridden by a `mapred.mapper.class` or `mapred.reducer.class` properties in the `job-xml`
file or `configuration` elements.

<a name="PipesMapReduceAction"></a>
##### 3.2.2.4 Pipes

Pipes information can be specified in the `pipes` element.

A subset of the command line options which can be used while using the Hadoop Pipes Submitter can be specified
via elements - `map`, `reduce`, `inputformat`, `partitioner`, `writer`, `program`.

The `program` element is used to specify the executable/script to be used.

User defined program must be bundled with the workflow application.

Some pipe jobs require Files found on HDFS to be available to the mapper/reducer scripts. This is done using
the `file` and `archive` elements described in the previous section.

Pipe properties can be overridden by specifying them in the `job-xml` file or `configuration` element.

##### 3.2.2.5 Syntax


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <map-reduce>
            <resource-manager>[RESOURCE-MANAGER]</resource-manager>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
                <delete path="[PATH]"/>
                ...
                <mkdir path="[PATH]"/>
                ...
            </prepare>
            <streaming>
                <mapper>[MAPPER-PROCESS]</mapper>
                <reducer>[REDUCER-PROCESS]</reducer>
                <record-reader>[RECORD-READER-CLASS]</record-reader>
                <record-reader-mapping>[NAME=VALUE]</record-reader-mapping>
                ...
                <env>[NAME=VALUE]</env>
                ...
            </streaming>
			<!-- Either streaming or pipes can be specified for an action, not both -->
            <pipes>
                <map>[MAPPER]</map>
                <reduce>[REDUCER]</reducer>
                <inputformat>[INPUTFORMAT]</inputformat>
                <partitioner>[PARTITIONER]</partitioner>
                <writer>[OUTPUTFORMAT]</writer>
                <program>[EXECUTABLE]</program>
            </pipes>
            <job-xml>[JOB-XML-FILE]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <config-class>com.example.MyConfigClass</config-class>
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
        </map-reduce>

        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The `prepare` element, if present, indicates a list of paths to delete before starting the job. This should be used
exclusively for directory cleanup or dropping of hcatalog table or table partitions for the job to be executed. The delete operation
will be performed in the `fs.default.name` filesystem for hdfs URIs. The format for specifying hcatalog table URI is
hcat://[metastore server]:[port]/[database name]/[table name] and format to specify a hcatalog table partition URI is
`hcat://[metastore server]:[port]/[database name]/[table name]/[partkey1]=[value];[partkey2]=[value]`.
In case of a hcatalog URI, the hive-site.xml needs to be shipped using `file` tag and the hcatalog and hive jars
need to be placed in workflow lib directory or specified using `archive` tag.

The `job-xml` element, if present, must refer to a Hadoop JobConf `job.xml` file bundled in the workflow application.
By default the `job.xml` file is taken from the workflow application namenode, regardless the namenode specified for the action.
To specify a `job.xml` on another namenode use a fully qualified file path.
The `job-xml` element is optional and as of schema 0.4, multiple `job-xml` elements are allowed in order to specify multiple Hadoop JobConf `job.xml` files.

The `configuration` element, if present, contains JobConf properties for the Hadoop job.

Properties specified in the `configuration` element override properties specified in the file specified in the
 `job-xml` element.

As of schema 0.5, the `config-class` element, if present, contains a class that implements OozieActionConfigurator that can be used
to further configure the MapReduce job.

Properties specified in the `config-class` class override properties specified in `configuration` element.

External Stats can be turned on/off by specifying the property _oozie.action.external.stats.write_ as _true_ or _false_ in the configuration element of workflow.xml. The default value for this property is _false_.

The `file` element, if present, must specify the target symbolic link for binaries by separating the original file and target with a # (file#target-sym-link). This is not required for libraries.

The `mapper` and `reducer` process for streaming jobs, should specify the executable command with URL encoding. e.g. '%' should be replaced by '%25'.

**Example:**


```
<workflow-app name="foo-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="myfirstHadoopJob">
        <map-reduce>
            <resource-manager>foo:8032</resource-manager>
            <name-node>bar:8020</name-node>
            <prepare>
                <delete path="hdfs://foo:8020/usr/tucu/output-data"/>
            </prepare>
            <job-xml>/myfirstjob.xml</job-xml>
            <configuration>
                <property>
                    <name>mapred.input.dir</name>
                    <value>/usr/tucu/input-data</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>/usr/tucu/input-data</value>
                </property>
                <property>
                    <name>mapred.reduce.tasks</name>
                    <value>${firstJobReducers}</value>
                </property>
                <property>
                    <name>oozie.action.external.stats.write</name>
                    <value>true</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to="myNextAction"/>
        <error to="errorCleanup"/>
    </action>
    ...
</workflow-app>
```

In the above example, the number of Reducers to be used by the Map/Reduce job has to be specified as a parameter of
the workflow job configuration when creating the workflow job.

**Streaming Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="firstjob">
        <map-reduce>
            <resource-manager>foo:8032</resource-manager>
            <name-node>bar:8020</name-node>
            <prepare>
                <delete path="${output}"/>
            </prepare>
            <streaming>
                <mapper>/bin/bash testarchive/bin/mapper.sh testfile</mapper>
                <reducer>/bin/bash testarchive/bin/reducer.sh</reducer>
            </streaming>
            <configuration>
                <property>
                    <name>mapred.input.dir</name>
                    <value>${input}</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>${output}</value>
                </property>
                <property>
                    <name>stream.num.map.output.key.fields</name>
                    <value>3</value>
                </property>
            </configuration>
            <file>/users/blabla/testfile.sh#testfile</file>
            <archive>/users/blabla/testarchive.jar#testarchive</archive>
        </map-reduce>
        <ok to="end"/>
        <error to="kill"/>
    </action>
  ...
</workflow-app>
```


**Pipes Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="firstjob">
        <map-reduce>
            <resource-manager>foo:8032</resource-manager>
            <name-node>bar:8020</name-node>
            <prepare>
                <delete path="${output}"/>
            </prepare>
            <pipes>
                <program>bin/wordcount-simple#wordcount-simple</program>
            </pipes>
            <configuration>
                <property>
                    <name>mapred.input.dir</name>
                    <value>${input}</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>${output}</value>
                </property>
            </configuration>
            <archive>/users/blabla/testarchive.jar#testarchive</archive>
        </map-reduce>
        <ok to="end"/>
        <error to="kill"/>
    </action>
  ...
</workflow-app>
```


<a name="PigAction"></a>
#### 3.2.3 Pig Action

The `pig` action starts a Pig job.

The workflow job will wait until the pig job completes before continuing to the next action.

The `pig` action has to be configured with the resource-manager, name-node, pig script and the necessary parameters and
configuration to run the Pig job.

A `pig` action can be configured to perform HDFS files/directories cleanup or HCatalog partitions cleanup before
starting the Pig job. This capability enables Oozie to retry a Pig job in the situation of a transient failure (Pig
creates temporary directories for intermediate data, thus a retry without cleanup would fail).

Hadoop JobConf properties can be specified as part of

   * the `config-default.xml` or
   * JobConf XML file bundled with the workflow application or
   * \<global\> tag in workflow definition or
   * Inline `pig` action configuration.

The configuration properties are loaded in the following above order i.e. `job-xml` and `configuration`, and
the precedence order is later values override earlier values.

Inline property values can be parameterized (templatized) using EL expressions.

The YARN `yarn.resourcemanager.address` and HDFS `fs.default.name` properties must not be present in the job-xml and inline
configuration.

As with Hadoop map-reduce jobs, it  is possible to add files and archives to be available to the Pig job, refer to
section [#FilesArchives][Adding Files and Archives for the Job].


**Syntax for Pig actions in Oozie schema 1.0:**

```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <pig>
            <resource-manager>[RESOURCE-MANAGER]</resource-manager>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
               <delete path="[PATH]"/>
               ...
               <mkdir path="[PATH]"/>
               ...
            </prepare>
            <job-xml>[JOB-XML-FILE]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <script>[PIG-SCRIPT]</script>
            <param>[PARAM-VALUE]</param>
                ...
            <param>[PARAM-VALUE]</param>
            <argument>[ARGUMENT-VALUE]</argument>
                ...
            <argument>[ARGUMENT-VALUE]</argument>
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
        </pig>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

**Syntax for Pig actions in Oozie schema 0.2:**

```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.2">
    ...
    <action name="[NODE-NAME]">
        <pig>
            <job-tracker>[JOB-TRACKER]</job-tracker>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
               <delete path="[PATH]"/>
               ...
               <mkdir path="[PATH]"/>
               ...
            </prepare>
            <job-xml>[JOB-XML-FILE]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <script>[PIG-SCRIPT]</script>
            <param>[PARAM-VALUE]</param>
                ...
            <param>[PARAM-VALUE]</param>
            <argument>[ARGUMENT-VALUE]</argument>
                ...
            <argument>[ARGUMENT-VALUE]</argument>
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
        </pig>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

**Syntax for Pig actions in Oozie schema 0.1:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:0.1">
    ...
    <action name="[NODE-NAME]">
        <pig>
            <job-tracker>[JOB-TRACKER]</job-tracker>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
               <delete path="[PATH]"/>
               ...
               <mkdir path="[PATH]"/>
               ...
            </prepare>
            <job-xml>[JOB-XML-FILE]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <script>[PIG-SCRIPT]</script>
            <param>[PARAM-VALUE]</param>
                ...
            <param>[PARAM-VALUE]</param>
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
        </pig>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The `prepare` element, if present, indicates a list of paths to delete before starting the job. This should be used
exclusively for directory cleanup or dropping of hcatalog table or table partitions for the job to be executed. The delete operation
will be performed in the `fs.default.name` filesystem for hdfs URIs. The format for specifying hcatalog table URI is
hcat://[metastore server]:[port]/[database name]/[table name] and format to specify a hcatalog table partition URI is
`hcat://[metastore server]:[port]/[database name]/[table name]/[partkey1]=[value];[partkey2]=[value]`.
In case of a hcatalog URI, the hive-site.xml needs to be shipped using `file` tag and the hcatalog and hive jars
need to be placed in workflow lib directory or specified using `archive` tag.

The `job-xml` element, if present, must refer to a Hadoop JobConf `job.xml` file bundled in the workflow application.
The `job-xml` element is optional and as of schema 0.4, multiple `job-xml` elements are allowed in order to specify multiple Hadoop JobConf `job.xml` files.

The `configuration` element, if present, contains JobConf properties for the underlying Hadoop jobs.

Properties specified in the `configuration` element override properties specified in the file specified in the
 `job-xml` element.

External Stats can be turned on/off by specifying the property _oozie.action.external.stats.write_ as _true_ or _false_ in the configuration element of workflow.xml. The default value for this property is _false_.

The inline and job-xml configuration properties are passed to the Hadoop jobs submitted by Pig runtime.

The `script` element contains the pig script to execute. The pig script can be templatized with variables of the
form `${VARIABLE}`. The values of these variables can then be specified using the `params` element.

NOTE: Oozie will perform the parameter substitution before firing the pig job. This is different from the
[parameter substitution mechanism provided by Pig](http://wiki.apache.org/pig/ParameterSubstitution), which has a
few limitations.

The `params` element, if present, contains parameters to be passed to the pig script.

**In Oozie schema 0.2:**
The `arguments` element, if present, contains arguments to be passed to the pig script.


All the above elements can be parameterized (templatized) using EL expressions.

**Example for Oozie schema 0.2:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:0.2">
    ...
    <action name="myfirstpigjob">
        <pig>
            <job-tracker>foo:8021</job-tracker>
            <name-node>bar:8020</name-node>
            <prepare>
                <delete path="${jobOutput}"/>
            </prepare>
            <configuration>
                <property>
                    <name>mapred.compress.map.output</name>
                    <value>true</value>
                </property>
                <property>
                    <name>oozie.action.external.stats.write</name>
                    <value>true</value>
                </property>
            </configuration>
            <script>/mypigscript.pig</script>
            <argument>-param</argument>
            <argument>INPUT=${inputDir}</argument>
            <argument>-param</argument>
            <argument>OUTPUT=${outputDir}/pig-output3</argument>
        </pig>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```


**Example for Oozie schema 0.1:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:0.1">
    ...
    <action name="myfirstpigjob">
        <pig>
            <job-tracker>foo:8021</job-tracker>
            <name-node>bar:8020</name-node>
            <prepare>
                <delete path="${jobOutput}"/>
            </prepare>
            <configuration>
                <property>
                    <name>mapred.compress.map.output</name>
                    <value>true</value>
                </property>
            </configuration>
            <script>/mypigscript.pig</script>
            <param>InputDir=/home/tucu/input-data</param>
            <param>OutputDir=${jobOutput}</param>
        </pig>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

<a name="FsAction"></a>
#### 3.2.4 Fs (HDFS) action

The `fs` action allows to manipulate files and directories in HDFS from a workflow application. The supported commands
are `move`, `delete`, `mkdir`, `chmod`, `touchz`, `setrep` and `chgrp`.

The FS commands are executed synchronously from within the FS action, the workflow job will wait until the specified
file commands are completed before continuing to the next action.

Path names specified in the `fs` action can be parameterized (templatized) using EL expressions.
Path name should be specified as a absolute path. In case of `move`, `delete`, `chmod` and `chgrp` commands, a glob pattern can also be specified instead of an absolute path.
For `move`, glob pattern can only be specified for source path and not the target.

Each file path must specify the file system URI, for move operations, the target must not specify the system URI.

**IMPORTANT:** For the purposes of copying files within a cluster it is recommended to refer to the `distcp` action
instead. Refer to [`distcp`](DG_DistCpActionExtension.html) action to copy files within a cluster.

**IMPORTANT:** All the commands within `fs` action do not happen atomically, if a `fs` action fails half way in the
commands being executed, successfully executed commands are not rolled back. The `fs` action, before executing any
command must check that source paths exist and target paths don't exist (constraint regarding target relaxed for the `move` action. See below for details), thus failing before executing any command.
Therefore the validity of all paths specified in one `fs` action are evaluated before any of the file operation are
executed. Thus there is less chance of an error occurring while the `fs` action executes.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <fs>
            <delete path='[PATH]' skip-trash='[true/false]'/>
            ...
            <mkdir path='[PATH]'/>
            ...
            <move source='[SOURCE-PATH]' target='[TARGET-PATH]'/>
            ...
            <chmod path='[PATH]' permissions='[PERMISSIONS]' dir-files='false' />
            ...
            <touchz path='[PATH]' />
            ...
            <chgrp path='[PATH]' group='[GROUP]' dir-files='false' />
            ...
            <setrep path='[PATH]' replication-factor='2'/>
        </fs>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The `delete` command deletes the specified path, if it is a directory it deletes recursively all its content and then
deletes the directory. By default it does skip trash. It can be moved to trash by setting the value of skip-trash to
'false'. It can also be used to drop hcat tables/partitions. This is the only FS command which supports HCatalog URIs as well.
For eg:

```
<delete path='hcat://[metastore server]:[port]/[database name]/[table name]'/>
OR
<delete path='hcat://[metastore server]:[port]/[database name]/[table name]/[partkey1]=[value];[partkey2]=[value];...'/>
```

The `mkdir` command creates the specified directory, it creates all missing directories in the path. If the directory
already exist it does a no-op.

In the `move` command the `source` path must exist. The following scenarios are addressed for a `move`:

   * The file system URI(e.g. `hdfs://{nameNode}`) can be skipped in the `target` path. It is understood to be the same as that of the source. But if the target path does contain the system URI, it cannot be different than that of the source.
   * The parent directory of the `target` path must exist
   * For the `target` path, if it is a file, then it must not already exist.
   * However, if the `target` path is an already existing directory, the `move` action will place your `source` as a child of the `target` directory.

The `chmod` command changes the permissions for the specified path. Permissions can be specified using the Unix Symbolic
representation (e.g. -rwxrw-rw-) or an octal representation (755).
When doing a `chmod` command on a directory, by default the command is applied to the directory and the files one level
within the directory. To apply the `chmod` command to the directory, without affecting the files within it,
the `dir-files` attribute must be set to `false`. To apply the `chmod` command
recursively to all levels within a directory, put a `recursive` element inside the \<chmod\> element.

The `touchz` command creates a zero length file in the specified path if none exists. If one already exists, then touchz will perform a touch operation.
Touchz works only for absolute paths.

The `chgrp` command changes the group for the specified path.
When doing a `chgrp` command on a directory, by default the command is applied to the directory and the files one level
within the directory. To apply the `chgrp` command to the directory, without affecting the files within it,
the `dir-files` attribute must be set to `false`.
To apply the `chgrp` command recursively to all levels within a directory, put a `recursive` element inside the \<chgrp\> element.

The `setrep` command changes replication factor of an hdfs file(s). Changing RF of directories or symlinks is not
supported; this action requires an argument for RF.

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="hdfscommands">
         <fs>
            <delete path='hdfs://foo:8020/usr/tucu/temp-data'/>
            <mkdir path='archives/${wf:id()}'/>
            <move source='${jobInput}' target='archives/${wf:id()}/processed-input'/>
            <chmod path='${jobOutput}' permissions='-rwxrw-rw-' dir-files='true'><recursive/></chmod>
            <chgrp path='${jobOutput}' group='testgroup' dir-files='true'><recursive/></chgrp>
            <setrep path='archives/${wf:id()/filename(s)}' replication-factor='2'/>
        </fs>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

In the above example, a directory named after the workflow job ID is created and the input of the job, passed as
workflow configuration parameter, is archived under the previously created directory.

As of schema 0.4, if a `name-node` element is specified, then it is not necessary for any of the paths to start with the file system
URI as it is taken from the `name-node` element. This is also true if the name-node is specified in the global section
(see [Global Configurations](WorkflowFunctionalSpec.html#GlobalConfigurations))

As of schema 0.4, zero or more `job-xml` elements can be specified; these must refer to Hadoop JobConf `job.xml` formatted files
bundled in the workflow application. They can be used to set additional properties for the FileSystem instance.

As of schema 0.4, if a `configuration` element is specified, then it will also be used to set additional JobConf properties for the
FileSystem instance. Properties specified in the `configuration` element override properties specified in the files specified
by any `job-xml` elements.

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:0.4">
    ...
    <action name="hdfscommands">
        <fs>
           <name-node>hdfs://foo:8020</name-node>
           <job-xml>fs-info.xml</job-xml>
           <configuration>
             <property>
               <name>some.property</name>
               <value>some.value</value>
             </property>
           </configuration>
           <delete path='/usr/tucu/temp-data'/>
        </fs>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

<a name="SubWorkflowAction"></a>
#### 3.2.5 Sub-workflow Action

The `sub-workflow` action runs a child workflow job, the child workflow job can be in the same Oozie system or in
another Oozie system.

The parent workflow job will wait until the child workflow job has completed.

There can be several sub-workflows defined within a single workflow, each under its own action element.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <sub-workflow>
            <app-path>[WF-APPLICATION-PATH]</app-path>
            <propagate-configuration/>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
        </sub-workflow>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The child workflow job runs in the same Oozie system instance where the parent workflow job is running.

The `app-path` element specifies the path to the workflow application of the child workflow job.

The `propagate-configuration` flag, if present, indicates that the workflow job configuration should be propagated to
the child workflow.

The `configuration` section can be used to specify the job properties that are required to run the child workflow job.

The configuration of the `sub-workflow` action can be parameterized (templatized) using EL expressions.

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="a">
        <sub-workflow>
            <app-path>child-wf</app-path>
            <configuration>
                <property>
                    <name>input.dir</name>
                    <value>${wf:id()}/second-mr-output</value>
                </property>
            </configuration>
        </sub-workflow>
        <ok to="end"/>
        <error to="kill"/>
    </action>
    ...
</workflow-app>
```

In the above example, the workflow definition with the name `child-wf` will be run on the Oozie instance at
 `.http://myhost:11000/oozie`. The specified workflow application must be already deployed on the target Oozie instance.

A configuration parameter `input.dir` is being passed as job property to the child workflow job.

The subworkflow can inherit the lib jars from the parent workflow by setting `oozie.subworkflow.classpath.inheritance` to true
in oozie-site.xml or on a per-job basis by setting `oozie.wf.subworkflow.classpath.inheritance` to true in a job.properties file.
If both are specified, `oozie.wf.subworkflow.classpath.inheritance` has priority.  If the subworkflow and the parent have
conflicting jars, the subworkflow's jar has priority.  By default, `oozie.wf.subworkflow.classpath.inheritance` is set to false.

To prevent errant workflows from starting infinitely recursive subworkflows, `oozie.action.subworkflow.max.depth` can be specified
in oozie-site.xml to set the maximum depth of subworkflow calls.  For example, if set to 3, then a workflow can start subwf1, which
can start subwf2, which can start subwf3; but if subwf3 tries to start subwf4, then the action will fail.  The default is 50.

<a name="JavaAction"></a>
#### 3.2.6 Java Action

The `java` action will execute the `public static void main(String[] args)` method of the specified main Java class.

Java applications are executed in the Hadoop cluster as map-reduce job with a single Mapper task.

The workflow job will wait until the java application completes its execution before continuing to the next action.

The `java` action has to be configured with the resource-manager, name-node, main Java class, JVM options and arguments.

To indicate an `ok` action transition, the main Java class must complete gracefully the `main` method invocation.

To indicate an `error` action transition, the main Java class must throw an exception.

The main Java class can call `System.exit(int n)`. Exit code zero is regarded as OK, while non-zero exit codes will
cause the `java` action to do an `error` transition and exit.

A `java` action can be configured to perform HDFS files/directories cleanup or HCatalog partitions cleanup before
starting the Java application. This capability enables Oozie to retry a Java application in the situation of a transient
or non-transient failure (This can be used to cleanup any temporary data which may have been created by the Java
application in case of failure).

A `java` action can create a Hadoop configuration for interacting with a cluster (e.g. launching a map-reduce job).
Oozie prepares a Hadoop configuration file which includes the environments site configuration files (e.g. hdfs-site.xml,
mapred-site.xml, etc) plus the properties added to the `<configuration>` section of the `java` action. The Hadoop configuration
file is made available as a local file to the Java application in its running directory. It can be added to the `java` actions
Hadoop configuration by referencing the system property: `oozie.action.conf.xml`. For example:


```
// loading action conf prepared by Oozie
Configuration actionConf = new Configuration(false);
actionConf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
```

If `oozie.action.conf.xml` is not added then the job will pick up the mapred-default properties and this may result
in unexpected behaviour. For repeated configuration properties later values override earlier ones.

Inline property values can be parameterized (templatized) using EL expressions.

The YARN `yarn.resourcemanager.address` (`resource-manager`) and HDFS `fs.default.name` (`name-node`) properties must not be present
in the `job-xml` and in the inline configuration.

As with `map-reduce` and `pig` actions, it  is possible to add files and archives to be available to the Java
application. Refer to section [#FilesArchives][Adding Files and Archives for the Job].

The `capture-output` element can be used to propagate values back into Oozie context, which can then be accessed via
EL-functions. This needs to be written out as a java properties format file. The filename is obtained via a System
property specified by the constant `oozie.action.output.properties`

**IMPORTANT:** In order for a Java action to succeed on a secure cluster, it must propagate the Hadoop delegation token like in the
following code snippet (this is benign on non-secure clusters):

```
// propagate delegation related props from launcher job to MR job
if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
    jobConf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
}
```

**IMPORTANT:** Because the Java application is run from within a Map-Reduce job, from Hadoop 0.20. onwards a queue must
be assigned to it. The queue name must be specified as a configuration property.

**IMPORTANT:** The Java application from a Java action is executed in a single map task.  If the task is abnormally terminated,
such as due to a TaskTracker restart (e.g. during cluster maintenance), the task will be retried via the normal Hadoop task
retry mechanism.  To avoid workflow failure, the application should be written in a fashion that is resilient to such retries,
for example by detecting and deleting incomplete outputs or picking back up from complete outputs.  Furthermore, if a Java action
spawns asynchronous activity outside the JVM of the action itself (such as by launching additional MapReduce jobs), the
application must consider the possibility of collisions with activity spawned by the new instance.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <java>
            <resource-manager>[RESOURCE-MANAGER]</resource-manager>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
               <delete path="[PATH]"/>
               ...
               <mkdir path="[PATH]"/>
               ...
            </prepare>
            <job-xml>[JOB-XML]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <main-class>[MAIN-CLASS]</main-class>
			<java-opts>[JAVA-STARTUP-OPTS]</java-opts>
			<arg>ARGUMENT</arg>
            ...
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
            <capture-output />
        </java>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The `prepare` element, if present, indicates a list of paths to delete before starting the Java application. This should
be used exclusively for directory cleanup or dropping of hcatalog table or table partitions for the Java application to be executed.
In case of `delete`, a glob pattern can be used to specify path.
The format for specifying hcatalog table URI is
hcat://[metastore server]:[port]/[database name]/[table name] and format to specify a hcatalog table partition URI is
`hcat://[metastore server]:[port]/[database name]/[table name]/[partkey1]=[value];[partkey2]=[value]`.
In case of a hcatalog URI, the hive-site.xml needs to be shipped using `file` tag and the hcatalog and hive jars
need to be placed in workflow lib directory or specified using `archive` tag.

The `java-opts` and `java-opt` elements, if present, contains the command line parameters which are to be used to start the JVM that
will execute the Java application. Using this element is equivalent to using the `mapred.child.java.opts`
or `mapreduce.map.java.opts` configuration properties, with the advantage that Oozie will append to these properties instead of
simply setting them (e.g. if you have one of these properties specified in mapred-site.xml, setting it again in
the `configuration` element will override it, but using `java-opts` or `java-opt` will instead append to it, preserving the original
value).  You can have either one `java-opts`, multiple `java-opt`, or neither; you cannot use both at the same time.  In any case,
Oozie will set both `mapred.child.java.opts` and `mapreduce.map.java.opts` to the same value based on combining them.  In other
words, after Oozie is finished:

```
mapred.child.java.opts  <-- "<mapred.child.java.opts> <mapreduce.map.java.opts> <java-opt...|java-opts>"
mapreduce.map.java.opts <-- "<mapred.child.java.opts> <mapreduce.map.java.opts> <java-opt...|java-opts>"
```
In the case that parameters are repeated, the latest instance of the parameter is used by Java.  This means that `java-opt`
(or `java-opts`) has the highest priority, followed by `mapreduce.map.java.opts`, and finally `mapred.child.java.opts`.  When
multiple `java-opt` are specified, they are included from top to bottom ordering, where the bottom has highest priority.

The `arg` elements, if present, contains arguments for the main function. The value of each `arg` element is considered
a single argument and they are passed to the `main` method in the same order.

All the above elements can be parameterized (templatized) using EL expressions.

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="myfirstjavajob">
        <java>
            <resource-manager>foo:8032</resource-manager>
            <name-node>bar:8020</name-node>
            <prepare>
                <delete path="${jobOutput}"/>
            </prepare>
            <configuration>
                <property>
                    <name>mapred.queue.name</name>
                    <value>default</value>
                </property>
            </configuration>
            <main-class>org.apache.oozie.MyFirstMainClass</main-class>
            <java-opts>-Dblah</java-opts>
			<arg>argument1</arg>
			<arg>argument2</arg>
        </java>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

##### 3.2.6.1 Overriding an action's Main class

This feature is useful for developers to change the Main classes without having to recompile or redeploy Oozie.

For most actions (not just the Java action), you can override the Main class it uses by specifying the following `configuration`
property and making sure that your class is included in the workflow's classpath.  If you specify this in the Java action,
the main-class element has priority.


```
<property>
   <name>oozie.launcher.action.main.class</name>
   <value>org.my.CustomMain</value>
</property>
```

**Note:** Most actions typically pass information to their corresponding Main in specific ways; you should look at the action's
existing Main to see how it works before creating your own.  In fact, its probably simplest to just subclass the existing Main and
add/modify/overwrite any behavior you want to change.

<a name="WorkflowParameterization"></a>
## 4 Parameterization of Workflows

Workflow definitions can be parameterized.

When workflow node is executed by Oozie all the ELs are resolved into concrete values.

The parameterization of workflow definitions it done using JSP Expression Language syntax from the
[JSP 2.0 Specification (JSP.2.3)](http://jcp.org/aboutJava/communityprocess/final/jsr152/index.html), allowing not only to
support variables as parameters but also functions and complex expressions.

EL expressions can be used in the configuration values of action and decision nodes. They can be used in XML attribute
values and in XML element and attribute values.

They cannot be used in XML element and attribute names. They cannot be used in the name of a node and they cannot be
used within the `transition` elements of a node.

<a name="WorkflowProperties"></a>
### 4.1 Workflow Job Properties (or Parameters)

When a workflow job is submitted to Oozie, the submitter may specify as many workflow job properties as required
(similar to Hadoop JobConf properties).

Workflow applications may define default values for the workflow job or action parameters. They must be defined in a
 `config-default.xml` file bundled with the workflow application archive (refer to section '7 Workflow
 Applications Packaging'). Job or action properties specified in the workflow definition have precedence over the default values.

Properties that are a valid Java identifier, `[A-Za-z_][0-9A-Za-z_]*`, are available as '${NAME}'
variables within the workflow definition.

Properties that are not valid Java Identifier, for example 'job.tracker', are available via the `String
wf:conf(String name)` function. Valid identifier properties are available via this function as well.

Using properties that are valid Java identifiers result in a more readable and compact definition.

By using properties
**Example:**

Parameterized Workflow definition:


```
<workflow-app name='hello-wf' xmlns="uri:oozie:workflow:1.0">
    ...
    <action name='firstjob'>
        <map-reduce>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.mapper.class</name>
                    <value>com.foo.FirstMapper</value>
                </property>
                <property>
                    <name>mapred.reducer.class</name>
                    <value>com.foo.FirstReducer</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>${inputDir}</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>${outputDir}</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to='secondjob'/>
        <error to='killcleanup'/>
    </action>
    ...
</workflow-app>
```

When submitting a workflow job for the workflow definition above, 3 workflow job properties must be specified:

   * `resourceManager:`
   * `inputDir:`
   * `outputDir:`

If the above 3 properties are not specified, the job will fail.

As of schema 0.4, a list of formal parameters can be provided which will allow Oozie to verify, at submission time, that said
properties are actually specified (i.e. before the job is executed and fails). Default values can also be provided.

**Example:**

The previous parameterized workflow definition with formal parameters:


```
<workflow-app name='hello-wf' xmlns="uri:oozie:workflow:1.0">
    <parameters>
        <property>
            <name>inputDir</name>
        </property>
        <property>
            <name>outputDir</name>
            <value>out-dir</value>
        </property>
    </parameters>
    ...
    <action name='firstjob'>
        <map-reduce>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.mapper.class</name>
                    <value>com.foo.FirstMapper</value>
                </property>
                <property>
                    <name>mapred.reducer.class</name>
                    <value>com.foo.FirstReducer</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>${inputDir}</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>${outputDir}</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to='secondjob'/>
        <error to='killcleanup'/>
    </action>
    ...
</workflow-app>
```

In the above example, if `inputDir` is not specified, Oozie will print an error message instead of submitting the job. If
`outputDir` is not specified, Oozie will use the default value, `out-dir`.

<a name="WorkflowELSupport"></a>
### 4.2 Expression Language Functions

Oozie, besides allowing the use of workflow job properties to parameterize workflow jobs, it provides a set of build
in EL functions that enable a more complex parameterization of workflow action nodes as well as the predicates in
decision nodes.

#### 4.2.1 Basic EL Constants

   * **KB:** 1024, one kilobyte.
   * **MB:** 1024 *** KB, one megabyte.
   * **GB:** 1024 *** MB, one gigabyte.
   * **TB:** 1024 *** GB, one terabyte.
   * **PB:** 1024 *** TG, one petabyte.

All the above constants are of type `long`.

#### 4.2.2 Basic EL Functions

**String firstNotNull(String value1, String value2)**

It returns the first not `null` value, or `null` if both are `null`.

Note that if the output of this function is `null` and it is used as string, the EL library converts it to an empty
string. This is the common behavior when using `firstNotNull()` in node configuration sections.

**String concat(String s1, String s2)**

It returns the concatenation of 2 strings. A string with `null` value is considered as an empty string.

**String replaceAll(String src, String regex, String replacement)**

Replace each occurrence of regular expression match in the first string with the `replacement` string and return the replaced string. A 'regex' string with `null` value is considered as no change. A 'replacement' string with `null` value is consider as an empty string.

**String appendAll(String src, String append, String delimeter)**

 Add the `append` string into each splitted sub-strings of the first string(`src`). The split is performed into `src` string using the `delimiter`. E.g. `appendAll("/a/b/,/c/b/,/c/d/", "ADD", ",")` will return `/a/b/ADD,/c/b/ADD,/c/d/ADD`. A `append` string with `null` value is consider as an empty string. A `delimiter` string with value `null` is considered as no append in the string.

**String trim(String s)**

It returns the trimmed value of the given string. A string with `null` value is considered as an empty string.

**String urlEncode(String s)**

It returns the URL UTF-8 encoded value of the given string. A string with `null` value is considered as an empty string.

**String timestamp()**

It returns the current datetime in ISO8601 format, down to minutes (yyyy-MM-ddTHH:mmZ), in the Oozie's processing timezone,
i.e. 1997-07-16T19:20Z

**String toJsonStr(Map<String, String>)** (since Oozie 3.3)

It returns an XML encoded JSON representation of a Map<String, String>. This function is useful to encode as
a single property the complete action-data of an action, **wf:actionData(String actionName)**, in order to pass
it in full to another action.

**String toPropertiesStr(Map<String, String>)** (since Oozie 3.3)

It returns an XML encoded Properties representation of a Map<String, String>. This function is useful to encode as
a single property the complete action-data of an action, **wf:actionData(String actionName)**, in order to pass
it in full to another action.

**String toConfigurationStr(Map<String, String>)** (since Oozie 3.3)

It returns an XML encoded Configuration representation of a Map<String, String>. This function is useful to encode as
a single property the complete action-data of an action, **wf:actionData(String actionName)**, in order to pass
it in full to another action.

#### 4.2.3 Workflow EL Functions

**String wf:id()**

It returns the workflow job ID for the current workflow job.

**String wf:name()**

It returns the workflow application name for the current workflow job.

**String wf:appPath()**

It returns the workflow application path for the current workflow job.

**String wf:conf(String name)**

It returns the value of the workflow job configuration property for the current workflow job, or an empty string if
undefined.

**String wf:user()**

It returns the user name that started the current workflow job.

**String wf:group()**

It returns the group/ACL for the current workflow job.

**String wf:callback(String stateVar)**

It returns the callback URL for the current workflow action node, `stateVar` can be a valid exit state (`OK` or
 `ERROR`) for the action or a token to be replaced with the exit state by the remote system executing the task.

**String wf:transition(String node)**

It returns the transition taken by the specified workflow action node, or an empty string if the action has not being
executed or it has not completed yet.

**String wf:lastErrorNode()**

It returns the name of the last workflow action node that exit with an `ERROR` exit state, or an empty string if no
action has exited with `ERROR` state in the current workflow job.

**String wf:errorCode(String node)**

It returns the error code for the specified action node, or an empty string if the action node has not exited
with `ERROR` state.

Each type of action node must define its complete error code list.

**String wf:errorMessage(String message)**

It returns the error message for the specified action node, or an empty string if no action node has not exited
with `ERROR` state.

The error message can be useful for debugging and notification purposes.

**int wf:run()**

It returns the run number for the current workflow job, normally `0` unless the workflow job is re-run, in which case
indicates the current run.

**Map<String, String> wf:actionData(String node)**

This function is only applicable to action nodes that produce output data on completion.

The output data is in a Java Properties format and via this EL function it is available as a `Map<String, String>`.

**String wf:actionExternalId(String node)**

It returns the external Id for an action node, or an empty string if the action has not being executed or it has not
completed yet.

**String wf:actionTrackerUri(String node)**

It returns the tracker URI for an action node, or an empty string if the action has not being executed or it has not
completed yet.

**String wf:actionExternalStatus(String node)**

It returns the external status for an action node, or an empty string if the action has not being executed or it has
not completed yet.

#### 4.2.4 Hadoop EL Constants

   * **RECORDS:** Hadoop record counters group name.
   * **MAP_IN:** Hadoop mapper input records counter name.
   * **MAP_OUT:** Hadoop mapper output records counter name.
   * **REDUCE_IN:** Hadoop reducer input records counter name.
   * **REDUCE_OUT:** Hadoop reducer input record counter name.
   * **GROUPS:** 1024 *** Hadoop mapper/reducer record groups counter name.

#### 4.2.5 Hadoop EL Functions

<a name="HadoopCountersEL"></a>
**Map < String, Map < String, Long > > hadoop:counters(String node)**

It returns the counters for a job submitted by a Hadoop action node. It returns `0` if the if the Hadoop job has not
started yet and for undefined counters.

The outer Map key is a counter group name. The inner Map value is a Map of counter names and counter values.

The Hadoop EL constants defined in the previous section provide access to the Hadoop built in record counters.

This function can also be used to access specific action statistics information. Examples of action stats and their usage through EL Functions (referenced in workflow xml) are given below.

**Example of MR action stats:**

```
{
    "ACTION_TYPE": "MAP_REDUCE",
    "org.apache.hadoop.mapred.JobInProgress$Counter": {
        "TOTAL_LAUNCHED_REDUCES": 1,
        "TOTAL_LAUNCHED_MAPS": 1,
        "DATA_LOCAL_MAPS": 1
    },
    "FileSystemCounters": {
        "FILE_BYTES_READ": 1746,
        "HDFS_BYTES_READ": 1409,
        "FILE_BYTES_WRITTEN": 3524,
        "HDFS_BYTES_WRITTEN": 1547
    },
    "org.apache.hadoop.mapred.Task$Counter": {
        "REDUCE_INPUT_GROUPS": 33,
        "COMBINE_OUTPUT_RECORDS": 0,
        "MAP_INPUT_RECORDS": 33,
        "REDUCE_SHUFFLE_BYTES": 0,
        "REDUCE_OUTPUT_RECORDS": 33,
        "SPILLED_RECORDS": 66,
        "MAP_OUTPUT_BYTES": 1674,
        "MAP_INPUT_BYTES": 1409,
        "MAP_OUTPUT_RECORDS": 33,
        "COMBINE_INPUT_RECORDS": 0,
        "REDUCE_INPUT_RECORDS": 33
    }
}
```

Below is the workflow that describes how to access specific information using hadoop:counters() EL function from the MR stats.
**Workflow xml:**

```
<workflow-app xmlns="uri:oozie:workflow:1.0" name="map-reduce-wf">
    <start to="mr-node"/>
    <action name="mr-node">
        <map-reduce>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <prepare>
                <delete path="${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}"/>
            </prepare>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
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
                    <name>mapred.map.tasks</name>
                    <value>1</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>/user/${wf:user()}/${examplesRoot}/input-data/text</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>/user/${wf:user()}/${examplesRoot}/output-data/${outputDir}</value>
                </property>
				<property>
					<name>oozie.action.external.stats.write</name>
					<value>true</value>
				</property>
            </configuration>
        </map-reduce>
        <ok to="java1"/>
        <error to="fail"/>
    </action>
    <action name="java1">
        <java>
        <resource-manager>${resourceManager}</resource-manager>
	    <name-node>${nameNode}</name-node>
	    <configuration>
	       <property>
		    <name>mapred.job.queue.name</name>
		    <value>${queueName}</value>
		</property>
	    </configuration>
	    <main-class>MyTest</main-class>
	    <arg>  ${hadoop:counters("mr-node")["FileSystemCounters"]["FILE_BYTES_READ"]}</arg>
        <capture-output/>
        </java>
        <ok to="end" />
        <error to="fail" />
    </action>
    <kill name="fail">
        <message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
```


**Example of Pig action stats:**

```
{
    "ACTION_TYPE": "PIG",
    "JOB_GRAPH": "job_201112191708_0008",
    "PIG_VERSION": "0.9.0",
    "FEATURES": "UNKNOWN",
    "ERROR_MESSAGE": null,
    "NUMBER_JOBS": "1",
    "RECORD_WRITTEN": "33",
    "BYTES_WRITTEN": "1410",
    "HADOOP_VERSION": "0.20.2",
    "SCRIPT_ID": "bbe016e9-f678-43c3-96fc-f24359957582",
    "PROACTIVE_SPILL_COUNT_RECORDS": "0",
    "PROACTIVE_SPILL_COUNT_OBJECTS": "0",
    "RETURN_CODE": "0",
    "ERROR_CODE": "-1",
    "SMM_SPILL_COUNT": "0",
    "DURATION": "36850",
    "job_201112191708_0008": {
        "MAP_INPUT_RECORDS": "33",
        "MIN_REDUCE_TIME": "0",
        "MULTI_STORE_COUNTERS": {},
        "MAX_REDUCE_TIME": "0",
        "NUMBER_REDUCES": "0",
        "ERROR_MESSAGE": null,
        "RECORD_WRITTEN": "33",
        "HDFS_BYTES_WRITTEN": "1410",
        "JOB_ID": "job_201112191708_0008",
        "REDUCE_INPUT_RECORDS": "0",
        "AVG_REDUCE_TIME": "0",
        "MAX_MAP_TIME": "9169",
        "BYTES_WRITTEN": "1410",
        "Alias": "A,B",
        "REDUCE_OUTPUT_RECORDS": "0",
        "SMMS_SPILL_COUNT": "0",
        "PROACTIVE_SPILL_COUNT_RECS": "0",
        "PROACTIVE_SPILL_COUNT_OBJECTS": "0",
        "HADOOP_COUNTERS": null,
        "MIN_MAP_TIME": "9169",
        "MAP_OUTPUT_RECORDS": "33",
        "AVG_MAP_TIME": "9169",
        "FEATURE": "MAP_ONLY",
        "NUMBER_MAPS": "1"
    }
}
```

Below is the workflow that describes how to access specific information using hadoop:counters() EL function from the Pig stats.
**Workflow xml:**

```
<workflow-app xmlns="uri:oozie:workflow:1.0" name="pig-wf">
    <start to="pig-node"/>
    <action name="pig-node">
        <pig>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <prepare>
                <delete path="${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/pig"/>
            </prepare>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property>
                <property>
                    <name>mapred.compress.map.output</name>
                    <value>true</value>
                </property>
				<property>
					<name>oozie.action.external.stats.write</name>
					<value>true</value>
				</property>
            </configuration>
            <script>id.pig</script>
            <param>INPUT=/user/${wf:user()}/${examplesRoot}/input-data/text</param>
            <param>OUTPUT=/user/${wf:user()}/${examplesRoot}/output-data/pig</param>
        </pig>
        <ok to="java1"/>
        <error to="fail"/>
    </action>
    <action name="java1">
        <java>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <configuration>
               <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property>
            </configuration>
            <main-class>MyTest</main-class>
            <arg>  ${hadoop:counters("pig-node")["JOB_GRAPH"]}</arg>
            <capture-output/>
        </java>
        <ok to="end" />
        <error to="fail" />
    </action>
    <kill name="fail">
        <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
```

<a name="Hadoop Configuration EL Function"></a>
**String hadoop:conf(String hadoopConfHostPort, String propName)**

It returns the value of a property of Hadoop configuration.

The hadoopConfHostPort is the 'host:port' of a Hadoop cluster, such as 'NameNodeHostAddress:Port' and
'JobTrackerHostAddress:Port'. The propName is the name of target property. If hadoopConfHostPort could
be connected, Hadoop Conf EL Function will return the value of propName from the specific host address
and port. If hadoopConfHostPort could not be connected, Hadoop Conf EL Function will generate a default
hadoop configuration object directly, and then return the value of target property.

**Example of usage of Hadoop Configuration EL Function:**


```
<action name="mr-node">
    <map-reduce>
        <resource-manager>${resourceManager}</resource-manager>
        <name-node>${nameNode}</name-node>
        <configuration>
            <property>
                <name>mapreduce.map.java.opts</name>
                <value>${hadoop:conf("9.181.7.69:9001","mapreduce.map.java.opts")} -Xss512k </value>
            </property>
        </configuration>
    </map-reduce>
    <ok to="end"/>
    <error to="fail"/>
</action>
```

#### 4.2.6 Hadoop Jobs EL Function

The function _wf:actionData()_ can be used to access Hadoop ID's for actions such as Pig, by specifying the key as _hadoopJobs_.
An example is shown below.


```
<workflow-app xmlns="uri:oozie:workflow:1.0" name="pig-wf">
    <start to="pig-node"/>
    <action name="pig-node">
        <pig>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <prepare>
                <delete path="${nameNode}/user/${wf:user()}/${examplesRoot}/output-data/pig"/>
            </prepare>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property>
                <property>
                    <name>mapred.compress.map.output</name>
                    <value>true</value>
                </property>
            </configuration>
            <script>id.pig</script>
            <param>INPUT=/user/${wf:user()}/${examplesRoot}/input-data/text</param>
            <param>OUTPUT=/user/${wf:user()}/${examplesRoot}/output-data/pig</param>
        </pig>
        <ok to="java1"/>
        <error to="fail"/>
    </action>
    <action name="java1">
        <java>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <configuration>
               <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property>
            </configuration>
            <main-class>MyTest</main-class>
            <arg> ${wf:actionData("pig-node")["hadoopJobs"]}</arg>
            <capture-output/>
        </java>
        <ok to="end" />
        <error to="fail" />
    </action>
    <kill name="fail">
        <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
```

#### 4.2.7 HDFS EL Functions

For all the functions in this section the path must include the FS URI. For example `hdfs://foo:8020/user/tucu`.

**boolean fs:exists(String path)**

It returns `true` or `false` depending if the specified path URI exists or not. If a glob pattern is used for the URI, it returns true when there is at least one matching path.

**boolean fs:isDir(String path)**

It returns `true` if the specified path URI exists and it is a directory, otherwise it returns `false`.

**long fs:dirSize(String path)**

It returns the size in bytes of all the files in the specified path. If the path is not a directory, or if it does not
exist it returns -1. It does not work recursively, only computes the size of the files under the specified path.

**long fs:fileSize(String path)**

It returns the size in bytes of specified file. If the path is not a file, or if it does not exist it returns -1.

**long fs:blockSize(String path)**

It returns the block size in bytes of specified file. If the path is not a file, or if it does not exist it returns -1.

#### 4.2.8 HCatalog EL Functions

For all the functions in this section the URI must be a hcatalog URI identifying a table or set of partitions in a table.
The format for specifying hcatalog table URI is
hcat://[metastore server]:[port]/[database name]/[table name] and format to specify a hcatalog table partition URI is
`hcat://[metastore server]:[port]/[database name]/[table name]/[partkey1]=[value];[partkey2]=[value]`. For example:

<pre>
hcat://foo:8020/mydb/mytable
hcat://foo:8020/mydb/mytable/region=us;dt=20121212
</pre>

**boolean hcat:exists(String uri)**

It returns `true` or `false` based on if the partitions in the table exists or not.

<a name="WorkflowNotifications"></a>
## 5 Workflow Notifications

Workflow jobs can be configured to make an HTTP GET notification upon start and end of a workflow action node and upon
the completion of a workflow job.

Oozie will make a best effort to deliver the notifications, in case of failure it will retry the notification a
pre-configured number of times at a pre-configured interval before giving up.

See also [Coordinator Notifications](CoordinatorFunctionalSpec.html#CoordinatorNotifications)

### 5.1 Workflow Job Status Notification

If the `oozie.wf.workflow.notification.url` property is present in the workflow job properties when submitting the job,
Oozie will make a notification to the provided URL when the workflow job changes its status.
`oozie.wf.workflow.notification.proxy` property can be used to configure either a http or socks proxy.
The format is proxyHostname:port or proxyType@proxyHostname:port. If proxy type is not specified, it defaults to http.
For eg: myhttpproxyhost.mydomain.com:80 or `socks@mysockshost.mydomain.com:1080`.


If the URL contains any of the following tokens, they will be replaced with the actual values by Oozie before making
the notification:

   * `$jobId` : The workflow job ID
   * `$status` : the workflow current state
   * `$parentId` : The parent ID of the workflow job. If there is no parent ID, it will be replaced with an empty string.

### 5.2 Node Start and End Notifications

If the `oozie.wf.action.notification.url` property is present in the workflow job properties when submitting the job,
Oozie will make a notification to the provided URL every time the workflow job enters and exits an action node. For
decision nodes, Oozie will send a single notification with the name of the first evaluation that resolved to `true`.
`oozie.wf.workflow.notification.proxy` property can be used to configure proxy, it should contain proxy host and port (xyz:4080).

If the URL contains any of the following tokens, they will be replaced with the actual values by Oozie before making
the notification:

   * `$jobId` : The workflow job ID
   * `$nodeName` : The name of the workflow node
   * `$status` : If the action has not completed yet, it contains the action status `S:<STATUS>`. If the action has ended, it contains the action transition `T:<TRANSITION>`

<a name="UserPropagation"></a>
## 6 User Propagation

When submitting a workflow job, the configuration must contain a `user.name` property. If security is enabled, Oozie
must ensure that the value of the `user.name` property in the configuration match the user credentials present in the
protocol (web services) request.

When submitting a workflow job, the configuration may contain the `oozie.job.acl` property (the `group.name` property
has been deprecated). If authorization is enabled, this property is treated as as the ACL for the job, it can contain
user and group IDs separated by commas.

The specified user and ACL are assigned to the created job.

Oozie must propagate the specified user and ACL to the system executing the actions.

It is not allowed for map-reduce, pig and fs actions to override user/group information.

<a name="AppDeployment"></a>
## 7 Workflow Application Deployment

While Oozie encourages the use of self-contained applications (J2EE application model), it does not enforce it.

Workflow applications are installed in an HDFS directory. To submit a job for a workflow application the path to the
HDFS directory where the application is must be specified.

The layout of a workflow application directory is:


```
    - /workflow.xml
    - /config-default.xml
    |
    - /lib/ (**.jar;**.so)
```

A workflow application must contain at least the workflow definition, the `workflow.xml` file.

All configuration files and scripts (Pig and shell) needed by the workflow action nodes should be under the application
HDFS directory.

All the JAR files and native libraries within the application 'lib/' directory are automatically added to the
map-reduce and pig jobs `classpath` and `LD_PATH`.

Additional JAR files and native libraries not present in the application 'lib/' directory can be specified in
map-reduce and pig actions with the 'file' element (refer to the map-reduce and pig documentation).

For Map-Reduce jobs (not including streaming or pipes), additional jar files can also be included via an uber jar. An uber jar is a
jar file that contains additional jar files within a "lib" folder. To let Oozie know about an uber jar, simply specify it with
the `oozie.mapreduce.uber.jar` configuration property and Oozie will tell Hadoop MapReduce that it is an uber jar. The ability to
specify an uber jar is governed by the `oozie.action.mapreduce.uber.jar.enable` property in `oozie-site.xml`. See
[Oozie Install](AG_Install.html#UberJar) for more information.


```
<action name="mr-node">
    <map-reduce>
        <resource-manager>${resourceManager}</resource-manager>
        <name-node>${nameNode}</name-node>
        <configuration>
            <property>
                <name>oozie.mapreduce.uber.jar</name>
                <value>${nameNode}/user/${wf:user()}/my-uber-jar.jar</value>
            </property>
        </configuration>
    </map-reduce>
    <ok to="end"/>
    <error to="fail"/>
</action>
```

The `config-default.xml` file defines, if any, default values for the workflow job or action parameters. This file must be in
the Hadoop Configuration XML format. EL expressions are not supported and `user.name` property cannot be specified in
this file.

Any other resources like `job.xml` files referenced from a workflow action node must be included under the
corresponding path, relative paths always start from the root of the workflow application.

## 8 External Data Assumptions

Oozie runs workflow jobs under the assumption all necessary data to execute an action is readily available at the
time the workflow job is about to executed the action.

In addition, it is assumed, but it is not the responsibility of Oozie, that all input data used by a workflow job is
immutable for the duration of the workflow job.

<a name="JobLifecycle"></a>
## 9 Workflow Jobs Lifecycle

### 9.1 Workflow Job Lifecycle
A workflow job can be in any of the following states:

 `PREP:` When a workflow job is first created it will be in `PREP` state. The workflow job is defined but it is not
running.

 `RUNNING:` When a `CREATED` workflow job is started it goes into `RUNNING` state, it will remain in `RUNNING` state
until it reaches its end state, ends in error or is suspended.

 `SUSPENDED:` A `RUNNING` workflow job can be suspended, it will remain in `SUSPENDED` state until the workflow job
 is resumed or it is killed.

 `SUCCEEDED:` When a `RUNNING` workflow job reaches the `end` node it ends reaching the `SUCCEEDED` final state.

 `KILLED:` When a `CREATED`, `RUNNING` or `SUSPENDED` workflow job is killed by an administrator or the owner via a
request to Oozie the workflow job ends reaching the `KILLED` final state.

 `FAILED:` When a `RUNNING` workflow job fails due to an unexpected error it ends reaching the `FAILED` final state.

**Workflow job state valid transitions:**

   * --> `PREP`
   * `PREP` --> `RUNNING` | `KILLED`
   * `RUNNING` --> `SUSPENDED` | `SUCCEEDED` | `KILLED` | `FAILED`
   * `SUSPENDED` --> `RUNNING` | `KILLED`

### 9.2 Workflow Action Lifecycle

When a workflow action is created, it is in the `PREP` state. If an attempt to start it succeeds,
it transitions to the `RUNNING` state; if the attempt fails in a way that Oozie deems to be transient, and a non-zero
number of retries is configured, it enters the `START_RETRY` state and Oozie automatically retries the action until
it either succeeds or the configured number of retries is reached. If the error is not transient or still persists
after the retries, the job transitions to the `START_MANUAL` state, where the user is expected to either kill the
action or manually resume it (after fixing any issues).

From the `RUNNING` state, the action normally transitions to the `DONE` state. From `DONE`, it goes to `OK` if it ends
successfully, otherwise to `ERROR` or `USER_RETRY`.

If an error is encountered while Oozie is trying to end the action, the action transitions to the `END_RETRY` state if
the error is transient and a non-zero number of retries is configured, or to the `END_MANUAL` state if it is not.
In the `END_RETRY` state, Oozie automatically retries ending the action until it either succeeds or the configured
number of retries is reached. If the error persists, the action goes to the `END_MANUAL` state, where the user is
expected to either kill the action or manually resume it (after fixing any issues).

The `USER_RETRY` state is used when retrying actions where the user has explicitly configured the number (and/or other
properties) of retries. For more information, see
[User-Retry for Workflow Actions](WorkflowFunctionalSpec.html#UserRetryWFActions).
From `USER_RETRY`, the action goes back to `RUNNING` and a retry is attempted. After the configured number of user
retries, if the action is still failing, it goes to the `ERROR` state.

If an action is killed, it transitions to the `KILLED` state. If there is an error while attempting to kill the action,
it goes to the `FAILED` state.

**Workflow action state valid transitions:**

   * --> `PREP`
   * `PREP` --> `START_RETRY` | `START_MANUAL` | `RUNNING` | `KILLED`
   * `START_RETRY` --> `START_MANUAL` | `RUNNING` | `KILLED`
   * `START_MANUAL` --> `RUNNING` | `KILLED`
   * `USER_RETRY` --> `RUNNING` | `DONE` | `KILLED`
   * `RUNNING` --> `DONE` | `KILLED`
   * `KILLED` --> `FAILED`
   * `DONE` --> `OK` | `ERROR` | `USER_RETRY` | `END_RETRY` | `END_MANUAL`
   * `END_RETRY` --> `END_MANUAL` | `KILLED` | `OK` | `ERROR`
   * `END_MANUAL` --> `KILLED` | `OK` | `ERROR`


<a name="JobReRun"></a>
## 10 Workflow Jobs Recovery (re-run)

Oozie must provide a mechanism by which a failed workflow job can be resubmitted and executed starting after any
action node that has completed its execution in the prior run. This is specially useful when the already executed
action of a workflow job are too expensive to be re-executed.

It is the responsibility of the user resubmitting the workflow job to do any necessary cleanup to ensure that the
rerun will not fail due to not cleaned up data from the previous run.

When starting a workflow job in recovery mode, the user must indicate either what workflow nodes in the workflow should be
skipped or whether job should be restarted from the failed node. At any rerun, only one option should be selected. The workflow nodes to skip must be specified in the `oozie.wf.rerun.skip.nodes` job configuration property,
node names must be separated by commas. On the other hand, user needs to specify `oozie.wf.rerun.failnodes` to rerun from the failed node. The value is `true` or `false`. All workflow nodes indicated as skipped must have completed in the previous
run. If a workflow node has not completed its execution in its previous run, and during the recovery submission is
flagged as a node to be skipped, the recovery submission must fail.

The recovery workflow job will run under the same workflow job ID as the original workflow job.

To submit a recovery workflow job the target workflow job to recover must be in an end state (`SUCCEEDED`, `FAILED`
or `KILLED`).

A recovery run could be done using a new workflow application path under certain constraints (see next paragraph).
This is to allow users to do a one off patch for the workflow application without affecting other running jobs for the
same application.

A recovery run could be done using different workflow job parameters, the new values of the parameters will take effect
only for the workflow nodes executed in the rerun.

The workflow application use for a re-run must match the execution flow, node types, node names and node configuration
for all executed nodes that will be skipped during recovery. This cannot be checked by Oozie, it is the responsibility
of the user to ensure this is the case.

Oozie provides the `int wf:run()` EL function to returns the current run for a job, this function allows workflow
applications to perform custom logic at workflow definition level (i.e. in a `decision` node) or at action node level
(i.e. by passing the value of the `wf:run()` function as a parameter to the task).

<a name="OozieWSAPI"></a>
## 11 Oozie Web Services API

See the [Web Services API](WebServicesAPI.html) page.

## 12 Client API

Oozie provides a Java [Client API](./client/apidocs/org/apache/oozie/client/package-summary.html) that allows to
perform all common workflow job operations.

The client API includes a [LocalOozie class](./core/apidocs/org/apache/oozie/local/LocalOozie.html) useful for testing
a workflow from within an IDE and for unit testing purposes.

The Client API is implemented as a client of the Web Services API.

## 13 Command Line Tools

Oozie provides command line tool that allows to perform all common workflow job operations.

The command line tool is implemented as a client of the Web Services API.

## 14 Web UI Console

Oozie provides a read-only Web based console that allows to allow to monitor Oozie system status, workflow
applications status and workflow jobs status.

The Web base console is implemented as a client of the Web Services API.

## 15 Customizing Oozie with Extensions

Out of the box Oozie provides support for a predefined set of action node types and Expression Language functions.

Oozie provides a well defined API, [Action executor](./core/apidocs/org/apache/oozie/action/package-summary.html)
API, to add support for additional action node types.

Extending Oozie should not require any code change to the Oozie codebase. It will require adding the JAR files
providing the new functionality and declaring them in Oozie system configuration.

## 16 Workflow Jobs Priority

Oozie does not handle workflow jobs priority. As soon as a workflow job is ready to do a transition, Oozie will
trigger the transition. Workflow transitions and action triggering are assumed to be fast and lightweight operations.

Oozie assumes that the remote systems are properly sized to handle the amount of remote jobs Oozie will trigger.

Any prioritization of jobs in the remote systems is outside of the scope of Oozie.

Workflow applications can influence the remote systems priority via configuration if the remote systems support it.

<a name="ShareLib"></a>
## 17 HDFS Share Libraries for Workflow Applications (since Oozie 2.3)

Oozie supports job and system share libraries for workflow jobs.

Share libraries can simplify the deployment and management of common components across workflow applications.

For example, if a workflow job uses a share library with the Streaming, Pig & Har JARs files it does not have to
bundled those JARs files in the workflow application `lib/` path.

If workflow job uses a share library, Oozie will include all the JAR/SO files in the library in the
classpath/libpath for all its actions.

A workflow job can specify a share library path using the job property `oozie.libpath`.

A workflow job can use the system share library by setting the job property `oozie.use.system.libpath` to `true`.
`oozie.use.system.libpath` can be also configured at action configuration.
`oozie.use.system.libpath` defined at action level overrides job property.

### 17.1 Action Share Library Override (since Oozie 3.3)

Oozie share libraries are organized per action type, for example Pig action share library directory is `share/lib/pig/`
and Mapreduce Streaming share library directory is `share/library/mapreduce-streaming/`.

Oozie bundles a share library for specific versions of streaming, pig, hive, sqoop, distcp actions. These versions
of streaming, pig, hive, sqoop and distcp have been tested and verified to work correctly with the version of Oozie
that includes them. Oozie also bundles a separate share library for hcatalog, which can be used with pig, hive and java
actions (since Oozie 4.x).

In addition, Oozie provides a mechanism to override the action share library JARs to allow using an alternate version
of of the action JARs.

This mechanism enables Oozie administrators to patch share library JARs, to include alternate versions of the share
libraries, to provide access to more than one version at the same time.

The share library override is supported at server level and at job level. The share library directory names are resolved
using the following precedence order:

   *  oozie.action.sharelib.for.#ACTIONTYPE# in the action configuration
   *  oozie.action.sharelib.for.#ACTIONTYPE# in the job configuration
   *  oozie.action.sharelib.for.#ACTIONTYPE# in the oozie server configuration
   *  The action or custom action's `ActionExecutor getDefaultShareLibName()` method

More than one share library directory name can be specified for an action by using a comma separated list (since Oozie 4.x).
For example: When using HCatLoader and HCatStorer in pig, `oozie.action.sharelib.for.pig` can be set to `pig,hcatalog` to include
both pig and hcatalog jars.

<a name="UserRetryWFActions"></a>
## 18 User-Retry for Workflow Actions (since Oozie 3.1)

Oozie provides User-Retry capabilities when an action is in `ERROR` or `FAILED` state.

Depending on the nature of the failure, Oozie can define what type of errors allowed for User-Retry. There are certain errors
Oozie is allowing for user-retry in default, for example, file-exists-error `FS009, FS008` when using chmod in workflow `fs`
action, output-directory-exists-error `JA018` in workflow `map-reduce` action, job-not-exists-error `JA017` in action executor,
FileNotFoundException `JA008` in action executor, and IOException `JA009` in action executor.

User-Retry allows user to give certain number of reties (must not exceed system max retries), so user can find the causes of
that problem and fix them when action is in `USER_RETRY` state. If failure or error does not go away after max retries,
the action becomes `FAILED` or `ERROR` and Oozie marks workflow job to `FAILED` or `KILLED`.

Oozie administrator can allow more error codes to be handled for User-Retry. By adding this configuration
`oozie.service.LiteWorkflowStoreService.user.retry.error.code.ext` to `oozie.site.xml`
and error codes as value, these error codes will be considered as User-Retry after system restart.

Since Oozie 4.3, User-retry allows user to mention retry policy. The value for policy can be `periodic`
or `exponential`, `periodic` being the default. Oozie administrator can define user retry policy for all workflow
actions by adding this configuration `oozie.service.LiteWorkflowStoreService.user.retry.policy` to `oozie.site.xml`.
This value will be considered as user retry policy after system restart. This value can be overridden while defining
actions in workflow xml if needed. The `retry-interval` should be specified in minutes.

Examples of User-Retry in a workflow action is :


```
<workflow-app xmlns="uri:oozie:workflow:1.0" name="wf-name">
<action name="a" retry-max="2" retry-interval="1" retry-policy="exponential">
</action>
```

<a name="GlobalConfigurations"></a>
## 19 Global Configurations

Oozie allows a global section to reduce the redundant resource-manager and name-node declarations for each action. The user can
define a `global` section in the beginning of the `workflow.xml`. The global section may contain the `job-xml`, `configuration`,
`resource-manager`, or `name-node` that the user would like to set for every action. If a user then redefines one of these in a
specific action node, Oozie will update use the specific declaration instead of the global one for that action.

Example of a global element:


```
<workflow-app xmlns="uri:oozie:workflow:1.0" name="wf-name">
<global>
   <resource-manager>${resourceManager}</resource-manager>
   <name-node>${namd-node}</name-node>
   <job-xml>job1.xml</job-xml>
   <configuration>
        <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
        </property>
    </configuration>
</global>
...
```

## 20 Suspend On Nodes

Specifying `oozie.suspend.on.nodes` in a job.properties file lets users specify a list of actions that will cause Oozie to
automatically suspend the workflow upon reaching any of those actions; like breakpoints in a debugger. To continue running the
workflow, the user simply uses the
[-resume command from the Oozie CLI](DG_CommandLineTool.html#Resuming_a_Workflow_Coordinator_or_Bundle_Job). Specifying a * will
cause Oozie to suspend the workflow on all nodes.

For example:

```
oozie.suspend.on.nodes=mr-node,my-pig-action,my-fork
```
Specifying the above in a job.properties file will cause Oozie to suspend the workflow when any of those three actions are about
to be executed.


## Appendixes

<a name="OozieWFSchema"></a>
### Appendix A, Oozie Workflow and Common XML Schemas

#### Oozie Workflow Schema Version 1.0

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:1.0"
           elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:1.0">
.
    <xs:include schemaLocation="oozie-common-1.0.xsd"/>
.
    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>
.
    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}"/>
        </xs:restriction>
    </xs:simpleType>
.
    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
            <xs:element name="parameters" type="workflow:PARAMETERS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="global" type="workflow:GLOBAL" minOccurs="0" maxOccurs="1"/>
            <xs:element name="credentials" type="workflow:CREDENTIALS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1 uri:oozie:sla:0.2" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="PARAMETERS">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="0" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="GLOBAL">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="launcher" type="workflow:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>
.
    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:element name="kill" type="workflow:KILL"/>
.
    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
.
    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1 uri:oozie:sla:0.2" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="cred" type="xs:string"/>
        <xs:attribute name="retry-max" type="xs:string"/>
        <xs:attribute name="retry-interval" type="xs:string"/>
        <xs:attribute name="retry-policy" type="xs:string"/>
    </xs:complexType>
.
    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="launcher" type="workflow:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="config-class" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="launcher" type="workflow:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="delete" type="workflow:DELETE"/>
                <xs:element name="mkdir" type="workflow:MKDIR"/>
                <xs:element name="move" type="workflow:MOVE"/>
                <xs:element name="chmod" type="workflow:CHMOD"/>
                <xs:element name="touchz" type="workflow:TOUCHZ"/>
                <xs:element name="chgrp" type="workflow:CHGRP"/>
            </xs:choice>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="launcher" type="workflow:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="java-opts" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java-opt" type="xs:string" minOccurs="1" maxOccurs="unbounded"/>
            </xs:choice>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="CREDENTIALS">
        <xs:sequence minOccurs="0" maxOccurs="unbounded">
            <xs:element name="credential" type="workflow:CREDENTIAL"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="CREDENTIAL">
        <xs:sequence  minOccurs="0" maxOccurs="unbounded" >
                 <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                    <xs:complexType>
                       <xs:sequence>
                            <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                            <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                            <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                       </xs:sequence>
                    </xs:complexType>
                 </xs:element>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
.
</xs:schema>
```

#### Oozie Common Schema Version 1.0

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
.
    <xs:complexType name="LAUNCHER">
        <xs:choice maxOccurs="unbounded">
            <!-- Oozie Launcher job memory in MB -->
            <xs:element name="memory.mb" minOccurs="0" type="xs:unsignedInt"/>
            <xs:element name="vcores" minOccurs="0" type="xs:unsignedInt"/>
            <xs:element name="java-opts" minOccurs="0" type="xs:string"/>
            <xs:element name="env" minOccurs="0" type="xs:string"/>
            <xs:element name="queue" minOccurs="0" type="xs:string"/>
            <xs:element name="sharelib" minOccurs="0" type="xs:string"/>
            <xs:element name="view-acl" minOccurs="0" type="xs:string"/>
            <xs:element name="modify-acl" minOccurs="0" type="xs:string"/>
        </xs:choice>
    </xs:complexType>
.
    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="FLAG"/>
.
    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="CHMOD">
        <xs:sequence>
            <xs:element name="recursive" type="FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="permissions" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>
.
    <xs:complexType name="TOUCHZ">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>
.
    <xs:complexType name="CHGRP">
        <xs:sequence>
            <xs:element name="recursive" type="FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="group" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>
.
</xs:schema>
```

#### Oozie Workflow Schema Version 0.5

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.5"
           elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.5">

    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>

    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
            <xs:element name="parameters" type="workflow:PARAMETERS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="global" type="workflow:GLOBAL" minOccurs="0" maxOccurs="1"/>
            <xs:element name="credentials" type="workflow:CREDENTIALS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1 uri:oozie:sla:0.2" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="PARAMETERS">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="0" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="GLOBAL">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="kill" type="workflow:KILL"/>

    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1 uri:oozie:sla:0.2" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="cred" type="xs:string"/>
        <xs:attribute name="retry-max" type="xs:string"/>
        <xs:attribute name="retry-interval" type="xs:string"/>
        <xs:attribute name="retry-policy" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="config-class" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="delete" type="workflow:DELETE"/>
                <xs:element name="mkdir" type="workflow:MKDIR"/>
                <xs:element name="move" type="workflow:MOVE"/>
                <xs:element name="chmod" type="workflow:CHMOD"/>
                <xs:element name="touchz" type="workflow:TOUCHZ"/>
                <xs:element name="chgrp" type="workflow:CHGRP"/>
            </xs:choice>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="java-opts" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java-opt" type="xs:string" minOccurs="1" maxOccurs="unbounded"/>
            </xs:choice>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FLAG"/>

    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="skip-trash" type="xs:boolean" use="optional"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHMOD">
        <xs:sequence>
            <xs:element name="recursive" type="workflow:FLAG" minOccurs="0" maxOccurs="1"></xs:element>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="permissions" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="TOUCHZ">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHGRP">
        <xs:sequence>
            <xs:element name="recursive" type="workflow:FLAG" minOccurs="0" maxOccurs="1"></xs:element>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="group" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="CREDENTIALS">
        <xs:sequence minOccurs="0" maxOccurs="unbounded">
            <xs:element name="credential" type="workflow:CREDENTIAL"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CREDENTIAL">
        <xs:sequence  minOccurs="0" maxOccurs="unbounded" >
                 <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                    <xs:complexType>
                       <xs:sequence>
                            <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                            <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                            <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                       </xs:sequence>
                    </xs:complexType>
                 </xs:element>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
</xs:schema>
```

#### Oozie Workflow Schema Version 0.4.5

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.4.5"
           elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.4.5">

    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>

    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
            <xs:element name="parameters" type="workflow:PARAMETERS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="global" type="workflow:GLOBAL" minOccurs="0" maxOccurs="1"/>
            <xs:element name="credentials" type="workflow:CREDENTIALS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="PARAMETERS">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="0" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="GLOBAL">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="kill" type="workflow:KILL"/>

    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="cred" type="xs:string"/>
        <xs:attribute name="retry-max" type="xs:string"/>
        <xs:attribute name="retry-interval" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="delete" type="workflow:DELETE"/>
                <xs:element name="mkdir" type="workflow:MKDIR"/>
                <xs:element name="move" type="workflow:MOVE"/>
                <xs:element name="chmod" type="workflow:CHMOD"/>
                <xs:element name="touchz" type="workflow:TOUCHZ"/>
                <xs:element name="chgrp" type="workflow:CHGRP"/>
            </xs:choice>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="java-opts" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java-opt" type="xs:string" minOccurs="1" maxOccurs="unbounded"/>
            </xs:choice>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FLAG"/>

    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHMOD">
        <xs:sequence>
            <xs:element name="recursive" type="workflow:FLAG" minOccurs="0" maxOccurs="1"></xs:element>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="permissions" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="TOUCHZ">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHGRP">
        <xs:sequence>
            <xs:element name="recursive" type="workflow:FLAG" minOccurs="0" maxOccurs="1"></xs:element>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="group" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="CREDENTIALS">
        <xs:sequence minOccurs="0" maxOccurs="unbounded">
            <xs:element name="credential" type="workflow:CREDENTIAL"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CREDENTIAL">
        <xs:sequence  minOccurs="0" maxOccurs="unbounded" >
                 <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                    <xs:complexType>
                       <xs:sequence>
                            <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                            <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                            <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                       </xs:sequence>
                    </xs:complexType>
                 </xs:element>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
</xs:schema>
```

#### Oozie Workflow Schema Version 0.4

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.4"
           elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.4">

    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>

    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
            <xs:element name="parameters" type="workflow:PARAMETERS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="global" type="workflow:GLOBAL" minOccurs="0" maxOccurs="1"/>
            <xs:element name="credentials" type="workflow:CREDENTIALS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="PARAMETERS">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="0" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="GLOBAL">
    	<xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="kill" type="workflow:KILL"/>

    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="cred" type="xs:string"/>
        <xs:attribute name="retry-max" type="xs:string"/>
        <xs:attribute name="retry-interval" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FS">
	<xs:sequence>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="delete" type="workflow:DELETE"/>
                <xs:element name="mkdir" type="workflow:MKDIR"/>
                <xs:element name="move" type="workflow:MOVE"/>
                <xs:element name="chmod" type="workflow:CHMOD"/>
                <xs:element name="touchz" type="workflow:TOUCHZ"/>
            </xs:choice>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FLAG"/>

    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHMOD">
        <xs:sequence>
            <xs:element name="recursive" type="workflow:FLAG" minOccurs="0" maxOccurs="1"></xs:element>
        </xs:sequence>
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="permissions" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="CREDENTIALS">
        <xs:sequence minOccurs="0" maxOccurs="unbounded">
            <xs:element name="credential" type="workflow:CREDENTIAL"/>
		</xs:sequence>
    </xs:complexType>

   	<xs:complexType name="CREDENTIAL">
        <xs:sequence  minOccurs="0" maxOccurs="unbounded" >
                   <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                	<xs:complexType>
	                    <xs:sequence>
	                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
	                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
	                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
	                    </xs:sequence>
                	</xs:complexType>
           		</xs:element>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
</xs:schema>
```

#### Oozie Workflow Schema Version 0.3

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.3"
           elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.3">

    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>

    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39})"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
        	<xs:element name="credentials" type="workflow:CREDENTIALS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="kill" type="workflow:KILL"/>

    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="cred" type="xs:string"/>
        <xs:attribute name="retry-max" type="xs:string"/>
        <xs:attribute name="retry-interval" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="move" type="workflow:MOVE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="chmod" type="workflow:CHMOD" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FLAG"/>

    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHMOD">
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="permissions" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="CREDENTIALS">
        <xs:sequence minOccurs="0" maxOccurs="unbounded">
            <xs:element name="credential" type="workflow:CREDENTIAL"/>
		</xs:sequence>
    </xs:complexType>

   	<xs:complexType name="CREDENTIAL">
        <xs:sequence  minOccurs="0" maxOccurs="unbounded" >
                   <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                	<xs:complexType>
	                    <xs:sequence>
	                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
	                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
	                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
	                    </xs:sequence>
                	</xs:complexType>
           		</xs:element>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>


</xs:schema>
```

#### Oozie Workflow Schema Version 0.2.5

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.2.5"
           elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.2.5">

    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>

    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39})"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
        	<xs:element name="credentials" type="workflow:CREDENTIALS" minOccurs="0" maxOccurs="1"/>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="kill" type="workflow:KILL"/>

    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="cred" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="move" type="workflow:MOVE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="chmod" type="workflow:CHMOD" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FLAG"/>

    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHMOD">
        <xs:attribute name="path" type="xs:string" use="required"/>
        <xs:attribute name="permissions" type="xs:string" use="required"/>
        <xs:attribute name="dir-files" type="xs:string"/>
    </xs:complexType>

    <xs:complexType name="CREDENTIALS">
        <xs:sequence minOccurs="0" maxOccurs="unbounded">
            <xs:element name="credential" type="workflow:CREDENTIAL"/>
		</xs:sequence>
    </xs:complexType>

   	<xs:complexType name="CREDENTIAL">
        <xs:sequence  minOccurs="0" maxOccurs="unbounded" >
                   <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                	<xs:complexType>
	                    <xs:sequence>
	                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
	                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
	                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
	                    </xs:sequence>
                	</xs:complexType>
           		</xs:element>
        </xs:sequence>
        <xs:attribute name="name" type="xs:string" use="required"/>
        <xs:attribute name="type" type="xs:string" use="required"/>
    </xs:complexType>
</xs:schema>
```

#### Oozie Workflow Schema Version 0.2

```
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.2"
    elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.2">

    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>

    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39})"/>
        </xs:restriction>
    </xs:simpleType>

    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="switch" type="workflow:SWITCH"/>

    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>

    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="kill" type="workflow:KILL"/>

    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:any namespace="uri:oozie:sla:0.1" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>

    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="move" type="workflow:MOVE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="chmod" type="workflow:CHMOD" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="FLAG"/>

    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="CHMOD">
        <xs:attribute name="path" type="xs:string" use="required" />
        <xs:attribute name="permissions" type="xs:string" use="required" />
        <xs:attribute name="dir-files" type="xs:string" />
    </xs:complexType>

</xs:schema>
```

<a name="SLASchema"></a>
#### Oozie SLA Version 0.2
   * Supported in Oozie workflow schema version 0.5


```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:sla="uri:oozie:sla:0.2" elementFormDefault="qualified"
           targetNamespace="uri:oozie:sla:0.2">

    <xs:element name="info" type="sla:SLA-INFO"/>

    <xs:complexType name="SLA-INFO">
        <xs:sequence>
            <xs:element name="nominal-time" type="xs:string" minOccurs="1"
                        maxOccurs="1"/>
            <xs:element name="should-start" type="xs:string" minOccurs="0"
                        maxOccurs="1"/>
            <xs:element name="should-end" type="xs:string" minOccurs="1"
                        maxOccurs="1"/>
            <xs:element name="max-duration" type="xs:string" minOccurs="0"
                        maxOccurs="1"/>

            <xs:element name="alert-events" type="xs:string" minOccurs="0"
                        maxOccurs="1"/>
            <xs:element name="alert-contact" type="xs:string" minOccurs="0"
                        maxOccurs="1"/>
            <xs:element name="notification-msg" type="xs:string" minOccurs="0"
                        maxOccurs="1"/>
            <xs:element name="upstream-apps" type="xs:string" minOccurs="0"
                        maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>

</xs:schema>
```


#### Oozie SLA Version 0.1
   * Oozie SLA schema is supported in Oozie workflow schema version 0.2 onwards


```
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
	xmlns:sla="uri:oozie:sla:0.1" elementFormDefault="qualified"
	targetNamespace="uri:oozie:sla:0.1">

	<xs:element name="info" type="sla:SLA-INFO" />

	<xs:complexType name="SLA-INFO">
		<xs:sequence>
			<xs:element name="app-name" type="xs:string" minOccurs="1"
				maxOccurs="1" />

			<xs:element name="nominal-time" type="xs:string"
				minOccurs="1" maxOccurs="1" />
			<xs:element name="should-start" type="xs:string"
				minOccurs="1" maxOccurs="1" />
			<xs:element name="should-end" type="xs:string" minOccurs="1"
				maxOccurs="1" />

			<xs:element name="parent-client-id" type="xs:string"
				minOccurs="0" maxOccurs="1" />
			<xs:element name="parent-sla-id" type="xs:string"
				minOccurs="0" maxOccurs="1" />

			<xs:element name="notification-msg" type="xs:string"
				minOccurs="0" maxOccurs="1" />
			<xs:element name="alert-contact" type="xs:string"
				minOccurs="1" maxOccurs="1" />
			<xs:element name="dev-contact" type="xs:string" minOccurs="1"
				maxOccurs="1" />
			<xs:element name="qa-contact" type="xs:string" minOccurs="1"
				maxOccurs="1" />
			<xs:element name="se-contact" type="xs:string" minOccurs="1"
				maxOccurs="1" />
			<xs:element name="alert-frequency" type="sla:alert-frequencyType"
				minOccurs="0" maxOccurs="1" />
			<xs:element name="alert-percentage" type="sla:alert-percentageType"
				minOccurs="0" maxOccurs="1" />

			<xs:element name="upstream-apps" type="xs:string"
				minOccurs="0" maxOccurs="1" />

		</xs:sequence>
	</xs:complexType>
    <xs:simpleType name="alert-percentageType">
         <xs:restriction base="xs:integer">
              <xs:minInclusive value="0"/>
              <xs:maxInclusive value="100"/>
         </xs:restriction>
    </xs:simpleType>

    <xs:simpleType name="alert-frequencyType">
        <xs:restriction base="xs:string">
            <xs:enumeration value="NONE"></xs:enumeration>
            <xs:enumeration value="LAST_HOUR"></xs:enumeration>
            <xs:enumeration value="LAST_DAY"></xs:enumeration>
            <xs:enumeration value="LAST_MONTH"></xs:enumeration>
        </xs:restriction>
    </xs:simpleType>

</xs:schema>
```

#### Oozie Workflow Schema Version 0.1

```
<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:workflow="uri:oozie:workflow:0.1"
    elementFormDefault="qualified" targetNamespace="uri:oozie:workflow:0.1">
    <xs:element name="workflow-app" type="workflow:WORKFLOW-APP"/>
    <xs:simpleType name="IDENTIFIER">
        <xs:restriction base="xs:string">
            <xs:pattern value="([a-zA-Z]([\-_a-zA-Z0-9])*){1,39})"/>
        </xs:restriction>
    </xs:simpleType>
    <xs:complexType name="WORKFLOW-APP">
        <xs:sequence>
            <xs:element name="start" type="workflow:START" minOccurs="1" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="unbounded">
                <xs:element name="decision" type="workflow:DECISION" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fork" type="workflow:FORK" minOccurs="1" maxOccurs="1"/>
                <xs:element name="join" type="workflow:JOIN" minOccurs="1" maxOccurs="1"/>
                <xs:element name="kill" type="workflow:KILL" minOccurs="1" maxOccurs="1"/>
                <xs:element name="action" type="workflow:ACTION" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="end" type="workflow:END" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="START">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="END">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="DECISION">
        <xs:sequence>
            <xs:element name="switch" type="workflow:SWITCH" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:element name="switch" type="workflow:SWITCH"/>
    <xs:complexType name="SWITCH">
        <xs:sequence>
            <xs:sequence>
                <xs:element name="case" type="workflow:CASE" minOccurs="1" maxOccurs="unbounded"/>
                <xs:element name="default" type="workflow:DEFAULT" minOccurs="1" maxOccurs="1"/>
            </xs:sequence>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="CASE">
        <xs:simpleContent>
            <xs:extension base="xs:string">
                <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
            </xs:extension>
        </xs:simpleContent>
    </xs:complexType>
    <xs:complexType name="DEFAULT">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="FORK_TRANSITION">
        <xs:attribute name="start" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="FORK">
        <xs:sequence>
            <xs:element name="path" type="workflow:FORK_TRANSITION" minOccurs="2" maxOccurs="unbounded"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="JOIN">
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:element name="kill" type="workflow:KILL"/>
    <xs:complexType name="KILL">
        <xs:sequence>
            <xs:element name="message" type="xs:string" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="ACTION_TRANSITION">
        <xs:attribute name="to" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:element name="map-reduce" type="workflow:MAP-REDUCE"/>
    <xs:element name="pig" type="workflow:PIG"/>
    <xs:element name="ssh" type="workflow:SSH"/>
    <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW"/>
    <xs:element name="fs" type="workflow:FS"/>
    <xs:element name="java" type="workflow:JAVA"/>
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="map-reduce" type="workflow:MAP-REDUCE" minOccurs="1" maxOccurs="1"/>
                <xs:element name="pig" type="workflow:PIG" minOccurs="1" maxOccurs="1"/>
                <xs:element name="ssh" type="workflow:SSH" minOccurs="1" maxOccurs="1"/>
                <xs:element name="sub-workflow" type="workflow:SUB-WORKFLOW" minOccurs="1" maxOccurs="1"/>
                <xs:element name="fs" type="workflow:FS" minOccurs="1" maxOccurs="1"/>
                <xs:element name="java" type="workflow:JAVA" minOccurs="1" maxOccurs="1"/>
                <xs:any namespace="##other" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="ok" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
            <xs:element name="error" type="workflow:ACTION_TRANSITION" minOccurs="1" maxOccurs="1"/>
        </xs:sequence>
        <xs:attribute name="name" type="workflow:IDENTIFIER" use="required"/>
    </xs:complexType>
    <xs:complexType name="MAP-REDUCE">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="0" maxOccurs="1">
                <xs:element name="streaming" type="workflow:STREAMING" minOccurs="0" maxOccurs="1"/>
                <xs:element name="pipes" type="workflow:PIPES" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="PIG">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="SSH">
        <xs:sequence>
            <xs:element name="host" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="command" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="args" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="SUB-WORKFLOW">
        <xs:sequence>
            <xs:element name="app-path" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="propagate-configuration" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="FS">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="move" type="workflow:MOVE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="chmod" type="workflow:CHMOD" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="JAVA">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="workflow:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="workflow:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="main-class" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="capture-output" type="workflow:FLAG" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="FLAG"/>
    <xs:complexType name="CONFIGURATION">
        <xs:sequence>
            <xs:element name="property" minOccurs="1" maxOccurs="unbounded">
                <xs:complexType>
                    <xs:sequence>
                        <xs:element name="name" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="value" minOccurs="1" maxOccurs="1" type="xs:string"/>
                        <xs:element name="description" minOccurs="0" maxOccurs="1" type="xs:string"/>
                    </xs:sequence>
                </xs:complexType>
            </xs:element>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="STREAMING">
        <xs:sequence>
            <xs:element name="mapper" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reducer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="record-reader-mapping" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="env" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="PIPES">
        <xs:sequence>
            <xs:element name="map" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="reduce" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="inputformat" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="partitioner" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="writer" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="program" type="xs:string" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="workflow:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="workflow:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>
    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>
    <xs:complexType name="MOVE">
        <xs:attribute name="source" type="xs:string" use="required"/>
        <xs:attribute name="target" type="xs:string" use="required"/>
    </xs:complexType>
    <xs:complexType name="CHMOD">
        <xs:attribute name="path" type="xs:string" use="required" />
        <xs:attribute name="permissions" type="xs:string" use="required" />
        <xs:attribute name="dir-files" type="xs:string" />
    </xs:complexType>
</xs:schema>
```

<a name="OozieWFExamples"></a>
### Appendix B, Workflow Examples

#### Fork and Join Example

The following workflow definition example executes 4 Map-Reduce jobs in 3 steps, 1 job, 2 jobs in parallel and 1 job.

The output of the jobs in the previous step are use as input for the next jobs.

**Required workflow job parameters:**

   * `resourcemanager` : ResourceManager HOST:PORT
   * `namenode` : NameNode HOST:PORT
   * `input` : input directory
   * `output` : output directory


```
<workflow-app name='example-forkjoinwf' xmlns="uri:oozie:workflow:1.0">
    <start to='firstjob' />
    <action name="firstjob">
        <map-reduce>
            <resource-manager>${resourcemanager}</resource-manager>
            <name-node>${namenode}</name-node>
            <configuration>
                <property>
                    <name>mapred.mapper.class</name>
                    <value>org.apache.hadoop.example.IdMapper</value>
                </property>
                <property>
                    <name>mapred.reducer.class</name>
                    <value>org.apache.hadoop.example.IdReducer</value>
                </property>
                <property>
                    <name>mapred.map.tasks</name>
                    <value>1</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>${input}</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>/usr/foo/${wf:id()}/temp1</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to="fork" />
        <error to="kill" />
    </action>
    <fork name='fork'>
        <path start='secondjob' />
        <path start='thirdjob' />
    </fork>
    <action name="secondjob">
        <map-reduce>
            <resource-manager>${resourcemanager}</resource-manager>
            <name-node>${namenode}</name-node>
            <configuration>
                <property>
                    <name>mapred.mapper.class</name>
                    <value>org.apache.hadoop.example.IdMapper</value>
                </property>
                <property>
                    <name>mapred.reducer.class</name>
                    <value>org.apache.hadoop.example.IdReducer</value>
                </property>
                <property>
                    <name>mapred.map.tasks</name>
                    <value>1</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>/usr/foo/${wf:id()}/temp1</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>/usr/foo/${wf:id()}/temp2</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to="join" />
        <error to="kill" />
    </action>
    <action name="thirdjob">
        <map-reduce>
            <resource-manager>${resourcemanager}</resource-manager>
            <name-node>${namenode}</name-node>
            <configuration>
                <property>
                    <name>mapred.mapper.class</name>
                    <value>org.apache.hadoop.example.IdMapper</value>
                </property>
                <property>
                    <name>mapred.reducer.class</name>
                    <value>org.apache.hadoop.example.IdReducer</value>
                </property>
                <property>
                    <name>mapred.map.tasks</name>
                    <value>1</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>/usr/foo/${wf:id()}/temp1</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>/usr/foo/${wf:id()}/temp3</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to="join" />
        <error to="kill" />
    </action>
    <join name='join' to='finalejob'/>
    <action name="finaljob">
        <map-reduce>
            <resource-manager>${resourcemanager}</resource-manager>
            <name-node>${namenode}</name-node>
            <configuration>
                <property>
                    <name>mapred.mapper.class</name>
                    <value>org.apache.hadoop.example.IdMapper</value>
                </property>
                <property>
                    <name>mapred.reducer.class</name>
                    <value>org.apache.hadoop.example.IdReducer</value>
                </property>
                <property>
                    <name>mapred.map.tasks</name>
                    <value>1</value>
                </property>
                <property>
                    <name>mapred.input.dir</name>
                    <value>/usr/foo/${wf:id()}/temp2,/usr/foo/${wf:id()}/temp3
                    </value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>${output}</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to="end" />
        <ok to="kill" />
    </action>
    <kill name="kill">
        <message>Map/Reduce failed, error message[${wf:errorMessage()}]</message>
    </kill>
    <end name='end'/>
</workflow-app>
```

[::Go back to Oozie Documentation Index::](index.html)


