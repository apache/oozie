

[::Go back to Oozie Documentation Index::](index.html)

# Fluent Job API

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Introduction

Oozie is a mature workflow scheduler system. XML is the standard way of defining workflow, coordinator, or bundle jobs.  For users
who prefer an alternative, the Fluent Job API provides a Java interface instead.

### Motivation

Prior to Oozie 5.1.0, the following ways were available to submit a workflow, coordinator, or bundle job: through Oozie CLI or via
HTTP submit a generic workflow, coordinator, or bundle job, or submit a Pig, Hive, Sqoop, or MapReduce workflow job.

As the generic way goes, the user has to have uploaded a workflow, coordinator, or bundle XML and all necessary dependencies like
scripts, JAR or ZIP files, to HDFS beforehand, as well as have a `job.properties` file at command line and / or provide any
missing parameters as part of the command.

As the specific Pig, Hive, or Sqoop ways go, the user can provide all necessary parameters as part of the command issued. A
 `workflow.xml` file will be generated with all the necessary details and stored to HDFS so that Oozie can grab it. Note that
dependencies have to be uploaded to HDFS beforehand as well.

There are some usability problems by using the XML job definition. XML is not an ideal way to express dependencies and a directed
acyclic graph (DAG). We have to define a control flow, that is, which action follows the actual one. It's also necessary to build
the whole control flow up front as XML is a declarative language that doesn't allow for dynamic evaluation. We have to define also
boilerplate actions like start and end - those are present in every Oozie workflow, still need to explicitly define these.

Apart from boilerplate actions, all the transitions between actions have also to be defined and taken care of. Furthermore, multiple
similar actions cannot inherit common properties from each other. Again, the reason being workflows are defined in XML.

Fork and join actions have to be defined in pairs, that is, there shouldn't be defined a join those incoming actions do not share
the same ancestor fork. Such situations would result still in a DAG, but Oozie doesn't currently allow that. Note that with Fluent
Job API new dependencies are introduced automatically when the DAG represented by API code couldn't have been expressed as
fork / join pairs automatically.

Either way, there were no programmatic ways to define workflow jobs. That doesn't mean users could not generate XML themselves -
actually this is something HUE's Oozie UI also tries to target.

### Goals

Fluent Job API aims to solve following from the user's perspective. It provides a Java API instead of declarative XML to define
workflows. It defines dependencies across actions as opposed to defining a control flow. This is how data engineers and data
scientists think. It eliminates all boilerplate actions and transitions. Only the necessary bits should be defined.

Multiple similar actions can inherit from each other. In fact, since Fluent Job API is programmatic, it's possible to generate
actions or even workflows using conditional, iterative, or recursive structures.

Fluent Job API is backwards compatible with workflows defined as XML. That is, it should also be possible to have a Fluent Job API
workflow rendered as XML, as well as coexist XML based and Fluent Job API based workflows in the same Oozie installation at the same
time all workflow action types. When XSDs change, as few manual steps are necessary as possible both on API internal and public
side.

### Non-goals

The following points are not targeted for the initial release of Fluent Job API with Oozie 5.1.0. It doesn't provide API in any
language other than Java. It doesn't provide a REPL. It doesn't allow for dynamic action instantiation depending on e.g. conditional
logic. That is, using the API users still have to implement the whole workflow generation logic in advance.

It has no support for programmatic coordinators and bundles, or even EL expressions created by API builders. Note that EL
expressions for workflows can now be expressed the way these are used in XML workflow definitions, as strings in the right places.

At the moment only the transformation from Fluent Job API to workflow definition is present. The other direction, from workflow
definition to Fluent Job API JAR artifact, though sensible, is not supported.

It's based only on latest XSDs. Older XSD versions, as well as conversion between XSD versions are not supported. Also no support
for user-supplied custom actions / XSDs.

Most of the non-goals may be targeted as enhancements of the Fluent Job API for future Oozie releases.

### Approach

When using the Fluent Job API, the following points are different from the XML jobs definition. Instead of control flow (successor)
definition, the user can define dependencies (parents of an action).

All boilerplate (start, end, ...) has been eliminated, only nodes having useful actions have to be defined.

Control flow and necessary boilerplate are generated automatically by keeping user defined dependencies, and possibly introducing
new dependencies to keep Oozie workflow format of nested fork / join pairs. Note that not every dependency DAG can be expressed in
the Oozie workflow format. When this is not possible, user is notified at build time.

## How To Use

### A Simple Example

The simplest thing to create using the Oozie Fluent Job API is a workflow consisting of only one action. Let's see how it goes, step
by step.

First, put the project `org.apache.oozie:oozie-fluent-job-api` to the build path. In case of a Maven managed build, create a new
Maven project and declare a Maven dependency to `org.apache.oozie:oozie-fluent-job-api`.

Then, create a class that `implements WorkflowFactory` and implement the method `WorkflowFactory#create()`. inside that method,
create a `ShellAction` using `ShellActionBuilder`, fill in some attributes then create a `Workflow` using `WorkflowBuilder` using
the `ShellAction` just built. Return the `Workflow`.

Compile a Fluent Job API jar that has the `Main-Class` attribute set to the `WorkflowFactory` subclass just created,
e.g. `shell-workflow.jar`.

Moving on, [check via command line](DG_CommandLineTool.html#Checking_a_workflow_definition_generated_by_a_Fluent_Job_API_jar_file) that
the compiled API JAR file is valid.

As a finishing touch,
[run via command line](DG_CommandLineTool.html#Running_a_workflow_definition_generated_by_a_Fluent_Job_API_jar_file) the Fluent Job API
workflow.

**For reference, a simplistic API JAR example consisting of a `Workflow` having only one `ShellAction`:**

```
public class MyFirstWorkflowFactory implements WorkflowFactory {
.
    @Override
    public Workflow create() {
        final ShellAction shellAction = ShellActionBuilder.create()
                .withName("shell-action")
                .withResourceManager("${resourceManager}")
                .withNameNode("${nameNode}")
                .withConfigProperty("mapred.job.queue.name", "${queueName}")
                .withExecutable("echo")
                .withArgument("my_output=Hello Oozie")
                .withCaptureOutput(true)
                .build();
.
        final Workflow shellWorkflow = new WorkflowBuilder()
                .withName("shell-workflow")
                .withDagContainingNode(shellAction).build();
.
        return shellWorkflow;
    }
}
```

**After check, the generated workflow XML looks like this:**

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workflow:workflow-app xmlns:workflow="uri:oozie:workflow:1.0"  xmlns:shell="uri:oozie:shell-action:1.0" name="shell-workflow">
.
    <workflow:start to="parent"/>
.
    <workflow:kill name="kill">
        <workflow:message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</workflow:message>
    </workflow:kill>
.
    <workflow:action name="shell-action">
        <shell:shell>
            <shell:resource-manager>${resourceManager}</shell:resource-manager>
            <shell:name-node>${nameNode}</shell:name-node>
            <shell:configuration>
                <shell:property>
                    <shell:name>mapred.job.queue.name</shell:name>
                    <shell:value>${queueName}</shell:value>
                </shell:property>
            </shell:configuration>
            <shell:exec>echo</shell:exec>
            <shell:argument>my_output=Hello Oozie</shell:argument>
            <shell:capture-output/>
        </shell:shell>
        <workflow:ok to="end"/>
        <workflow:error to="kill"/>
    </workflow:action>
.
    <workflow:end name="end"/>
.
</workflow:workflow-app>
```


### A More Verbose Example

**Error handling**

If you would like to provide some error handling in case of action failure, you should add an `ErrorHandler` to the `Node`
representing the action. The error handler action will be added as the `"error-transition"` of the original action in the generated
Oozie workflow XML. Both the `"ok-transition"` and the `"error-transition"` of the error handler action itself will lead to an
autogenerated kill node.

**Here you find an example consisting of a `Workflow` having three `ShellAction`s, an error handler `EmailAction`, and one `decision`
to sort out which way to go:**

```
public class MySecondWorkflowFactory implements WorkflowFactory {
.
    @Override
    public Workflow create() {
        final ShellAction parent = ShellActionBuilder.create()
                .withName("parent")
                .withResourceManager("${resourceManager}")
                .withNameNode("${nameNode}")
                .withConfigProperty("mapred.job.queue.name", "${queueName}")
                .withExecutable("echo")
                .withArgument("my_output=Hello Oozie")
                .withCaptureOutput(true)
                .withErrorHandler(ErrorHandler.buildAsErrorHandler(EmailActionBuilder.create()
                        .withName("email-on-error")
                        .withRecipient("somebody@apache.org")
                        .withSubject("Workflow error")
                        .withBody("Shell action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]")))
                .build();
.
        ShellActionBuilder.createFromExistingAction(parent)
                .withName("happy-path")
                .withParentWithCondition(parent, "${wf:actionData('parent')['my_output'] eq 'Hello Oozie'}")
                .withoutArgument("my_output=Hello Oozie")
                .withArgument("Happy path")
                .withCaptureOutput(null)
                .build();
.
        ShellActionBuilder.createFromExistingAction(parent)
                .withName("sad-path")
                .withParentDefaultConditional(parent)
                .withArgument("Sad path")
                .build();
.
        final Workflow workflow = new WorkflowBuilder()
                .withName("shell-example")
                .withDagContainingNode(parent).build();
.
        return workflow;
    }
}
```

**After check, the generated workflow XML looks like this:**

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workflow:workflow-app ... name="shell-example">
.
    <workflow:start to="parent"/>
.
    <workflow:kill name="kill">
        <workflow:message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</workflow:message>
    </workflow:kill>
.
    <workflow:action name="email-on-error">
        <email:email>
            <email:to>somebody@apache.org</email:to>
            <email:subject>Workflow error</email:subject>
            <email:body>Shell action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</email:body>
        </email:email>
        <workflow:ok to="kill"/>
        <workflow:error to="kill"/>
    </workflow:action>
.
    <workflow:action name="parent">
        <shell:shell>
            <shell:resource-manager>${resourceManager}</shell:resource-manager>
            <shell:name-node>${nameNode}</shell:name-node>
            <shell:configuration>
                <shell:property>
                    <shell:name>mapred.job.queue.name</shell:name>
                    <shell:value>${queueName}</shell:value>
                </shell:property>
            </shell:configuration>
            <shell:exec>echo</shell:exec>
            <shell:argument>my_output=Hello Oozie</shell:argument>
            <shell:capture-output/>
        </shell:shell>
        <workflow:ok to="decision1"/>
        <workflow:error to="email-on-error"/>
    </workflow:action>
.
    <workflow:decision name="decision1">
        <workflow:switch>
            <workflow:case to="happy-path">${wf:actionData('parent')['my_output'] eq 'Hello Oozie'}</workflow:case>
            <workflow:default to="sad-path"/>
        </workflow:switch>
    </workflow:decision>
.
    <workflow:action name="happy-path">
        <shell:shell>
            <shell:resource-manager>${resourceManager}</shell:resource-manager>
            <shell:name-node>${nameNode}</shell:name-node>
            <shell:configuration>
                <shell:property>
                    <shell:name>mapred.job.queue.name</shell:name>
                    <shell:value>${queueName}</shell:value>
                </shell:property>
            </shell:configuration>
            <shell:exec>echo</shell:exec>
            <shell:argument>Happy path</shell:argument>
        </shell:shell>
        <workflow:ok to="end"/>
        <workflow:error to="email-on-error"/>
    </workflow:action>
.
    <workflow:action name="sad-path">
        <shell:shell>
            <shell:resource-manager>${resourceManager}</shell:resource-manager>
            <shell:name-node>${nameNode}</shell:name-node>
            <shell:configuration>
                <shell:property>
                    <shell:name>mapred.job.queue.name</shell:name>
                    <shell:value>${queueName}</shell:value>
                </shell:property>
            </shell:configuration>
            <shell:exec>echo</shell:exec>
            <shell:argument>my_output=Hello Oozie</shell:argument>
            <shell:argument>Sad path</shell:argument>
            <shell:capture-output/>
        </shell:shell>
        <workflow:ok to="end"/>
        <workflow:error to="email-on-error"/>
    </workflow:action>
.
    <workflow:end name="end"/>
.
</workflow:workflow-app>
```

### Running the JavaMain example

Oozie contains several simple Fluent Job API examples. The JavaMain Fluent JOB API example generates a workflow
similar to the basic java-main example.

The source code for all the Fluent Job API examples can be found in the `oozie-examples.tar.gz` file.

#### Compilation

To compile the examples we also need the `oozie-fluent-job-api` jar file.
The name of the file also contains the Oozie version number, in this example we assume that the name of the file is
`oozie-fluent-job-api-5.1.0.jar`.

Assuming that the `src` directory contains the source files we can use the following command to compile the JavaMain example:

```
javac -classpath oozie-fluent-job-api-5.1.0.jar src/org/apache/oozie/example/fluentjob/JavaMain.java
```

#### Jar creation

The next command creates the jar file:

```
jar cfe fluentjob-javamain-example.jar org.apache.oozie.example.fluentjob.JavaMain -C src \
org/apache/oozie/example/fluentjob/JavaMain.class
```

This jar contains only the `JavaMain.class` file. The content of the `MAINFEST.MF` file is the following:

```
Manifest-Version: 1.0
Created-By: 1.8.0_171 (Oracle Corporation)
Main-Class: org.apache.oozie.example.fluentjob.JavaMain
```

#### Validating the jar

It is possible to validate the jar file:

```
oozie job -oozie http://localhost:11000/oozie -validatejar fluentjob-javamain-example.jar
```

The command should print out: `Valid workflow-app`

If we also use the `-verbose` option the command prints out the generated XML as well:

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workflow:workflow-app xmlns:email="uri:oozie:email-action:0.2" xmlns:workflow="uri:oozie:workflow:1.0" name="java-main-example">
    <workflow:start to="java-main"/>
    <workflow:kill name="kill">
        <workflow:message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</workflow:message>
    </workflow:kill>
    <workflow:action name="email-on-error">
        <email:email>
            <email:to>somebody@apache.org</email:to>
            <email:subject>Workflow error</email:subject>
            <email:body>Shell action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</email:body>
        </email:email>
        <workflow:ok to="kill"/>
        <workflow:error to="kill"/>
    </workflow:action>
    <workflow:action name="java-main">
        <workflow:java>
            <workflow:resource-manager>${resourceManager}</workflow:resource-manager>
            <workflow:name-node>${nameNode}</workflow:name-node>
            <workflow:configuration>
                <workflow:property>
                    <workflow:name>mapred.job.queue.name</workflow:name>
                    <workflow:value>${queueName}</workflow:value>
                </workflow:property>
            </workflow:configuration>
            <workflow:main-class>org.apache.oozie.example.DemoJavaMain</workflow:main-class>
            <workflow:arg>Hello</workflow:arg>
            <workflow:arg>Oozie!</workflow:arg>
            <workflow:archive>${nameNode}/user/${wf:user()}/${examplesRoot}/apps/java-main/lib/oozie-examples-${projectVersion}.jar</workflow:archive>
        </workflow:java>
        <workflow:ok to="end"/>
        <workflow:error to="email-on-error"/>
    </workflow:action>
    <workflow:end name="end"/>
</workflow:workflow-app>
```

### Running the jar

To run the jar it is also necessary to provide a properties file using the `-config` option:

```
oozie job -oozie http://localhost:11000/oozie -runjar fluentjob-javamain-example.jar -config fluentjob-javamain.properties
```

The contents of the `fluentjob-javamain.properties` file is similar to the `job.properties` file of the basic java-main example,
but we also need one extra property called `projectVersion`. The following shows a sample properties file:

```
resourceManager=localhost:8032
nameNode=hdfs://localhost:9000
queueName=default
examplesRoot=examples
projectVersion=5.1.0
```

It is also possible to use the `-verbose` option here if we want to print out the generated XML.

### Runtime Limitations

Even if Fluent Job API tries to abstract away the task of assembly job descriptor XML files, there are some runtime
limitations apart from the [non-goals section](DG_FluentJobAPI.html#Non-goals). All such limitations are based on the current
implementations and subject to further improvements and fixes.

There is only one `kill` possibility in every `workflow`. That is, there can be defined only one `action` to be executed just before
any other `action` turns to be `kill`ed. Furthermore, `kill` goes to `end` directly. That means, there cannot be defined an
intricate network of `kill` nodes, cascading sometimes to other `action` nodes, avoiding going to `end` in the first place.

There are places where `decision` node generation fails, throwing an `Exception`. The problem is that during the transformation,
Fluent Job API reaches a state where there is a `fork` that transitions to two `decision` nodes, which in turn split into two paths
each. One of the paths from the first `decision` joins a path from the other `decision`, but the remaining conditional paths never
meet. Therefore, not all paths originating from the `fork` converge to the same `join`.

## Appendixes

### AE.A Appendix A, API JAR format

It's kept simple - all the necessary Java class files that are needed are packed into a JAR file, that has a `META-INF/MANIFEST.MF`
with a single entry having the `Main-Class` attribute set to the fully qualified name of the entry class, the one that
`implements WorkflowFactory`:

```
Main-Class: org.apache.oozie.jobs.api.factory.MyFirstWorkflowFactory
```

**An example of the command line assembly of such an API JAR:**

```
jar cfe simple-workflow.jar org.apache.oozie.fluentjob.api.factory.MyFirstWorkflowFactory \
-C /Users/forsage/Workspace/oozie/fluent-job/fluent-job-api/target/classes \
org/apache/oozie/jobs/api/factory/MyFirstWorkflowFactory.class
```

### AE.B Appendix B, Some Useful Builder classes

For a complete list of `Builder` classes, please have a look at `oozie-fluent-job-api` artifact's following packages:

   * `org.apache.oozie.fluentjob.api.action` - `ActionBuilder` classes
   * `org.apache.oozie.fluentjob.api.factory` - the single entry point, `WorkflowFactory` is here
   * `org.apache.oozie.fluentjob.api.workflow` - workflow related `Builder` classes

On examples how to use these please see `oozie-examples` artifact's `org.apache.oozie.example.fluentjob` package.

### AE.C Appendix C, How To Extend

Sometimes there are new XSD versions of an existing custom or core workflow action, sometimes it's a new custom workflow action that
gets introduced. In any case, Fluent Job API needs to keep up with the changes.

Here are the steps needed:

   * in `fluent-job-api/pom.xml` extend or modify `jaxb2-maven-plugin` section `sources` by a new `source`
   * in `fluent-job-api/src/main/xjb/bindings.xml` extend by a new or modify an existing `jaxb:bindings`
   * in `fluent-job-api`, `org.apache.oozie.fluentjob.api.mapping` package, introduce a new or modify an existing `DozerConverter`
   * in `dozer_config.xml`, introduce a new or modify an existing `converter` inside `custom-converters`
   * in `fluent-job-api`, `org.apache.oozie.fluentjob.api.action`, introduce a new `Action` and a new `Builder`
   * write new / modify existing relevant unit and integration tests

### AE.D Appendix D, API compatibility guarantees

Fluent Job API is available beginning version 5.1.0. It's marked `@InterfaceAudience.Private` (intended for use in Oozie itself) and
`@InterfaceStability.Unstable` (no stability guarantees are provided across any level of release granularity) to indicate that for
the next few minor releases it's bound to change a lot.

Beginning from around 5.4.0 planning the next phase, `@InterfaceStability.Evolving` (compatibility breaking only between minors),
and a few minor releases later, `@InterfaceAudience.Public` (safe to use outside of Oozie).

[::Go back to Oozie Documentation Index::](index.html)


