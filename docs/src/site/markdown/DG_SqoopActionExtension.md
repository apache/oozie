

[::Go back to Oozie Documentation Index::](index.html)

-----

# Oozie Sqoop Action Extension

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Sqoop Action

**IMPORTANT:** The Sqoop action requires Apache Hadoop 1.x or 2.x.

The `sqoop` action runs a Sqoop job.

The workflow job will wait until the Sqoop job completes before
continuing to the next action.

To run the Sqoop job, you have to configure the `sqoop` action with the `resource-manager`, `name-node` and Sqoop `command`
or `arg` elements as well as configuration.

A `sqoop` action can be configured to create or delete HDFS directories
before starting the Sqoop job.

Sqoop configuration can be specified with a file, using the `job-xml`
element, and inline, using the `configuration` elements.

Oozie EL expressions can be used in the inline configuration. Property
values specified in the `configuration` element override values specified
in the `job-xml` file.

Note that YARN `yarn.resourcemanager.address` / `resource-manager` and HDFS `fs.default.name` / `name-node` properties must not
be present in the inline configuration.

As with Hadoop `map-reduce` jobs, it is possible to add files and
archives in order to make them available to the Sqoop job. Refer to the
[WorkflowFunctionalSpec#FilesArchives][Adding Files and Archives for the Job]
section for more information about this feature.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <sqoop xmlns="uri:oozie:sqoop-action:1.0">
            <resource-manager>[RESOURCE-MANAGER]</resource-manager>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
               <delete path="[PATH]"/>
               ...
               <mkdir path="[PATH]"/>
               ...
            </prepare>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <command>[SQOOP-COMMAND]</command>
            <arg>[SQOOP-ARGUMENT]</arg>
            ...
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
        </sqoop>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The `prepare` element, if present, indicates a list of paths to delete
or create before starting the job. Specified paths must start with `hdfs://HOST:PORT`.

The `job-xml` element, if present, specifies a file containing configuration
for the Sqoop job. As of schema 0.3, multiple `job-xml` elements are allowed in order to
specify multiple `job.xml` files.

The `configuration` element, if present, contains configuration
properties that are passed to the Sqoop job.

**Sqoop command**

The Sqoop command can be specified either using the `command` element or multiple `arg`
elements.

When using the `command` element, Oozie will split the command into multiple arguments. There are two command splitting algorithms
in Oozie.

If `oozie.action.sqoop.shellsplitter` property is set to `false` Oozie will split the command on every space.

If `oozie.action.sqoop.shellsplitter` property is set to `true` Oozie will split the command like `bash` splits the commands.
In this case it's possible to group strings together using quotes. For instance oozie will split `--query "select * from employee"`
into two tokens: `--query` and `select * from employee`.

The default value of the `oozie.action.sqoop.shellsplitter` property is `false`.

When using the `arg` elements, Oozie will pass each argument value as an argument to Sqoop.

Consult the Sqoop documentation for a complete list of valid Sqoop commands.

All the above elements can be parameterized (templatized) using EL
expressions.

**Examples:**

Using the `command` element:


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="myfirsthivejob">
        <sqoop xmlns="uri:oozie:sqoop-action:1.0">
            <resource-manager>foo:8032</resource-manager>
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
            <command>import  --connect jdbc:hsqldb:file:db.hsqldb --table TT --target-dir hdfs://localhost:8020/user/tucu/foo -m 1</command>
        </sqoop>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

The same Sqoop action using `arg` elements:


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="myfirstsqoopjob">
        <sqoop xmlns="uri:oozie:sqoop-action:1.0">
            <resource-manager>foo:8032</resource-manager>
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
            <arg>import</arg>
            <arg>--connect</arg>
            <arg>jdbc:hsqldb:file:db.hsqldb</arg>
            <arg>--table</arg>
            <arg>TT</arg>
            <arg>--target-dir</arg>
            <arg>hdfs://localhost:8020/user/tucu/foo</arg>
            <arg>-m</arg>
            <arg>1</arg>
        </sqoop>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

NOTE: The `arg` elements syntax, while more verbose, allows to have spaces in a single argument, something useful when
using free from queries.

### Sqoop Action Counters

The counters of the map-reduce job run by the Sqoop action are available to be used in the workflow via the
[hadoop:counters() EL function](WorkflowFunctionalSpec.html#HadoopCountersEL).

If the Sqoop action run an import all command, the `hadoop:counters()` EL will return the aggregated counters
of all map-reduce jobs run by the Sqoop import all command.

### Sqoop Action Logging

Sqoop action logs are redirected to the Oozie Launcher map-reduce job task STDOUT/STDERR that runs Sqoop.

The logging level of the Sqoop action can set in the Sqoop action configuration using the
property `oozie.sqoop.log.level`. The default value is `INFO`.

## Appendix, Sqoop XML-Schema

### AE.A Appendix A, Sqoop XML-Schema

#### Sqoop Action Schema Version 1.0

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:sqoop="uri:oozie:sqoop-action:1.0"
           elementFormDefault="qualified"
           targetNamespace="uri:oozie:sqoop-action:1.0">
.
    <xs:include schemaLocation="oozie-common-1.0.xsd"/>
.
    <xs:element name="sqoop" type="sqoop:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="sqoop:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="launcher" type="sqoop:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="sqoop:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice>
                <xs:element name="command" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="arg" type="xs:string" minOccurs="1" maxOccurs="unbounded"/>
            </xs:choice>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
</xs:schema>
```

#### Sqoop Action Schema Version 0.3

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:sqoop="uri:oozie:sqoop-action:0.3" elementFormDefault="qualified"
           targetNamespace="uri:oozie:sqoop-action:0.3">

    <xs:element name="sqoop" type="sqoop:ACTION"/>

    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="sqoop:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="sqoop:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice>
                <xs:element name="command" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="arg" type="xs:string" minOccurs="1" maxOccurs="unbounded"/>
            </xs:choice>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

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

    <xs:complexType name="PREPARE">
        <xs:sequence>
            <xs:element name="delete" type="sqoop:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="sqoop:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>

    <xs:complexType name="DELETE">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

    <xs:complexType name="MKDIR">
        <xs:attribute name="path" type="xs:string" use="required"/>
    </xs:complexType>

</xs:schema>
```

#### Sqoop Action Schema Version 0.2

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:sqoop="uri:oozie:sqoop-action:0.2" elementFormDefault="qualified"
           targetNamespace="uri:oozie:sqoop-action:0.2">

    <xs:element name="sqoop" type="sqoop:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="sqoop:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="sqoop:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice>
                <xs:element name="command" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="arg" type="xs:string" minOccurs="1" maxOccurs="unbounded"/>
            </xs:choice>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
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
            <xs:element name="delete" type="sqoop:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="sqoop:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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
</xs:schema>
```

[::Go back to Oozie Documentation Index::](index.html)


