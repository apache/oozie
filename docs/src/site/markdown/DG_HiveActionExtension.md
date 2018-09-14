

[::Go back to Oozie Documentation Index::](index.html)

-----

# Oozie Hive Action Extension

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

<a name="HiveAction"></a>
## Hive Action

The `hive` action runs a Hive job.

The workflow job will wait until the Hive job completes before
continuing to the next action.

To run the Hive job, you have to configure the `hive` action with the `resource-manager`, `name-node` and Hive `script`
(or Hive `query`) elements as well as the necessary parameters and configuration.

A `hive` action can be configured to create or delete HDFS directories
before starting the Hive job.

Hive configuration can be specified with a file, using the `job-xml`
element, and inline, using the `configuration` elements.

Oozie EL expressions can be used in the inline configuration. Property
values specified in the `configuration` element override values specified
in the `job-xml` file.

Note that YARN `yarn.resourcemanager.address` (`resource-manager`) and HDFS `fs.default.name` (`name-node`) properties
must not be present in the inline configuration.

As with Hadoop `map-reduce` jobs, it is possible to add files and
archives in order to make them available to the Hive job. Refer to the
[WorkflowFunctionalSpec#FilesArchives][Adding Files and Archives for the Job]
section for more information about this feature.

Oozie Hive action supports Hive scripts with parameter variables, their
syntax is `${VARIABLES}`.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <hive xmlns="uri:oozie:hive-action:1.0">
            <resource-manager>[RESOURCE-MANAGER]</resource-manager>
            <name-node>[NAME-NODE]</name-node>
            <prepare>
               <delete path="[PATH]"/>
               ...
               <mkdir path="[PATH]"/>
               ...
            </prepare>
            <job-xml>[HIVE SETTINGS FILE]</job-xml>
            <configuration>
                <property>
                    <name>[PROPERTY-NAME]</name>
                    <value>[PROPERTY-VALUE]</value>
                </property>
                ...
            </configuration>
            <script>[HIVE-SCRIPT]</script>
            <param>[PARAM-VALUE]</param>
                ...
            <param>[PARAM-VALUE]</param>
            <file>[FILE-PATH]</file>
            ...
            <archive>[FILE-PATH]</archive>
            ...
        </hive>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

The `prepare` element, if present, indicates a list of paths to delete
or create before starting the job. Specified paths must start with `hdfs://HOST:PORT`.

The `job-xml` element, if present, specifies a file containing configuration
for the Hive job. As of schema 0.3, multiple `job-xml` elements are allowed in order to
specify multiple `job.xml` files.

The `configuration` element, if present, contains configuration
properties that are passed to the Hive job.

The `script` element must contain the path of the Hive script to
execute. The Hive script can be templatized with variables of the form
`${VARIABLE}`. The values of these variables can then be specified
using the `params` element.

The `query` element available from uri:oozie:hive-action:0.6, can be used instead of the
`script` element. It allows for embedding queries within the `worklfow.xml` directly.
Similar to the `script` element, it also allows for the templatization of variables in the
form `${VARIABLE}`.

The `params` element, if present, contains parameters to be passed to
the Hive script.

All the above elements can be parameterized (templatized) using EL
expressions.

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="myfirsthivejob">
        <hive xmlns="uri:oozie:hive-action:1.0">
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
            <script>myscript.q</script>
            <param>InputDir=/home/tucu/input-data</param>
            <param>OutputDir=${jobOutput}</param>
        </hive>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

### Hive Default and Site Configuration Files

Hive (as of Hive 0.8) ignores a `hive-default.xml` file.  As a result, Oozie (as of Oozie 3.4) ignores the `oozie.hive.defaults`
property that was previously required by earlier versions of Oozie for the Hive action.

### Hive Action Logging

Hive action logs are redirected to the Oozie Launcher map-reduce job task STDOUT/STDERR that runs Hive.

From Oozie web-console, from the Hive action pop up using the 'Console URL' link, it is possible
to navigate to the Oozie Launcher map-reduce job task logs via the Hadoop job-tracker web-console.

The logging level of the Hive action can set in the Hive action configuration using the
property `oozie.hive.log.level`. The default value is `INFO`.

## Appendix, Hive XML-Schema

### AE.A Appendix A, Hive XML-Schema

#### Hive Action Schema Version 1.0

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hive="uri:oozie:hive-action:1.0"
           elementFormDefault="qualified"
           targetNamespace="uri:oozie:hive-action:1.0">
.
    <xs:include schemaLocation="oozie-common-1.0.xsd"/>
.
    <xs:element name="hive" type="hive:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="hive:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="launcher" type="hive:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="hive:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="query" type="xs:string" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="file" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="archive" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
</xs:schema>
```

#### Hive Action Schema Version 0.6

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hive="uri:oozie:hive-action:0.6" elementFormDefault="qualified"
           targetNamespace="uri:oozie:hive-action:0.6">
.
    <xs:element name="hive" type="hive:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="hive:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="hive:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:choice minOccurs="1" maxOccurs="1">
                <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="query"  type="xs:string" minOccurs="1" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="hive:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="hive:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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
#### Hive Action Schema Version 0.5

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hive="uri:oozie:hive-action:0.5" elementFormDefault="qualified"
           targetNamespace="uri:oozie:hive-action:0.5">
.
    <xs:element name="hive" type="hive:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="hive:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="hive:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="argument" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="hive:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="hive:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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

#### Hive Action Schema Version 0.4

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hive="uri:oozie:hive-action:0.4" elementFormDefault="qualified"
           targetNamespace="uri:oozie:hive-action:0.4">
.
    <xs:element name="hive" type="hive:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="hive:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="hive:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="hive:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="hive:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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

#### Hive Action Schema Version 0.3

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hive="uri:oozie:hive-action:0.3" elementFormDefault="qualified"
           targetNamespace="uri:oozie:hive-action:0.3">
.
    <xs:element name="hive" type="hive:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="hive:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="configuration" type="hive:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="hive:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="hive:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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

#### Hive Action Schema Version 0.2

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:hive="uri:oozie:hive-action:0.2" elementFormDefault="qualified"
           targetNamespace="uri:oozie:hive-action:0.2">
.
    <xs:element name="hive" type="hive:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="hive:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="job-xml" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="hive:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="script" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="param" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="hive:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="hive:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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


