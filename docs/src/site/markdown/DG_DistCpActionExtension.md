

[::Go back to Oozie Documentation Index::](index.html)

-----

# Oozie DistCp Action Extension

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## DistCp Action

The `DistCp` action uses Hadoop distributed copy to copy files from one cluster to another or within the same cluster.

**IMPORTANT:** The DistCp action may not work properly with all configurations (secure, insecure) in all versions
of Hadoop. For example, distcp between two secure clusters is tested and works well. Same is true with two insecure
clusters. In cases where a secure and insecure clusters are involved, distcp will not work.

Both Hadoop clusters have to be configured with proxyuser for the Oozie process as explained
[here](DG_QuickStart.html#HadoopProxyUser) on the Quick Start page.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="distcp-example">
        <distcp xmlns="uri:oozie:distcp-action:1.0">
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode1}</name-node>
            <arg>${nameNode1}/path/to/input.txt</arg>
            <arg>${nameNode2}/path/to/output.txt</arg>
            </distcp>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    ...
</workflow-app>
```

The first `arg` indicates the input and the second `arg` indicates the output.  In the above example, the input is on `namenode1`
and the output is on `namenode2`.

**IMPORTANT:** If using the DistCp action between 2 secure clusters, the following property must be added to the `configuration` of
the action:

```
<property>
    <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
    <value>${nameNode1},${nameNode2}</value>
</property>
```

The `DistCp` action is also commonly used to copy files within the same cluster. Cases where copying files within
a directory to another directory or directories to target directory is supported. Example below will illustrate a
copy within a cluster, notice the source and target `nameNode` is the same and use of `*` syntax is supported to
represent only child files or directories within a source directory. For the sake of the example, `jobTracker` and `resourceManager`
are synonymous.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="copy-example">
        <distcp xmlns="uri:oozie:distcp-action:1.0">
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <arg>${nameNode}/path/to/source/*</arg>
            <arg>${nameNode}/path/to/target/</arg>
        </distcp>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    ...
</workflow-app>
```

## Appendix, DistCp XML-Schema

### AE.A Appendix A, DistCp XML-Schema

#### DistCp Action Schema Version 1.0

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:distcp="uri:oozie:distcp-action:1.0" elementFormDefault="qualified"
           targetNamespace="uri:oozie:distcp-action:1.0">
.
    <xs:include schemaLocation="oozie-common-1.0.xsd"/>
.
    <xs:element name="distcp" type="distcp:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:choice>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            </xs:choice>
            <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="prepare" type="distcp:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="launcher" type="distcp:LAUNCHER" minOccurs="0" maxOccurs="1"/>
            <xs:element name="configuration" type="distcp:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
            <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
        </xs:sequence>
    </xs:complexType>
.
</xs:schema>
```

#### DistCp Action Schema Version 0.2

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:distcp="uri:oozie:distcp-action:0.2" elementFormDefault="qualified"
           targetNamespace="uri:oozie:distcp-action:0.2">
.
    <xs:element name="distcp" type="distcp:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
                <xs:element name="job-tracker" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="name-node" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="prepare" type="distcp:PREPARE" minOccurs="0" maxOccurs="1"/>
                <xs:element name="configuration" type="distcp:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
                <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="distcp:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="distcp:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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

#### DistCp Action Schema Version 0.1

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:distcp="uri:oozie:distcp-action:0.1" elementFormDefault="qualified"
           targetNamespace="uri:oozie:distcp-action:0.1">
.
    <xs:element name="distcp" type="distcp:ACTION"/>
.
    <xs:complexType name="ACTION">
        <xs:sequence>
                <xs:element name="job-tracker" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
                <xs:element name="prepare" type="distcp:PREPARE" minOccurs="0" maxOccurs="1"/>
                <xs:element name="configuration" type="distcp:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
                <xs:element name="java-opts" type="xs:string" minOccurs="0" maxOccurs="1"/>
                <xs:element name="arg" type="xs:string" minOccurs="0" maxOccurs="unbounded"/>
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
            <xs:element name="delete" type="distcp:DELETE" minOccurs="0" maxOccurs="unbounded"/>
            <xs:element name="mkdir" type="distcp:MKDIR" minOccurs="0" maxOccurs="unbounded"/>
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


