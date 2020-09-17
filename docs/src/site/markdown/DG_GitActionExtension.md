

[::Go back to Oozie Documentation Index::](index.html)

-----

# Oozie Git Action Extension

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Git Action

The `git` action allows one to clone a Git repository into HDFS. The supported options are `git-uri`, `branch`, `key-path`
and `destination-uri`.

The `git clone` action is executed asynchronously by one of the YARN containers assigned to run on the cluster. If an SSH key is
specified it will be created on the file system in a YARN container's local directory, relying on YARN NodeManager to remove the
file after the action has run.

Path names specified in the `git` action should be able to be parameterized (templatized) using EL expressions,
e.g. `${wf:user()}` . Path name should be specified as an absolute path. Each file path must specify the file system URI.

**Syntax:**


```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <git>
            <git-uri>[SOURCE-URI]</git-uri>
            ...
            <branch>[BRANCH]</branch>
            ...
            <key-path>[HDFS-PATH]</key-path>
            ...
            <destination-uri>[HDFS-PATH]</destination-uri>
        </git>
        <ok to="[NODE-NAME]"/>
        <error to="[NODE-NAME]"/>
    </action>
    ...
</workflow-app>
```

**Example:**


```
<workflow-app name="sample-wf" xmlns="uri:oozie:workflow:0.1">
    ...
    <action name="clone_oozie">
        <git>
            <git-uri>https://github.com/apache/oozie</git-uri>
            <destination-uri>hdfs://my_git_repo_directory</destination-uri>
        </git>
        <ok to="myotherjob"/>
        <error to="errorcleanup"/>
    </action>
    ...
</workflow-app>
```

In the above example, a Git repository on e.g. GitHub.com is cloned to the HDFS directory `my_git_repo_directory` which should not
exist previously on the filesystem. Note that repository addresses outside of GitHub.com but accessible to the YARN container
running the Git action may also be used.

If a `name-node` element is specified, then it is not necessary for any of the paths to start with the file system URI as it is
taken from the `name-node` element.

The `resource-manager` (Oozie 5.x) element has to be specified to name the YARN ResourceManager address.

If any of the paths need to be served from another HDFS namenode, its address has to be part of
that filesystem URI prefix:

```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <git>
            ...
            <name-node>hdfs://name-node.first.company.com:8020</name-node>
            ...
            <key-path>hdfs://name-node.second.company.com:8020/[HDFS-PATH]</key-path>
            ...
        </git>
        ...
    </action>
    ...
</workflow-app>
```

This is also true if the name-node is specified in the global section (see
[Global Configurations](WorkflowFunctionalSpec.html#GlobalConfigurations)).

Be aware that `key-path` might point to a secure object store location other than the current `fs.defaultFS`. In that case,
appropriate file permissions are still necessary (readable by submitting user), credentials provided, etc.

As of workflow schema 1.0, zero or more `job-xml` elements can be specified; these must refer to Hadoop JobConf `job.xml` formatted
files bundled in the workflow application. They can be used to set additional properties for the `FileSystem` instance.

As of schema workflow schema 1.0, if a `configuration` element is specified, then it will also be used to set additional `JobConf`
properties for the `FileSystem` instance. Properties specified in the `configuration` element are overridden by properties
specified in the files specified by any `job-xml` elements.

**Example:**

```
<workflow-app name="[WF-DEF-NAME]" xmlns="uri:oozie:workflow:1.0">
    ...
    <action name="[NODE-NAME]">
        <git>
            ...
            <name-node>hdfs://foo:8020</name-node>
            <job-xml>fs-info.xml</job-xml>
            <configuration>
                <property>
                    <name>some.property</name>
                    <value>some.value</value>
                </property>
            </configuration>
        </git>
        ...
    </action>
    ...
</workflow>
```

## Appendix, Git XML-Schema

### AE.A Appendix A, Git XML-Schema

#### Git Action Schema Version 1.0

```
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:git="uri:oozie:git-action:1.0"
           elementFormDefault="qualified"
           targetNamespace="uri:oozie:git-action:1.0">
    <xs:include schemaLocation="oozie-common-1.0.xsd"/>
    <xs:element name="git" type="git:ACTION"/>
    <xs:complexType name="ACTION">
        <xs:sequence>
            <xs:element name="resource-manager" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="name-node" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="prepare" type="git:PREPARE" minOccurs="0" maxOccurs="1"/>
            <xs:element name="git-uri" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="branch" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="key-path" type="xs:string" minOccurs="0" maxOccurs="1"/>
            <xs:element name="destination-uri" type="xs:string" minOccurs="1" maxOccurs="1"/>
            <xs:element name="configuration" type="git:CONFIGURATION" minOccurs="0" maxOccurs="1"/>
        </xs:sequence>
    </xs:complexType>
</xs:schema>
```

[::Go back to Oozie Documentation Index::](index.html)


