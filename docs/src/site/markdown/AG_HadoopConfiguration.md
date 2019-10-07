

[::Go back to Oozie Documentation Index::](index.html)

# Hadoop Configuration

## Hadoop Services Whitelisting

Oozie supports whitelisting Hadoop services (JobTracker, HDFS), via 2 configuration properties:


```
...
    <property>
        <name>oozie.service.HadoopAccessorService.jobTracker.whitelist</name>
        <value> </value>
        <description>
            Whitelisted job tracker for Oozie service.
        </description>
    </property>
    <property>
        <name>oozie.service.HadoopAccessorService.nameNode.whitelist</name>
        <value> </value>
        <description>
            Whitelisted job tracker for Oozie service.
        </description>
    </property>
...
```

The value must follow the pattern `[AUTHORITY,...]`. Where `AUTHORITY` is the `HOST:PORT` of
the Hadoop service (JobTracker, HDFS).

If the value is empty any HOST:PORT is accepted. Empty is the default value.

## Hadoop Default Configuration Values

Oozie supports Hadoop configuration equivalent to the Hadoop `*-site.xml` files.

The configuration property in the `oozie-site.xml` is `oozie.service.HadoopAccessorService.hadoop.configurations`
and its value must follow the pattern `[<AUTHORITY>=<HADOOP_CONF_DIR>,]*`. Where `<AUTHORITY>` is the `HOST:PORT` of
the Hadoop service (JobTracker, HDFS). The `<HADOOP_CONF_DIR>` is a Hadoop configuration directory. If the specified
 directory is a relative path, it will be looked under the Oozie configuration directory. And absolute path can
 also be specified. Oozie will load the Hadoop `*-site.xml` files in the following order: core-site.xml, hdfs-site.xml,
 mapred-site.xml, yarn-site.xml, hadoop-site.xml, ssl-client.xml.

In addition to explicit authorities, a '*' wildcard is supported. The configuration file associated with the wildcard
will be used as default if there is no configuration for the requested Hadoop service.

For example, the configuration in the `oozie-site.xml` would look like:


```
...
    <property>
        <name>oozie.service.HadoopAccessorService.hadoop.configurations</name>
        <value>*=hadoop-conf,jt-bar:8021=bar-cluster,nn-bar:8020=bar-cluster</value>
    </property>
...
```

The Hadoop configuration files use the Hadoop configuration syntax.

By default Oozie defines `*=hadoop-conf` and the default values of the `hadoop-site.xml` file are:


```
<configuration>
   <property>
        <name>mapreduce.jobtracker.kerberos.principal</name>
        <value>mapred/_HOST@LOCALREALM</value>
    </property>
    <property>
      <name>yarn.resourcemanager.principal</name>
      <value>yarn/_HOST@LOCALREALM</value>
    </property>
    <property>
        <name>dfs.namenode.kerberos.principal</name>
        <value>hdfs/_HOST@LOCALREALM</value>
    </property>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
```
## File system custom properties
Some users notified us about issues when they started using Amazon S3A file system -
see [OOZIE-3529](https://issues.apache.org/jira/browse/OOZIE-3529).
Oozie from version 5.2.0 supports custom file system properties which can be defined in the following way.
The example shows how to resolve issues mentioned in OOZIE-3529 by setting the following in oozie-site.xml:
```
    <property>
        <name>oozie.service.HadoopAccessorService.fs.s3a</name>
        <value>fs.s3a.fast.upload.buffer=bytebuffer,fs.s3a.impl.disable.cache=true</value>
    </property>
```
Use `oozie.service.HadoopAccessorService.fs.%s` where `%s` is the schema of the file system.
The value shall be a list of key=value pairs separated using a comma (,). You can use properties as describe below:
 * `property_name=property_value_1=property_value2` will be read as:
     * name: property_name
     * value: property_value_1=property_value2
 * `property1_name=value1,property2_name`
     * name: property1_name
     * value: value1
     * property2_name will be ignored
 * Limitation: the custom file system properties cannot contain comma neither in key nor in value.
 See [OOZIE-3547](https://issues.apache.org/jira/browse/OOZIE-3547).

## Limitations

All actions in a workflow application must interact with the same Hadoop JobTracker and NameNode.

[::Go back to Oozie Documentation Index::](index.html)


