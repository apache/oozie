

[::Go back to Oozie Documentation Index::](index.html)

# Action Configuration

Oozie supports providing default configuration for actions of a particular
action type and default configuration for all actions

## Hadoop Default Configuration Values

Oozie supports action configuration equivalent to the component's `*-site.xml` and `*.properties` files.

The configuration property in the `oozie-site.xml` is `oozie.service.HadoopAccessorService.action.configurations`
and its value must follow the pattern _\[AUTHORITY=ACTION-CONF-DIR_,\]*. Where _AUTHORITY_ is the _HOST:PORT_ of
the Hadoop service (JobTracker/ResourceManager or HDFS). The _ACTION-CONF-DIR_ is the action configuration directory. If the specified directory is a relative path, it will be looked under the Oozie configuration directory. An absolute path can
also be specified. Oozie will load and process the action configuration files in the following order.

   1. All files in _default_/*.xml (sorted by lexical name, files with names lexically lower have lesser precedence than the following ones), if present.
   1. _default_.xml, if present.
   1. All supported files in _actionname_/\*, e.g. _actionname_/\*.xml and _actionname_/*.properties (based on filename extension, sorted by lexical name, files with names lexically lower have lesser precedence than the following ones), if present.
   1. _actionname_.xml, if present.


For example, for _Hive_ action (which has the _actionname_ defined as _hive_ ), the list of files (under relevant _ACTION-CONF-DIR_ ) processed would be,

   1. All files in _default_/*.xml, if present
   1. _default_.xml, if present.
   1. All files in _hive_/\*.xml and _hive_/\*.properties, if present
   1. _hive_.xml, if present.


Files processed earlier for an action have the lowest precedence and can have the configuration parameters redefined.  All files and directories are relative to the _ACTION-CONF-DIR_ directory.

In addition to explicit authorities, a '*' wildcard is supported. The configuration file associated with the wildcard
will be used as default if there is no action configuration for the requested Hadoop service.

For example, the configuration in the `oozie-site.xml` would look like:


```
...
    <property>
        <name>oozie.service.HadoopAccessorService.action.configurations</name>
        <value>*=hadoop-conf,jt-bar:8021=bar-cluster,nn-bar:8020=bar-cluster</value>
    </property>
...
```

The action configuration files use the Hadoop configuration syntax.

By default Oozie does not define any default action configurations.

## Dependency deduplication

Using Oozie with Hadoop 3 may require to have dependency file names distinguishable,
 which means having two files on sharelib and in your app's dependencies with identical names, leads to job submission failure.
To avoid this you can enable the deduplicator by setting oozie.action.dependency.deduplicate=true in oozie-site.xml
(false, by default).
Dependencies which are closer to your application has higher priority: action jar > user workflow libs > action libs > system lib,
where dependency with greater prio is used.

Real world example:
You have an application workflow which is uploaded to HDFS in /apps/app directory. You have your app.jar and dependency jars.
You also define a spark action in your workflow and set use system libs; the HDFS tree is similar to this:

```
 + /apps/app/
   - app.jar
   - workflow.xml
   + libs
     - app.jar
     - jackson-annotations-1.0.jar
 + share/lib/
   + spark
     - app.jar
     - jackson-annotations-1.0.jar
   + oozie
     - jackson-annotations-1.0.jar
```
The deduplicator code will create the following list of files:
`/apps/app/app.jar,/apps/app/libs/jackson-annotations-1.0.jar`
And no other files will be passed at job submission.

[::Go back to Oozie Documentation Index::](index.html)


