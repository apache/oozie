

# Oozie, Workflow Engine for Apache Hadoop

Oozie v3 is a server based _Bundle Engine_ that provides a higher-level oozie abstraction that will batch a set of coordinator applications. The user will be able to start/stop/suspend/resume/rerun a set coordinator jobs in the bundle level resulting a better and easy operational control.

Oozie v2 is a server based _Coordinator Engine_ specialized in running workflows based on time and data triggers.
(e.g. wait for my input data to exist before running my workflow).

Oozie v1 is a server based _Workflow Engine_ specialized in running workflow jobs with actions that
execute Hadoop Map/Reduce and Pig jobs.

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Distribution Contents

Oozie distribution consists of a single 'tar.gz' file containing:

   * Readme, license, notice & [Release log](./release-log.txt) files.
   * Oozie server: `oozie-server` directory.
   * Scripts: `bin/` directory, client and server scripts.
   * Binaries: `lib/` directory, client JAR files.
   * Configuration: `conf/` server configuration directory.
   * Archives:
      * `oozie-client-*.tar.gz`: Client tools.
      * `oozie.war`: Oozie WAR file.
      * `docs.zip`: Documentation.
      * `oozie-examples-*.tar.gz`: Examples.
      * `oozie-sharelib-*.tar.gz`: Share libraries (with Streaming, Pig JARs).

## Quick Start

Enough reading already? Follow the steps in [Oozie Quick Start](DG_QuickStart.html) to get Oozie up and running.

## Developer Documentation

   * [Overview](DG_Overview.html)
   * [Oozie Quick Start](DG_QuickStart.html)
   * [Running the Examples](DG_Examples.html)
   * [Workflow Functional Specification](WorkflowFunctionalSpec.html)
   * [Coordinator Functional Specification](CoordinatorFunctionalSpec.html)
   * [Bundle Functional Specification](BundleFunctionalSpec.html)
   * [EL Expression Language Quick Reference](https://docs.oracle.com/javaee/7/tutorial/jsf-el.htm)
   * [Command Line Tool](DG_CommandLineTool.html)
   * [Workflow Re-runs Explained](DG_WorkflowReRun.html)
   * [HCatalog Integration Explained](DG_HCatalogIntegration.html)
   * [Oozie Client Javadocs](./client/apidocs/index.html)
   * [Oozie Core Javadocs](./core/apidocs/index.html)
   * [Oozie Web Services API](WebServicesAPI.html)
   * [Action Authentication](DG_ActionAuthentication.html)
   * [Fluent Job API](DG_FluentJobAPI.html)

### Action Extensions

   * [Email Action](DG_EmailActionExtension.html)
   * [Shell Action](DG_ShellActionExtension.html)
   * [Hive Action](DG_HiveActionExtension.html)
   * [Hive 2 Action](DG_Hive2ActionExtension.html)
   * [Sqoop Action](DG_SqoopActionExtension.html)
   * [Ssh Action](DG_SshActionExtension.html)
   * [DistCp Action](DG_DistCpActionExtension.html)
   * [Spark Action](DG_SparkActionExtension.html)
   * [Git Action](DG_GitActionExtension.html)
   * [Writing a Custom Action Executor](DG_CustomActionExecutor.html)

### Job Status and SLA Monitoring

   * [JMS Notifications for Job and SLA](DG_JMSNotifications.html)
   * [Configuring and Monitoring SLA](DG_SLAMonitoring.html)

## Administrator Documentation

   * [Oozie Install](AG_Install.html)
   * [Oozie Logging](AG_OozieLogging.html)
   * [Hadoop Configuration](AG_HadoopConfiguration.html)
   * [Action Configuration](AG_ActionConfiguration.html)
   * [Oozie Monitoring](AG_Monitoring.html)
   * [Command Line Tool](DG_CommandLineTool.html)
   * [Oozie Upgrade](AG_OozieUpgrade.html)

<a name="LicenseInfo"></a>
## Licensing Information

Oozie is distributed under [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

For details on the license of the dependent components, refer to the
[Dependencies Report, Licenses section](./dependencies.html#Licenses).

Oozie bundles an embedded Jetty 9.x.

Some of the components in the dependencies report don't mention their license in the published POM. They are:

   * JDOM: [JDOM License](http://www.jdom.org/docs/faq.html#a0030) (Apache style).
   * Oro: [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).



## Engineering Documentation

   * [Building Oozie](ENG_Building.html)
   * [Dependencies Report](./dependencies.html)

## MiniOozie Documentation

   * [Testing User Oozie Applications Using MiniOozie](ENG_MiniOozie.html)

## Oozie User Authentication Documentation

   * [Create Custom Oozie Authentication](ENG_Custom_Authentication.html)


