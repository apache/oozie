

[::Go back to Oozie Documentation Index::](index.html)

# Oozie Monitoring

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Oozie Instrumentation

Oozie code is instrumented in several places to collect runtime metrics. The instrumentation data can be used to
determine the health of the system, performance of the system, and to tune the system.

This comes in two flavors:

   * metrics (by default enabled since 5.0.0)
   * instrumentation (deprecated and by default disabled since 5.0.0)

The instrumentation is accessible via the Admin web-services API (see the [metrics](WebServicesAPI.html#Oozie_Metrics) and
[instrumentation](WebServicesAPI.html#Oozie_Instrumentation) Web Services API documentations for more details) and is also written on
regular intervals to an instrumentation log.

Instrumentation data includes variables, samplers, timers and counters.

### Variables

   * oozie
      * version: Oozie build version.

   * configuration
      * config.dir: directory from where the configuration files are loaded. If null, all configuration files are loaded from the classpath. [Configuration files are described here](AG_Install.html#Oozie_Configuration).
      * config.file: the Oozie custom configuration for the instance.

   * jvm
      * free.memory
      * max.memory
      * total.memory

   * locks
      * locks: Locks are used by Oozie to synchronize access to workflow and action entries when the database being used does not support 'select for update' queries. (MySQL supports 'select for update').

   * logging
      * config.file: Log4j '.properties' configuration file.
      * from.classpath: whether the config file has been read from the classpath or from the config directory.
      * reload.interval: interval at which the config file will be reloaded. 0 if the config file will never be reloaded, when loaded from the classpath is never reloaded.

### Samplers - Poll data at a fixed interval (default 1 sec) and report an average utilization over a longer period of time (default 60 seconds).

Poll for data over fixed interval and generate an average over the time interval. Unless specified, all samplers in
Oozie work on a 1 minute interval.

   * callablequeue
      * delayed.queue.size: The size of the delayed command queue.
      * queue.size: The size of the command queue.
      * threads.active: The number of threads processing callables.

   * jdbc:
      * connections.active: Active Connections over the past minute.

   * webservices: Requests to the Oozie HTTP endpoints over the last minute.
      * admin
      * callback
      * job
      * jobs
      * requests
      * version

### Counters - Maintain statistics about the number of times an event has occurred, for the running Oozie instance. The values are reset if the Oozie instance is restarted.

   * action.executors - Counters related to actions.
      * [action_type]#action.[operation_performed] (start, end, check, kill)
      * [action_type]#ex.[exception_type] (transient, non-transient, error, failed)
      * e.g.
```
ssh#action.end: 306
ssh#action.start: 316
```

   * callablequeue - count of events in various execution queues.
      * delayed.queued: Number of commands queued with a delay.
      * executed: Number of executions from the queue.
      * failed: Number of queue attempts which failed.
      * queued: Number of queued commands.

   * commands: Execution Counts for various commands. This data is generated for all commands.
      * action.end
      * action.notification
      * action.start
      * callback
      * job.info
      * job.notification
      * purge
      * signal
      * start
      * submit

   * jobs: Job Statistics
      * start: Number of started jobs.
      * submit: Number of submitted jobs.
      * succeeded: Number of jobs which succeeded.
      * kill: Number of killed jobs.

   * authorization
      * failed: Number of failed authorization attempts.

   * webservices: Number of request to various web services along with the request type.
      * failed: total number of failed requests.
      * requests: total number of requests.
      * admin
      * admin-GET
      * callback
      * callback-GET
      * jobs
      * jobs-GET
      * jobs-POST
      * version
      * version-GET

### Timers - Maintain information about the time spent in various operations.

   * action.executors - Counters related to actions.
      * [action_type]#action.[operation_performed] (start, end, check, kill)

   * callablequeue
      * time.in.queue: Time a callable spent in the queue before being processed.

   * commands: Generated for all Commands.
      * action.end
      * action.notification
      * action.start
      * callback
      * job.info
      * job.notification
      * purge
      * signal
      * start
      * submit

   * db - Timers related to various database operations.
      * create-workflow
      * load-action
      * load-pending-actions
      * load-running-actions
      * load-workflow
      * load-workflows
      * purge-old-workflows
      * save-action
      * update-action
      * update-workflow

   * webservices
      * admin
      * admin-GET
      * callback
      * callback-GET
      * jobs
      * jobs-GET
      * jobs-POST
      * version
      * version-GET

## Oozie JVM Thread Dump
The `admin/jvminfo.jsp` servlet can be used to get some basic jvm stats and thread dump.
For eg: `http://localhost:11000/oozie/admin/jvminfo.jsp?cpuwatch=1000&threadsort=cpu`. It takes the following optional
query parameters:

   * threadsort - The order in which the threads are sorted for display. Valid values are name, cpu, state. Default is state.
   * cpuwatch - Time interval in milliseconds to monitor cpu usage of threads. Default value is 0.

## Monitoring Database Schema Integrity

Oozie stores all of its state in a database.  Hence, ensuring that the database schema is correct is very important to ensuring that
Oozie is healthy and behaves correctly.  To help with this, Oozie includes a `SchemaCheckerService` which periodically runs and
performs a series of checks on the database schema.  More specifically, it checks the following:

   * Existence of the required tables
   * Existence of the required columns in each table
   * Each column has the correct type and default value
   * Existence of the required primary keys and indexes

After each run, the `SchemaCheckerService` writes the result of the checks to the Oozie log and to the "schema-checker.status"
instrumentation variable.  If there's a problem, it will be logged at the ERROR level, while correct checks are logged at the DEBUG
level.

By default, the `SchemaCheckerService` runs every 7 days.  This can be configured
by `oozie.service.SchemaCheckerService.check.interval`

By default, the `SchemaCheckerService` will consider "extra" tables, columns, and indexes to be incorrect. Advanced users who have
added additional tables, columns, and indexes can tell Oozie to ignore these by
setting `oozie.service.SchemaCheckerService.ignore.extras` to `false`.

The `SchemaCheckerService` currently only supports MySQL, PostgreSQL, and Oracle databases.  SQL Server and Derby are currently not
supported.

When Oozie HA is enabled, only one of the Oozie servers will perform the checks.

[::Go back to Oozie Documentation Index::](index.html)


