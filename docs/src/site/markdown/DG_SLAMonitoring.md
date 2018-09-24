

[::Go back to Oozie Documentation Index::](index.html)

# Oozie SLA Monitoring

<!-- MACRO{toc|fromDepth=1|toDepth=4} -->

## Overview

Critical jobs can have certain SLA requirements associated with them. This SLA can be in terms of time
i.e. a maximum allowed time limit associated with when the job should start, by when should it end,
and its duration of run. Oozie workflows and coordinators allow defining such SLA limits in the application definition xml.

With the addition of SLA Monitoring, Oozie can now actively monitor the state of these SLA-sensitive jobs
and send out notifications for SLA mets and misses.

In versions earlier than 4.x, this was a passive feature where users needed to query the Oozie client SLA API
to fetch the records regarding job status changes, and use their own custom calculation engine to compute
whether SLA was met or missed, based on initial definition of time limits.

Oozie now also has a SLA tab in the Oozie UI, where users can query for SLA information and have a summarized view
of how their jobs fared against their SLAs.


## Oozie Server Configuration

Refer to [Notifications Configuration](AG_Install.html#Notifications_Configuration) for configuring Oozie server to track
SLA for jobs and send notifications.

## SLA Tracking

Oozie allows tracking SLA for meeting the following criteria:

   * Start time
   * End time
   * Job Duration

### Event Status
Corresponding to each of these 3 criteria, your jobs are processed for whether Met or Miss i.e.

   * START_MET, START_MISS
   * END_MET, END_MISS
   * DURATION_MET, DURATION_MISS

### SLA Status
Expected end-time is the most important criterion for majority of users while deciding overall SLA Met or Miss.
Hence the _"SLA_Status"_ for a job will transition through these four stages

   * Not_Started <-- Job not yet begun
   * In_Process <-- Job started and is running, and SLAs are being tracked
   * Met <-- caused by an END_MET
   * Miss <-- caused by an END_MISS

In addition to overshooting expected end-time, and END_MISS (and so an eventual SLA MISS) also occurs when the
job does not end successfully e.g. goes to error state - Failed/Killed/Error/Timedout.

## Configuring SLA in Applications

To make your jobs trackable for SLA, you simply need to add the `<sla:info>` tag to your workflow application definition.
If you were already using the existing SLA schema
in your workflows (Schema xmlns:sla="uri:oozie:sla:0.1"), you don't need to
do anything extra to receive SLA notifications via JMS messages. This new SLA monitoring framework is backward-compatible -
no need to change application XML for now and you can continue to fetch old records via the [command line API](DG_CommandLineTool.html#SLAOperations).
However, usage of old schema
and API is deprecated and we strongly recommend using new schema.

   * New SLA schema
is 'uri:oozie:sla:0.2'
   * In order to use new SLA schema,
 you will need to upgrade your workflow/coordinator schema
to 0.5 i.e. 'uri:oozie:workflow:0.5'

### SLA Definition in Workflow
Example:

```
<workflow-app name="test-wf-job-sla"
              xmlns="uri:oozie:workflow:0.5"
              xmlns:sla="uri:oozie:sla:0.2">
    <start to="grouper"/>
    <action name="grouper">
        <map-reduce>
            <job-tracker>jt</job-tracker>
            <name-node>nn</name-node>
            <configuration>
                <property>
                    <name>mapred.input.dir</name>
                    <value>input</value>
                </property>
                <property>
                    <name>mapred.output.dir</name>
                    <value>output</value>
                </property>
            </configuration>
        </map-reduce>
        <ok to="end"/>
        <error to="end"/>
    </action>
    <end name="end"/>
    <sla:info>
        <sla:nominal-time>${nominal_time}</sla:nominal-time>
        <sla:should-start>${10 * MINUTES}</sla:should-start>
        <sla:should-end>${30 * MINUTES}</sla:should-end>
        <sla:max-duration>${30 * MINUTES}</sla:max-duration>
        <sla:alert-events>start_miss,end_miss,duration_miss</sla:alert-events>
        <sla:alert-contact>joe@example.com</sla:alert-contact>
    </sla:info>
</workflow-app>
```

For the list of tags usable under `<sla:info>`, refer to [Schemas Appendix](WorkflowFunctionalSpec.html#SLASchema).
This new schema
is much more compact and meaningful, getting rid of redundant and unused tags.

   * `nominal-time`: As the name suggests, this is the time relative to which your jobs' SLAs will be calculated. Generally since Oozie workflows are aligned with synchronous data dependencies, this nominal time can be parameterized to be passed the value of your coordinator nominal time. Nominal time is also required in case of independent workflows and you can specify the time in which you expect the workflow to be run if you don't have a synchronous dataset associated with it.
   * `should-start`: Relative to `nominal-time` this is the amount of time (along with time-unit - MINUTES, HOURS, DAYS) within which your job should *start running* to meet SLA. This is optional.
   * `should-end`: Relative to `nominal-time` this is the amount of time (along with time-unit - MINUTES, HOURS, DAYS) within which your job should *finish* to meet SLA.
   * `max-duration`: This is the maximum amount of time (along with time-unit - MINUTES, HOURS, DAYS) your job is expected to run. This is optional.
   * `alert-events`: Specify the types of events for which **Email** alerts should be sent. Allowable values in this comma-separated list are start_miss, end_miss and duration_miss. *_met events can generally be deemed low priority and hence email alerting for these is not necessary. However, note that this setting is only for alerts via *email* alerts and not via JMS messages, where all events send out notifications, and user can filter them using desired selectors. This is optional and only applicable when alert-contact is configured.
   * `alert-contact`: Specify a comma separated list of email addresses where you wish your alerts to be sent. This is optional and need not be configured if you just want to view your job SLA history in the UI and do not want to receive email alerts.

NOTE: All tags can be parameterized as a EL function or a fixed value.

Same schema
can be applied to and embedded under Workflow-Action as well as Coordinator-Action XML.

### SLA Definition in Workflow Action


```
<workflow-app name="test-wf-action-sla" xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2">
    <start to="grouper"/>
    <action name="grouper">
        ...
        <ok to="end"/>
        <error to="end"/>
        <sla:info>
            <sla:nominal-time>${nominal_time}</sla:nominal-time>
            <sla:should-start>${10 * MINUTES}</sla:should-start>
        ...
        </sla:info>
    </action>
    <end name="end"/>
</workflow-app>
```

### SLA Definition in Coordinator Action

```
<coordinator-app name="test-coord-sla" frequency="${coord:days(1)}" freq_timeunit="DAY"
    end_of_duration="NONE" start="2013-06-20T08:01Z" end="2013-12-01T08:01Z"
    timezone="America/Los_Angeles" xmlns="uri:oozie:coordinator:0.4" xmlns:sla="uri:oozie:sla:0.2">
    <action>
        <workflow>
            <app-path>${wfAppPath}</app-path>
        </workflow>
        <sla:info>
            <sla:nominal-time>${nominal_time}</sla:nominal-time>
            ...
        </sla:info>
    </action>
</coordinator-app>
```

## Accessing SLA Information

SLA information is accessible via the following ways:

   * Through the SLA tab of the Oozie Web UI.
   * JMS messages sent to a configured JMS provider for instantaneous tracking.
   * RESTful API to query for SLA summary.
   * As an `Instrumentation.Counter` entry that is accessible via RESTful API and reflects to the number of all SLA tracked external
   entities. Name of this counter is `sla-calculator.sla-map`.

For JMS Notifications, you have to have a message broker in place, on which Oozie publishes messages and you can
hook on a subscriber to receive those messages. For more info on setting up and consuming JMS messages, refer
[JMS Notifications](DG_JMSNotifications.html) documentation.

In the REST API, the following filters can be applied while fetching SLA information:

   * `app_name` - Application name
   * `id`  - id of the workflow job, workflow action or coordinator action
   * `parent_id` - Parent id of the workflow job, workflow action or coordinator action
   * `nominal_after` and `nominal_before` - Start and End range for nominal time of the workflow or coordinator.
   * `bundle` -  Bundle Job ID or Bundle App Name. Fetches SLA information for actions of all coordinators in that bundle.
   * `event_status` - event status such as START_MET/START_MISS/DURATION_MET/DURATION_MISS/END_MET/END_MISS
   * `sla_status` - sla status such as NOT_STARTED/IN_PROCESS/MET/MISS
   * `job_status` - job status such as CREATED/STARTED/SUCCEEDED/KILLED/FAILED
   * `app_type` - application type such as COORDINATOR_ACTION/COORDINATOR_JOB/WORKFLOW_JOB/WORKFLOW_ACTION
   * `user_name` - the username of the user who submitted the job
   * `created_after` and `created_before` - Start and End range for created time of the workflow or coordinator.
   * `expectedstart_after` and `expectedstart_before` - Start and End range for expected start time of the workflow or coordinator.
   * `expectedend_after` and `expectedend_before` - Start and End range for expected end time of the workflow or coordinator.
   * `actualstart_after` and `actualstart_before` - Start and End range for actual start time of the workflow or coordinator.
   * `actualend_after` and `actualend_before` - Start and End range for actual end time of the workflow or coordinator.
   * `actual_duration_min` and `actual_duration_max` - Min and Max range for actual duration (in milliseconds)
   * `expected_duration_min` and `expected_duration_max` - Min and Max range for expected duration (in milliseconds)

It is possible to specify multiple filter conditions with semicolon separation, only information meeting all the conditions will be
fetched.

Multiple `event_status` and `sla_status` can be specified with comma separation.
When multiple statuses are specified, they are considered as OR.
For example, `event_status=START_MET,END_MISS` list the coordinator actions where event status is either `START_MET` OR `END_MISS`.

For the `app_name`, `app_type`, `user_name`, and `job_status` filter fields two wildchars can also be used:
the percent sign ( `%` ) represents zero, one, or multiple characters, the underscore ( `_` ) character represents a single
character.

For compatibility reasons `nominal_start` and `nominal_end` filter names can also be used instead of `nominal_after`
and `nominal_before`.

When `timezone` query parameter is specified, the expected and actual start/end time returned is formatted. If not specified,
the number of milliseconds that have elapsed since January 1, 1970 00:00:00.000 GMT is returned.

It is possible to specify the ordering of the list by using the `sortby` and the `order` parameters. The possible values for the
`sortby` parameter are: actualDuration, actualEndTS, actualStartTS, appName, appType, createdTimeTS, eventProcessed, eventStatus,
expectedDuration, expectedEndTS, expectedStartTS, jobId, jobStatus, lastModifiedTS, nominalTimeTS, parentId, slaStatus, and user.
The possible value for the `order` parameter are: desc and asc. The default value for the `sortby` parameter is nominalTimeTS, the
default value for the `order` parameter is asc. If several items has the same sortby column values the these items will be sorted
by the ascending order of the nominalTimeTS field.

The examples below demonstrate the use of REST API and explains the JSON response.

### Scenario 1: Workflow Job Start_Miss
**Request:**

```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=nominal_after=2013-06-18T00:01Z;nominal_before=2013-06-23T00:01Z;app_name=my-sla-app
```

**JSON Response**

```
{

    id : "000056-1238791320234-oozie-joe-W"
    parentId : "000001-1238791320234-oozie-joe-C@8"
    appType : "WORKFLOW_JOB"
    msgType : "SLA"
    appName : "my-sla-app"
    slaStatus : "IN_PROCESS"
    jobStatus : "RUNNING"
    user: "joe"
    nominalTime: "2013-16-22T05:00Z"
    expectedStartTime: "2013-16-22T05:10Z" <-- (should start by this time)
    actualStartTime: "2013-16-22T05:30Z" <-- (20 min late relative to expected start)
    expectedEndTime: "2013-16-22T05:40Z" <-- (should end by this time)
    actualEndTime: null
    expectedDuration: 900000 <-- (expected duration in milliseconds)
    actualDuration: 120000 <-- (actual duration in milliseconds)
    notificationMessage: "My Job has encountered an SLA event!"
    upstreamApps: "dependent-app-1, dependent-app-2"

}
```

### Scenario 2: Workflow Action End_Miss
**Request:**

```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=parent_id=000056-1238791320234-oozie-joe-W
```

**JSON Response**

```
{

    id : "000056-1238791320234-oozie-joe-W@map-reduce-action"
    parentId : "000056-1238791320234-oozie-joe-W"
    appType : "WORKFLOW_ACTION"
    msgType : "SLA"
    appName : "map-reduce-action"
    slaStatus : "MISS"
    jobStatus : "SUCCEEDED"
    user: "joe"
    nominalTime: "2013-16-22T05:00Z"
    expectedStartTime: "2013-16-22T05:10Z"
    actualStartTime: "2013-16-22T05:05Z"
    expectedEndTime: "2013-16-22T05:40Z" <-- (should end by this time)
    actualEndTime: "2013-16-22T06:00Z" <-- (20 min late relative to expected end)
    expectedDuration: 3600000 <-- (expected duration in milliseconds)
    actualDuration: 3300000 <-- (actual duration in milliseconds)
    notificationMessage: "My Job has encountered an SLA event!"
    upstreamApps: "dependent-app-1, dependent-app-2"

}
```

### Scenario 3: Coordinator Action Duration_Miss
**Request:**

```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=id=000001-1238791320234-oozie-joe-C
```

**JSON Response**

```
{

    id : "000001-1238791320234-oozie-joe-C@2"
    parentId : "000001-1238791320234-oozie-joe-C"
    appType : "COORDINATOR_ACTION"
    msgType : "SLA"
    appName : "my-coord-app"
    slaStatus : "MET"
    jobStatus : "SUCCEEDED"
    user: "joe"
    nominalTime: "2013-16-22T05:00Z"
    expectedStartTime: "2013-16-22T05:10Z"
    actualStartTime: "2013-16-22T05:05Z"
    expectedEndTime: "2013-16-22T05:40Z"
    actualEndTime: "2013-16-22T05:30Z"
    expectedDuration: 900000 <-- (expected duration in milliseconds)
    actualDuration: 1500000 <- (actual duration in milliseconds)
    notificationMessage: "My Job has encountered an SLA event!"
    upstreamApps: "dependent-app-1, dependent-app-2"

}
```

Scenario #3 is particularly interesting because it is an overall "MET" because it met its expected End-time,
but it is "Duration_Miss" because the actual run (between actual start and actual end) exceeded expected duration.

### Scenario 4: All Coordinator actions in a Bundle
**Request:**

```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=bundle=1234567-150130225116604-oozie-B;event_status=END_MISS
```

**JSON Response**

```
{
    id : "000001-1238791320234-oozie-joe-C@1"
    parentId : "000001-1238791320234-oozie-joe-C"
    appType : "COORDINATOR_ACTION"
    msgType : "SLA"
    appName : "my-coord-app"
    slaStatus : "MET"
    eventStatus : "START_MET,DURATION_MISS,END_MISS"
    user: "joe"
    nominalTime: "2014-01-10T12:00Z"
    expectedStartTime: "2014-01-10T12:00Z"
    actualStartTime: "2014-01-10T11:59Z"
    startDelay: -1
    expectedEndTime: "2014-01-10T13:00Z"
    actualEndTime: "2014-01-10T13:05Z"
    endDelay: 5
    expectedDuration: 3600000 <-- (expected duration in milliseconds)
    actualDuration: 3960000 <-- (actual duration in milliseconds)
    durationDelay: 6 <-- (duration delay in minutes)
}
{
    id : "000001-1238791320234-oozie-joe-C@2"
    parentId : "000001-1238791320234-oozie-joe-C"
    appType : "COORDINATOR_ACTION"
    msgType : "SLA"
    appName : "my-coord-app"
    slaStatus : "MET"
    eventStatus : "START_MISS,DURATION_MET,END_MISS"
    user: "joe"
    nominalTime: "2014-01-11T12:00Z"
    expectedStartTime: "2014-01-11T12:00Z"
    actualStartTime: "2014-01-11T12:05Z"
    startDelay: 5
    expectedEndTime: "2014-01-11T13:00Z"
    actualEndTime: "2014-01-11T13:01Z"
    endDelay: 1
    expectedDuration: 3600000 <-- (expected duration in milliseconds)
    actualDuration: 3360000 <-- (actual duration in milliseconds)
    durationDelay: -4 <-- (duration delay in minutes)
}
```

Scenario #4 (All Coordinator actions in a Bundle) is to get SLA information of all coordinator actions under bundle job in one call.
startDelay/durationDelay/endDelay values returned indicate how much delay compared to expected time (positive values in case of MISS, and negative values in case of MET).

### Scenario 5: Workflow jobs actually started in a 24 hour period
*Request:*
```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=app_type=WORKFLOW_JOB;actualstart_after=2018-08-13T00:01Z;actualstart_before=2018-08-14T00:01Z
```

*JSON Response*
```
{
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "one-op-wf",
      "actualEnd": "Mon, 13 Aug 2018 14:49:21 GMT",
      "actualDuration": 503,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "DURATION_MET,END_MISS,START_MISS",
      "startDelay": 4531068,
      "id": "0000001-180813160322492-oozie-test-W",
      "lastModified": "Mon, 13 Aug 2018 14:49:31 GMT",
      "user": "testuser",
      "actualStart": "Mon, 13 Aug 2018 14:49:20 GMT",
      "endDelay": 4531059
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 02:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 02:10:00 GMT",
      "appName": "one-op-wf",
      "actualEnd": "Mon, 13 Aug 2018 14:49:21 GMT",
      "actualDuration": 222,
      "expectedStart": "Fri, 01 Jan 2010 02:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "DURATION_MET,END_MISS,START_MISS",
      "startDelay": 4531008,
      "id": "0000002-180813160322492-oozie-test-W",
      "lastModified": "Mon, 13 Aug 2018 14:49:41 GMT",
      "user": "testuser",
      "actualStart": "Mon, 13 Aug 2018 14:49:21 GMT",
      "endDelay": 4530999
    }
```

Scenario #5 is to get SLA information of all workflow jobs by filtering for the actual start date
instead of the nominal start date.

### Scenario 6: Workflow jobs executed much faster than required
*Request:*
```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=app_type=WORKFLOW_JOB;expected_duration_min=10000;actual_duration_max=1000
```

*JSON Response*
```
{
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "one-op-wf",
      "actualEnd": "Mon, 13 Aug 2018 14:49:21 GMT",
      "actualDuration": 503,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "DURATION_MET,END_MISS,START_MISS",
      "startDelay": 4531068,
      "id": "0000001-180813160322492-oozie-test-W",
      "lastModified": "Mon, 13 Aug 2018 14:49:31 GMT",
      "user": "testuser",
      "actualStart": "Mon, 13 Aug 2018 14:49:20 GMT",
      "endDelay": 4531059
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 02:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 02:10:00 GMT",
      "appName": "one-op-wf",
      "actualEnd": "Mon, 13 Aug 2018 14:49:21 GMT",
      "actualDuration": 222,
      "expectedStart": "Fri, 01 Jan 2010 02:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "DURATION_MET,END_MISS,START_MISS",
      "startDelay": 4531008,
      "id": "0000002-180813160322492-oozie-test-W",
      "lastModified": "Mon, 13 Aug 2018 14:49:41 GMT",
      "user": "testuser",
      "actualStart": "Mon, 13 Aug 2018 14:49:21 GMT",
      "endDelay": 4530999
    }
```

Scenario #6 is to get SLA information of all workflow jobs where the expected duration was more than 10 seconds (10000ms),
but the actual duration was less than a second (1000ms).

### Scenario 7: Coordinator actions with START_MET or END_MET event status

*Request:*
```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=app_type=COORDINATOR_ACTION;event_status=START_MET,END_MET
```

*JSON Response*
```
    {
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "aggregator-coord",
      "actualEnd": "Wed, 29 Aug 2018 10:29:59 GMT",
      "actualDuration": 167,
      "expectedStart": "Tue, 18 Feb 2200 11:41:46 GMT",
      "expectedDuration": 60000,
      "parentId": "0000006-180829120813646-oozie-test-C",
      "durationDelay": 0,
      "slaStatus": "MISS",
      "appType": "COORDINATOR_ACTION",
      "slaAlertStatus": "Disabled",
      "eventStatus": "START_MET,DURATION_MET,END_MISS",
      "startDelay": -95446151,
      "id": "0000006-180829120813646-oozie-test-C@1",
      "lastModified": "Wed, 29 Aug 2018 10:30:07 GMT",
      "user": "testuser",
      "actualStart": "Wed, 29 Aug 2018 10:29:59 GMT",
      "endDelay": 4553839
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 05 Jan 2029 11:39:31 GMT",
      "appName": "aggregator-coord",
      "actualEnd": "Wed, 29 Aug 2018 10:15:48 GMT",
      "actualDuration": 394,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 60000,
      "parentId": "0000000-180829120813646-oozie-test-C",
      "durationDelay": 0,
      "slaStatus": "MET",
      "appType": "COORDINATOR_ACTION",
      "slaAlertStatus": "Disabled",
      "eventStatus": "START_MISS,DURATION_MET,END_MET",
      "startDelay": 4553834,
      "id": "0000000-180829120813646-oozie-test-C@1",
      "lastModified": "Wed, 29 Aug 2018 10:15:57 GMT",
      "user": "testuser",
      "actualStart": "Wed, 29 Aug 2018 10:15:48 GMT",
      "endDelay": -5446163
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 02:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 02:10:00 GMT",
      "appName": "aggregator-coord",
      "actualEnd": "Wed, 29 Aug 2018 10:29:59 GMT",
      "actualDuration": 172,
      "expectedStart": "Tue, 18 Feb 2200 12:41:46 GMT",
      "expectedDuration": 60000,
      "parentId": "0000006-180829120813646-oozie-test-C",
      "durationDelay": 0,
      "slaStatus": "MISS",
      "appType": "COORDINATOR_ACTION",
      "slaAlertStatus": "Disabled",
      "eventStatus": "START_MET,DURATION_MET,END_MISS",
      "startDelay": -95446211,
      "id": "0000006-180829120813646-oozie-test-C@2",
      "lastModified": "Wed, 29 Aug 2018 10:30:17 GMT",
      "user": "testuser",
      "actualStart": "Wed, 29 Aug 2018 10:29:59 GMT",
      "endDelay": 4553779
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 02:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 05 Jan 2029 12:39:31 GMT",
      "appName": "aggregator-coord",
      "actualEnd": "Wed, 29 Aug 2018 10:15:48 GMT",
      "actualDuration": 208,
      "expectedStart": "Fri, 01 Jan 2010 02:01:00 GMT",
      "expectedDuration": 60000,
      "parentId": "0000000-180829120813646-oozie-test-C",
      "durationDelay": 0,
      "slaStatus": "MET",
      "appType": "COORDINATOR_ACTION",
      "slaAlertStatus": "Disabled",
      "eventStatus": "START_MISS,DURATION_MET,END_MET",
      "startDelay": 4553774,
      "id": "0000000-180829120813646-oozie-test-C@2",
      "lastModified": "Wed, 29 Aug 2018 10:16:07 GMT",
      "user": "testuser",
      "actualStart": "Wed, 29 Aug 2018 10:15:48 GMT",
      "endDelay": -5446223
    }
```

Scenario #7 shows the possibility of filtering multiple event statuses. We list two comma separated statuses
(START_MET,END_MET) and list coordinator actions with either START_MET or END_MET event status.

### Scenario 8: Not yet started workflow jobs expected to start before a specified date.

*Request:*
```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=app_type=WORKFLOW_JOB;sla_status=NOT_STARTED;expectedstart_before=2018-08-14T00:01Z
```

*JSON Response*
```
    {
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "PREP",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "one-op-wf",
      "actualEnd": null,
      "actualDuration": -1,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "slaStatus": "NOT_STARTED",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "START_MISS,END_MISS",
      "startDelay": 4561259,
      "id": "0000031-180903152228376-oozie-test-W",
      "lastModified": "Mon, 03 Sep 2018 14:00:50 GMT",
      "user": "testuser",
      "actualStart": null,
      "endDelay": 4561250
    }
```

Scenario #8 shows the possibility to list problematic jobs even before they start. It also shows the possibility to combine
several filter fields.

### Scenario 9: Filtering for app_name using % wildchar

*Request:*
```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=app_name=appname-%25
```

Note that the filter is URL encoded, its decoded value is `app_name=appname-%`

*JSON Response*
```
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "appname-2",
      "actualEnd": "Wed, 19 Sep 2018 15:02:48 GMT",
      "actualDuration": 245,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "START_MISS,END_MISS,DURATION_MET",
      "startDelay": 4584361,
      "id": "0000003-180919170132414-oozie-test-W",
      "lastModified": "Wed, 19 Sep 2018 15:02:56 GMT",
      "user": "testuser",
      "actualStart": "Wed, 19 Sep 2018 15:02:48 GMT",
      "endDelay": 4584352
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "appname-1",
      "actualEnd": "Wed, 19 Sep 2018 15:02:23 GMT",
      "actualDuration": 378,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "START_MISS,END_MISS,DURATION_MET",
      "startDelay": 4584361,
      "id": "0000001-180919170132414-oozie-test-W",
      "lastModified": "Wed, 19 Sep 2018 15:02:26 GMT",
      "user": "testuser",
      "actualStart": "Wed, 19 Sep 2018 15:02:23 GMT",
      "endDelay": 4584352
    }
```

### Scenario 9: Filtering for app_name using % wildchar and sorting the order by the application name in descending order

*Request:*
```
GET <oozie-host>:<port>/oozie/v2/sla?timezone=GMT&filter=app_name=appname-%25&sortby=appName&order=desc
```

Note that the filter is URL encoded, its decoded value is `app_name=appname-%`

*JSON Response*
```
{
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "appname-2",
      "actualEnd": "Wed, 19 Sep 2018 15:02:48 GMT",
      "actualDuration": 245,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "START_MISS,END_MISS,DURATION_MET",
      "startDelay": 4584361,
      "id": "0000003-180919170132414-oozie-test-W",
      "lastModified": "Wed, 19 Sep 2018 15:02:56 GMT",
      "user": "testuser",
      "actualStart": "Wed, 19 Sep 2018 15:02:48 GMT",
      "endDelay": 4584352
    },
    {
      "nominalTime": "Fri, 01 Jan 2010 01:00:00 GMT",
      "jobStatus": "SUCCEEDED",
      "expectedEnd": "Fri, 01 Jan 2010 01:10:00 GMT",
      "appName": "appname-1",
      "actualEnd": "Wed, 19 Sep 2018 15:02:23 GMT",
      "actualDuration": 378,
      "expectedStart": "Fri, 01 Jan 2010 01:01:00 GMT",
      "expectedDuration": 300000,
      "durationDelay": -4,
      "slaStatus": "MISS",
      "appType": "WORKFLOW_JOB",
      "slaAlertStatus": "Enabled",
      "eventStatus": "START_MISS,END_MISS,DURATION_MET",
      "startDelay": 4584361,
      "id": "0000001-180919170132414-oozie-test-W",
      "lastModified": "Wed, 19 Sep 2018 15:02:26 GMT",
      "user": "testuser",
      "actualStart": "Wed, 19 Sep 2018 15:02:23 GMT",
      "endDelay": 4584352
    }
```

### Sample Email Alert

```
Subject: OOZIE - SLA END_MISS (AppName=wf-sla-job, JobID=0000004-130610225200680-oozie-oozi-W)


Status:
  SLA Status - END_MISS
  Job Status - RUNNING
  Notification Message - Missed SLA for Data Pipeline job
Job Details:
  App Name - wf-sla-job
  App Type - WORKFLOW_JOB
  User - strat_ci
  Job ID - 0000004-130610225200680-oozie-oozi-W
  Job URL - http://host.domain.com:4080/oozie//?job=0000004-130610225200680-oozie-oozi-W
  Parent Job ID - N/A
  Parent Job URL - N/A
  Upstream Apps - wf-sla-up-app
SLA Details:
  Nominal Time - Mon Jun 10 23:33:00 UTC 2013
  Expected Start Time - Mon Jun 10 23:35:00 UTC 2013
  Actual Start Time - Mon Jun 10 23:34:04 UTC 2013
  Expected End Time - Mon Jun 10 23:38:00 UTC 2013
  Expected Duration (in mins) - 5
  Actual Duration (in mins) - -1
```

### Changing job SLA definition and alerting
Following are ways to enable/disable SLA alerts for coordinator actions.

#### 1. Specify in Bundle XML during submission.
Following properties can be specified in bundle xml as properties for coordinator.

`oozie.sla.disable.alerts.older.than` this property can be specified in hours, the SLA notification for
coord actions will be disabled whose nominal is time older then this value. Default is 48 hours.

```
<property>
    <name>oozie.sla.disable.alerts.older.than</name>
    <value>12</value>
</property>
```

`oozie.sla.disable.alerts` List of coord actions to be disabled. Value can be specified as list of coord actions or date range.

```
<property>
    <name>oozie.sla.disable.alerts</name>
    <value>1,3-4,7-10</value>
</property>
```
Will disable alert for coord actions 1,3,5,7,8,9,10

`oozie.sla.enable.alerts` List of coord actions to be enabled. Value can be specified as list of coord actions or date range.

```
<property>
    <name>oozie.sla.enable.alerts</name>
    <value>2009-01-01T01:00Z::2009-05-31T23:59Z</value>
</property>
```
This will enable SLA alert for coord actions whose nominal time is in between (inclusive) 2009-01-01T01:00Z and 2009-05-31T23:59Z.

ALL keyword can be specified to specify all actions. Below property will disable SLA notifications for all coord actions.

```
<property>
    <name>oozie.sla.disable.alerts</name>
    <value>ALL</value>
</property>
```

#### 2. Specify during Coordinator job submission or update
Above properties can be specified in job.properties in
[Coord job update command](DG_CommandLineTool.html#Updating_coordinator_definition_and_properties),
in [Coord job submit command](DG_CommandLineTool.html#Submitting_a_Workflow_Coordinator_or_Bundle_Job)
or in [Coord job run command](DG_CommandLineTool.html#Running_a_Workflow_Coordinator_or_Bundle_Job)

#### 3. Change using command line
Refer [Changing job SLA definition and alerting](DG_CommandLineTool.html#Changing_job_SLA_definition_and_alerting) for commandline usage.

#### 4. Change using REST API
Refer the REST API [Changing job SLA definition and alerting](WebServicesAPI.html#Changing_job_SLA_definition_and_alerting).

## In-memory SLA entries and database content

There are special circumstances when the in-memory `SLACalcStatus` entries can exist without the workflow or coordinator job or
action instances in database. For example:

   * SLA tracked database content may already have been deleted, and `SLA_SUMMARY` entry is not present anymore in database
   * SLA tracked database content and `SLA_SUMMARY` entry aren't yet present in database

By the time `SLAService` scheduled job will be running, SLA map contents are checked. When the `SLA_SUMMARY` entry for the in-memory
SLA entry is missing, a counter is increased. When this counter reaches the server-wide preconfigured value
`oozie.sla.service.SLAService.maximum.retry.count` (by default `3`), in-memory SLA entry will get purged.

## Known issues
There are two known issues when you define SLA for a workflow action.
   * If there are decision nodes and SLA is defined for a workflow action not in the execution path because of the decision node, you will still get an SLA_MISS notification.
   * If you have dangling action nodes in your workflow definition and SLA is defined for it, you will still get an SLA_MISS notification.

[::Go back to Oozie Documentation Index::](index.html)


