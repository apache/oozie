/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.client.rest;

/**
 * Json element tags used by client beans.
 */
public interface JsonTags {

    String OOZIE_SAFE_MODE = "safeMode"; //Applicable for V0 only
    String OOZIE_SYSTEM_MODE = "systemMode";
    String BUILD_INFO = "buildInfo";
    String BUILD_VERSION = "buildVersion";
    String QUEUE_DUMP = "queueDump";
    String CALLABLE_DUMP = "callableDump";
    String UNIQUE_MAP_DUMP = "uniqueMapDump";
    String UNIQUE_ENTRY_DUMP = "uniqueEntryDump";

    String SHARELIB_LIB_UPDATE = "sharelibUpdate";
    String SHARELIB_LIB = "sharelib";
    String SHARELIB_LIB_NAME = "name";
    String SHARELIB_LIB_FILES = "files";
    String SHARELIB_UPDATE_HOST = "host";
    String SHARELIB_UPDATE_STATUS = "status";

    String JOB_ID = "id";

    String JOB_IDS = "ids";

    String WORKFLOW_APP_PATH = "appPath";
    String WORKFLOW_APP_NAME = "appName";
    String WORKFLOW_ID = "id";
    String WORKFLOW_EXTERNAL_ID = "externalId";
    String WORKFLOW_PARENT_ID = "parentId";
    String WORKFLOW_CONF = "conf";
    String WORKFLOW_STATUS = "status";
    String WORKFLOW_LAST_MOD_TIME = "lastModTime";
    String WORKFLOW_CREATED_TIME = "createdTime";
    String WORKFLOW_START_TIME = "startTime";
    String WORKFLOW_END_TIME = "endTime";
    String WORKFLOW_USER = "user";
    @Deprecated
    String WORKFLOW_GROUP = "group";
    String WORKFLOW_ACL = "acl";
    String WORKFLOW_RUN = "run";
    String WORKFLOW_CONSOLE_URL = "consoleUrl";
    String WORKFLOW_ACTIONS = "actions";

    String WORKFLOWS_JOBS = "workflows";
    String WORKFLOWS_TOTAL = "total";
    String WORKFLOWS_OFFSET = "offset";
    String WORKFLOWS_LEN = "len";

    String WORKFLOW_ACTION_ID = "id";
    String WORKFLOW_ACTION_NAME = "name";
    String WORKFLOW_ACTION_AUTH = "cred";
    String WORKFLOW_ACTION_TYPE = "type";
    String WORKFLOW_ACTION_CONF = "conf";
    String WORKFLOW_ACTION_RETRIES = "retries";
    String WORKFLOW_ACTION_START_TIME = "startTime";
    String WORKFLOW_ACTION_END_TIME = "endTime";
    String WORKFLOW_ACTION_STATUS = "status";
    String WORKFLOW_ACTION_TRANSITION = "transition";
    String WORKFLOW_ACTION_DATA = "data";
    String WORKFLOW_ACTION_STATS = "stats";
    String WORKFLOW_ACTION_EXTERNAL_CHILD_IDS = "externalChildIDs";
    String WORKFLOW_ACTION_EXTERNAL_ID = "externalId";
    String WORKFLOW_ACTION_EXTERNAL_STATUS = "externalStatus";
    String WORKFLOW_ACTION_TRACKER_URI = "trackerUri";
    String WORKFLOW_ACTION_CONSOLE_URL = "consoleUrl";
    String WORKFLOW_ACTION_ERROR_CODE = "errorCode";
    String WORKFLOW_ACTION_ERROR_MESSAGE = "errorMessage";
    String WORKFLOW_ACTION_USER_RETRY_INTERVAL = "userRetryInterval";
    String WORKFLOW_ACTION_USER_RETRY_COUNT = "userRetryCount";
    String WORKFLOW_ACTION_USER_RETRY_MAX = "userRetryMax";
    String WORKFLOW_ACTION_CRED = "cred";

    String COORDINATOR_JOB_ID = "coordJobId";
    String COORDINATOR_JOB_NAME = "coordJobName";
    String COORDINATOR_JOB_PATH = "coordJobPath";
    String COORDINATOR_JOB_FREQUENCY = "frequency";
    String COORDINATOR_JOB_TIMEUNIT = "timeUnit";
    String COORDINATOR_JOB_TIMEZONE = "timeZone";
    String COORDINATOR_JOB_CONCURRENCY = "concurrency";
    String COORDINATOR_JOB_MAT_THROTTLING = "mat_throttling";
    String COORDINATOR_JOB_EXECUTION = "execution";
    String COORDINATOR_JOB_TIMEOUT = "timeOut";
    String COORDINATOR_JOB_LAST_ACTION_TIME = "lastAction";
    String COORDINATOR_JOB_NEXT_MATERIALIZED_TIME = "nextMaterializedTime";
    String COORDINATOR_JOB_CONF = "conf";
    String COORDINATOR_JOB_STATUS = "status";
    String COORDINATOR_JOB_EXECUTIONPOLICY = "executionPolicy";
    String COORDINATOR_JOB_CREATED_TIME = "createdTime";
    String COORDINATOR_JOB_START_TIME = "startTime";
    String COORDINATOR_JOB_END_TIME = "endTime";
    String COORDINATOR_JOB_PAUSE_TIME = "pauseTime";
    String COORDINATOR_JOB_CONSOLE_URL = "consoleUrl";
    String COORDINATOR_JOB_ACTIONS = "actions";
    String COORDINATOR_JOB_USER = "user";
    String COORDINATOR_JOB_NUM_ACTION = "total";

    @Deprecated
    String COORDINATOR_JOB_GROUP = "group";
    String COORDINATOR_JOB_ACL = "acl";
    String COORDINATOR_JOB_EXTERNAL_ID = "coordExternalId";

    String COORDINATOR_ACTION_ID = "id";
    String COORDINATOR_ACTION_NAME = "name";
    String COORDINATOR_ACTION_TYPE = "type";
    String COORDINATOR_ACTION_CREATED_CONF = "createdConf";
    String COORDINATOR_ACTION_RUNTIME_CONF = "runConf";
    String COORDINATOR_ACTION_NUMBER = "actionNumber";
    String COORDINATOR_ACTION_CREATED_TIME = "createdTime";
    String COORDINATOR_ACTION_EXTERNALID = "externalId";
    String COORDINATOR_JOB_BUNDLE_ID = "bundleId";
    String COORDINATOR_ACTION_LAST_MODIFIED_TIME = "lastModifiedTime";
    String COORDINATOR_ACTION_NOMINAL_TIME = "nominalTime";
    String COORDINATOR_ACTION_STATUS = "status";
    String COORDINATOR_ACTION_MISSING_DEPS = "missingDependencies";
    String COORDINATOR_ACTION_PUSH_MISSING_DEPS = "pushMissingDependencies";
    String COORDINATOR_ACTION_EXTERNAL_STATUS = "externalStatus";
    String COORDINATOR_ACTION_TRACKER_URI = "trackerUri";
    String COORDINATOR_ACTION_CONSOLE_URL = "consoleUrl";
    String COORDINATOR_ACTION_ERROR_CODE = "errorCode";
    String COORDINATOR_ACTION_ERROR_MESSAGE = "errorMessage";
    String COORDINATOR_ACTIONS = "actions";
    String COORDINATOR_ACTION_DATA = "data";
    String COORDINATOR_JOB_DATA = "data";
    String COORDINATOR_ACTION_DATASETS = "dataSets";
    String COORDINATOR_ACTION_DATASET = "dataSet";

    String COORDINATOR_WF_ACTION_NUMBER = "actionNumber";
    String COORDINATOR_WF_ACTION = "action";
    String COORDINATOR_WF_ACTION_NULL_REASON = "nullReason";
    String COORDINATOR_WF_ACTIONS = "actions";

    String BUNDLE_JOB_ID = "bundleJobId";
    String BUNDLE_JOB_NAME = "bundleJobName";
    String BUNDLE_JOB_PATH = "bundleJobPath";
    String BUNDLE_JOB_TIMEUNIT = "timeUnit";
    String BUNDLE_JOB_TIMEOUT = "timeOut";
    String BUNDLE_JOB_CONF = "conf";
    String BUNDLE_JOB_STATUS = "status";
    String BUNDLE_JOB_KICKOFF_TIME = "kickoffTime";
    String BUNDLE_JOB_START_TIME = "startTime";
    String BUNDLE_JOB_END_TIME = "endTime";
    String BUNDLE_JOB_PAUSE_TIME = "pauseTime";
    String BUNDLE_JOB_CREATED_TIME = "createdTime";
    String BUNDLE_JOB_CONSOLE_URL = "consoleUrl";
    String BUNDLE_JOB_USER = "user";
    @Deprecated
    String BUNDLE_JOB_GROUP = "group";
    String BUNDLE_JOB_ACL = "acl";
    String BUNDLE_JOB_EXTERNAL_ID = "bundleExternalId";
    String BUNDLE_COORDINATOR_JOBS = "bundleCoordJobs";

    String SLA_SUMMARY_LIST = "slaSummaryList";
    String SLA_SUMMARY_ID = "id";
    String SLA_SUMMARY_PARENT_ID = "parentId";
    String SLA_SUMMARY_APP_NAME = "appName";
    String SLA_SUMMARY_APP_TYPE = "appType";
    String SLA_SUMMARY_USER = "user";
    String SLA_SUMMARY_NOMINAL_TIME = "nominalTime";
    String SLA_SUMMARY_EXPECTED_START = "expectedStart";
    String SLA_SUMMARY_ACTUAL_START = "actualStart";
    String SLA_SUMMARY_START_DELAY = "startDelay";
    String SLA_SUMMARY_EXPECTED_END = "expectedEnd";
    String SLA_SUMMARY_ACTUAL_END = "actualEnd";
    String SLA_SUMMARY_END_DELAY = "endDelay";
    String SLA_SUMMARY_EXPECTED_DURATION = "expectedDuration";
    String SLA_SUMMARY_ACTUAL_DURATION = "actualDuration";
    String SLA_SUMMARY_DURATION_DELAY = "durationDelay";
    String SLA_SUMMARY_JOB_STATUS = "jobStatus";
    String SLA_SUMMARY_SLA_STATUS = "slaStatus";
    String SLA_SUMMARY_EVENT_STATUS = "eventStatus";
    String SLA_SUMMARY_LAST_MODIFIED = "lastModified";
    String SLA_ALERT_STATUS = "slaAlertStatus";


    String TO_STRING = "toString";


    String ERROR = "error";
    String ERROR_CODE = "code";
    String ERROR_MESSAGE = "message";

    String HTTP_STATUS_CODE = "httpStatusCode";

    String INSTR_TIMERS = "timers";
    String INSTR_VARIABLES = "variables";
    String INSTR_SAMPLERS = "samplers";
    String INSTR_COUNTERS = "counters";
    String INSTR_DATA = "data";

    String INSTR_GROUP = "group";
    String INSTR_NAME = "name";

    String INSTR_TIMER_OWN_TIME_AVG = "ownTimeAvg";
    String INSTR_TIMER_TOTAL_TIME_AVG = "totalTimeAvg";
    String INSTR_TIMER_TICKS = "ticks";
    String INSTR_TIMER_OWN_STD_DEV = "ownTimeStdDev";
    String INSTR_TIMER_TOTAL_STD_DEV = "totalTimeStdDev";
    String INSTR_TIMER_OWN_MIN_TIME = "ownMinTime";
    String INSTR_TIMER_OWN_MAX_TIME = "ownMaxTime";
    String INSTR_TIMER_TOTAL_MIN_TIME = "totalMinTime";
    String INSTR_TIMER_TOTAL_MAX_TIME = "totalMaxTime";

    String INSTR_VARIABLE_VALUE = "value";
    String INSTR_SAMPLER_VALUE = "value";

    Object COORDINATOR_JOBS = "coordinatorjobs";
    Object COORD_JOB_TOTAL = "total";
    Object COORD_JOB_OFFSET = "offset";
    Object COORD_JOB_LEN = "len";

    Object BUNDLE_JOBS = "bundlejobs";
    Object BUNDLE_JOB_TOTAL = "total";
    Object BUNDLE_JOB_OFFSET = "offset";
    Object BUNDLE_JOB_LEN = "len";

    String BULK_RESPONSE_BUNDLE = "bulkbundle";
    String BULK_RESPONSE_COORDINATOR = "bulkcoord";
    String BULK_RESPONSE_ACTION = "bulkaction";
    Object BULK_RESPONSES = "bulkresponses";
    Object BULK_RESPONSE_TOTAL = "total";
    Object BULK_RESPONSE_OFFSET = "offset";
    Object BULK_RESPONSE_LEN = "len";

    String AVAILABLE_TIME_ZONES = "available-timezones";
    String TIME_ZOME_DISPLAY_NAME = "timezoneDisplayName";
    String TIME_ZONE_ID = "timezoneId";

    String JMS_TOPIC_PATTERN = "jmsTopicPattern";
    String JMS_JNDI_PROPERTIES = "jmsJNDIProps";
    String JMS_TOPIC_PREFIX = "jmsTopicPrefix";

    String JMS_TOPIC_NAME = "jmsTopicName";
    String COORD_UPDATE = RestConstants.JOB_COORD_UPDATE;
    String COORD_UPDATE_DIFF = "diff";

    String STATUS = "status";
    String ACTION_ATTEMPT = "attempt";
    String VALIDATE = "validate";
    String COORD_ACTION_MISSING_DEPENDENCIES = "missingDependencies";
    String COORD_ACTION_FIRST_MISSING_DEPENDENCIES = "blockedOn";


    String PURGE = "purge";
}
