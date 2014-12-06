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

    public static final String OOZIE_SAFE_MODE = "safeMode"; //Applicable for V0 only
    public static final String OOZIE_SYSTEM_MODE = "systemMode";
    public static final String BUILD_VERSION = "buildVersion";
    public static final String QUEUE_DUMP = "queueDump";
    public static final String CALLABLE_DUMP = "callableDump";
    public static final String UNIQUE_MAP_DUMP = "uniqueMapDump";
    public static final String UNIQUE_ENTRY_DUMP = "uniqueEntryDump";

    public static final String SHARELIB_LIB_UPDATE = "sharelibUpdate";
    public static final String SHARELIB_LIB = "sharelib";
    public static final String SHARELIB_LIB_NAME = "name";
    public static final String SHARELIB_LIB_FILES = "files";
    public static final String SHARELIB_UPDATE_HOST = "host";
    public static final String SHARELIB_UPDATE_STATUS = "status";

    public static final String JOB_ID = "id";

    public static final String WORKFLOW_APP_PATH = "appPath";
    public static final String WORKFLOW_APP_NAME = "appName";
    public static final String WORKFLOW_ID = "id";
    public static final String WORKFLOW_EXTERNAL_ID = "externalId";
    public static final String WORKFLOW_PARENT_ID = "parentId";
    public static final String WORKFLOW_CONF = "conf";
    public static final String WORKFLOW_STATUS = "status";
    public static final String WORKFLOW_LAST_MOD_TIME = "lastModTime";
    public static final String WORKFLOW_CREATED_TIME = "createdTime";
    public static final String WORKFLOW_START_TIME = "startTime";
    public static final String WORKFLOW_END_TIME = "endTime";
    public static final String WORKFLOW_USER = "user";
    @Deprecated
    public static final String WORKFLOW_GROUP = "group";
    public static final String WORKFLOW_ACL = "acl";
    public static final String WORKFLOW_RUN = "run";
    public static final String WORKFLOW_CONSOLE_URL = "consoleUrl";
    public static final String WORKFLOW_ACTIONS = "actions";

    public static final String WORKFLOWS_JOBS = "workflows";
    public static final String WORKFLOWS_TOTAL = "total";
    public static final String WORKFLOWS_OFFSET = "offset";
    public static final String WORKFLOWS_LEN = "len";

    public static final String WORKFLOW_ACTION_ID = "id";
    public static final String WORKFLOW_ACTION_NAME = "name";
    public static final String WORKFLOW_ACTION_AUTH = "cred";
    public static final String WORKFLOW_ACTION_TYPE = "type";
    public static final String WORKFLOW_ACTION_CONF = "conf";
    public static final String WORKFLOW_ACTION_RETRIES = "retries";
    public static final String WORKFLOW_ACTION_START_TIME = "startTime";
    public static final String WORKFLOW_ACTION_END_TIME = "endTime";
    public static final String WORKFLOW_ACTION_STATUS = "status";
    public static final String WORKFLOW_ACTION_TRANSITION = "transition";
    public static final String WORKFLOW_ACTION_DATA = "data";
    public static final String WORKFLOW_ACTION_STATS = "stats";
    public static final String WORKFLOW_ACTION_EXTERNAL_CHILD_IDS = "externalChildIDs";
    public static final String WORKFLOW_ACTION_EXTERNAL_ID = "externalId";
    public static final String WORKFLOW_ACTION_EXTERNAL_STATUS = "externalStatus";
    public static final String WORKFLOW_ACTION_TRACKER_URI = "trackerUri";
    public static final String WORKFLOW_ACTION_CONSOLE_URL = "consoleUrl";
    public static final String WORKFLOW_ACTION_ERROR_CODE = "errorCode";
    public static final String WORKFLOW_ACTION_ERROR_MESSAGE = "errorMessage";


    public static final String COORDINATOR_JOB_ID = "coordJobId";
    public static final String COORDINATOR_JOB_NAME = "coordJobName";
    public static final String COORDINATOR_JOB_PATH = "coordJobPath";
    public static final String COORDINATOR_JOB_FREQUENCY = "frequency";
    public static final String COORDINATOR_JOB_TIMEUNIT = "timeUnit";
    public static final String COORDINATOR_JOB_TIMEZONE = "timeZone";
    public static final String COORDINATOR_JOB_CONCURRENCY = "concurrency";
    public static final String COORDINATOR_JOB_MAT_THROTTLING = "mat_throttling";
    public static final String COORDINATOR_JOB_EXECUTION = "execution";
    public static final String COORDINATOR_JOB_TIMEOUT = "timeOut";
    public static final String COORDINATOR_JOB_LAST_ACTION_TIME = "lastAction";
    public static final String COORDINATOR_JOB_NEXT_MATERIALIZED_TIME = "nextMaterializedTime";
    public static final String COORDINATOR_JOB_CONF = "conf";
    public static final String COORDINATOR_JOB_STATUS = "status";
    public static final String COORDINATOR_JOB_EXECUTIONPOLICY = "executionPolicy";
    public static final String COORDINATOR_JOB_START_TIME = "startTime";
    public static final String COORDINATOR_JOB_END_TIME = "endTime";
    public static final String COORDINATOR_JOB_PAUSE_TIME = "pauseTime";
    public static final String COORDINATOR_JOB_CONSOLE_URL = "consoleUrl";
    public static final String COORDINATOR_JOB_ACTIONS = "actions";
    public static final String COORDINATOR_JOB_USER = "user";
    public static final String COORDINATOR_JOB_NUM_ACTION = "total";

    @Deprecated
    public static final String COORDINATOR_JOB_GROUP = "group";
    public static final String COORDINATOR_JOB_ACL = "acl";
    public static final String COORDINATOR_JOB_EXTERNAL_ID = "coordExternalId";

    public static final String COORDINATOR_ACTION_ID = "id";
    public static final String COORDINATOR_ACTION_NAME = "name";
    public static final String COORDINATOR_ACTION_TYPE = "type";
    public static final String COORDINATOR_ACTION_CREATED_CONF = "createdConf";
    public static final String COORDINATOR_ACTION_RUNTIME_CONF = "runConf";
    public static final String COORDINATOR_ACTION_NUMBER = "actionNumber";
    public static final String COORDINATOR_ACTION_CREATED_TIME = "createdTime";
    public static final String COORDINATOR_ACTION_EXTERNALID = "externalId";
    public static final String COORDINATOR_JOB_BUNDLE_ID = "bundleId";
    public static final String COORDINATOR_ACTION_LAST_MODIFIED_TIME = "lastModifiedTime";
    public static final String COORDINATOR_ACTION_NOMINAL_TIME = "nominalTime";
    public static final String COORDINATOR_ACTION_STATUS = "status";
    public static final String COORDINATOR_ACTION_MISSING_DEPS = "missingDependencies";
    public static final String COORDINATOR_ACTION_PUSH_MISSING_DEPS = "pushMissingDependencies";
    public static final String COORDINATOR_ACTION_EXTERNAL_STATUS = "externalStatus";
    public static final String COORDINATOR_ACTION_TRACKER_URI = "trackerUri";
    public static final String COORDINATOR_ACTION_CONSOLE_URL = "consoleUrl";
    public static final String COORDINATOR_ACTION_ERROR_CODE = "errorCode";
    public static final String COORDINATOR_ACTION_ERROR_MESSAGE = "errorMessage";
    public static final String COORDINATOR_ACTIONS = "actions";
    public static final String COORDINATOR_ACTION_DATA = "data";
    public static final String COORDINATOR_JOB_DATA = "data";

    public static final String BUNDLE_JOB_ID = "bundleJobId";
    public static final String BUNDLE_JOB_NAME = "bundleJobName";
    public static final String BUNDLE_JOB_PATH = "bundleJobPath";
    public static final String BUNDLE_JOB_TIMEUNIT = "timeUnit";
    public static final String BUNDLE_JOB_TIMEOUT = "timeOut";
    public static final String BUNDLE_JOB_CONF = "conf";
    public static final String BUNDLE_JOB_STATUS = "status";
    public static final String BUNDLE_JOB_KICKOFF_TIME = "kickoffTime";
    public static final String BUNDLE_JOB_START_TIME = "startTime";
    public static final String BUNDLE_JOB_END_TIME = "endTime";
    public static final String BUNDLE_JOB_PAUSE_TIME = "pauseTime";
    public static final String BUNDLE_JOB_CREATED_TIME = "createdTime";
    public static final String BUNDLE_JOB_CONSOLE_URL = "consoleUrl";
    public static final String BUNDLE_JOB_USER = "user";
    @Deprecated
    public static final String BUNDLE_JOB_GROUP = "group";
    public static final String BUNDLE_JOB_ACL = "acl";
    public static final String BUNDLE_JOB_EXTERNAL_ID = "bundleExternalId";
    public static final String BUNDLE_COORDINATOR_JOBS = "bundleCoordJobs";

    public static final String SLA_SUMMARY_LIST = "slaSummaryList";
    public static final String SLA_SUMMARY_ID = "id";
    public static final String SLA_SUMMARY_PARENT_ID = "parentId";
    public static final String SLA_SUMMARY_APP_NAME = "appName";
    public static final String SLA_SUMMARY_APP_TYPE = "appType";
    public static final String SLA_SUMMARY_USER = "user";
    public static final String SLA_SUMMARY_NOMINAL_TIME = "nominalTime";
    public static final String SLA_SUMMARY_EXPECTED_START = "expectedStart";
    public static final String SLA_SUMMARY_ACTUAL_START = "actualStart";
    public static final String SLA_SUMMARY_EXPECTED_END = "expectedEnd";
    public static final String SLA_SUMMARY_ACTUAL_END = "actualEnd";
    public static final String SLA_SUMMARY_EXPECTED_DURATION = "expectedDuration";
    public static final String SLA_SUMMARY_ACTUAL_DURATION = "actualDuration";
    public static final String SLA_SUMMARY_JOB_STATUS = "jobStatus";
    public static final String SLA_SUMMARY_SLA_STATUS = "slaStatus";
    public static final String SLA_SUMMARY_LAST_MODIFIED = "lastModified";

    public static final String TO_STRING = "toString";


    public static final String ERROR = "error";
    public static final String ERROR_CODE = "code";
    public static final String ERROR_MESSAGE = "message";

    public static final String INSTR_TIMERS = "timers";
    public static final String INSTR_VARIABLES = "variables";
    public static final String INSTR_SAMPLERS = "samplers";
    public static final String INSTR_COUNTERS = "counters";
    public static final String INSTR_DATA = "data";

    public static final String INSTR_GROUP = "group";
    public static final String INSTR_NAME = "name";

    public static final String INSTR_TIMER_OWN_TIME_AVG = "ownTimeAvg";
    public static final String INSTR_TIMER_TOTAL_TIME_AVG = "totalTimeAvg";
    public static final String INSTR_TIMER_TICKS = "ticks";
    public static final String INSTR_TIMER_OWN_STD_DEV = "ownTimeStdDev";
    public static final String INSTR_TIMER_TOTAL_STD_DEV = "totalTimeStdDev";
    public static final String INSTR_TIMER_OWN_MIN_TIME = "ownMinTime";
    public static final String INSTR_TIMER_OWN_MAX_TIME = "ownMaxTime";
    public static final String INSTR_TIMER_TOTAL_MIN_TIME = "totalMinTime";
    public static final String INSTR_TIMER_TOTAL_MAX_TIME = "totalMaxTime";

    public static final String INSTR_VARIABLE_VALUE = "value";
    public static final String INSTR_SAMPLER_VALUE = "value";

    public static final Object COORDINATOR_JOBS = "coordinatorjobs";
    public static final Object COORD_JOB_TOTAL = "total";
    public static final Object COORD_JOB_OFFSET = "offset";
    public static final Object COORD_JOB_LEN = "len";

    public static final Object BUNDLE_JOBS = "bundlejobs";
    public static final Object BUNDLE_JOB_TOTAL = "total";
    public static final Object BUNDLE_JOB_OFFSET = "offset";
    public static final Object BUNDLE_JOB_LEN = "len";

    public static final String BULK_RESPONSE_BUNDLE = "bulkbundle";
    public static final String BULK_RESPONSE_COORDINATOR = "bulkcoord";
    public static final String BULK_RESPONSE_ACTION = "bulkaction";
    public static final Object BULK_RESPONSES = "bulkresponses";
    public static final Object BULK_RESPONSE_TOTAL = "total";
    public static final Object BULK_RESPONSE_OFFSET = "offset";
    public static final Object BULK_RESPONSE_LEN = "len";

    public static final String AVAILABLE_TIME_ZONES = "available-timezones";
    public static final String TIME_ZOME_DISPLAY_NAME = "timezoneDisplayName";
    public static final String TIME_ZONE_ID = "timezoneId";

    public static final String JMS_TOPIC_PATTERN = "jmsTopicPattern";
    public static final String JMS_JNDI_PROPERTIES = "jmsJNDIProps";
    public static final String JMS_TOPIC_PREFIX = "jmsTopicPrefix";

    public static final String JMS_TOPIC_NAME = "jmsTopicName";
    public static final String COORD_UPDATE = RestConstants.JOB_COORD_UPDATE;
    public static final String COORD_UPDATE_DIFF = "diff";

    public static final String STATUS = "status";
}
