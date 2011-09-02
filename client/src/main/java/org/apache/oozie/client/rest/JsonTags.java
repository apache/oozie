/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

    public static final String SYSTEM_SAFE_MODE = "safeMode";
    public static final String BUILD_VERSION = "buildVersion";

    public static final String JOB_ID = "id";

    public static final String WORKFLOW_APP_PATH = "appPath";
    public static final String WORKFLOW_APP_NAME = "appName";
    public static final String WORKFLOW_ID = "id";
    public static final String WORKFLOW_EXTERNAL_ID = "externalId";
    public static final String WORKFLOW_CONF = "conf";
    public static final String WORKFLOW_STATUS = "status";
    public static final String WORKFLOW_LAST_MOD_TIME = "lastModTime";
    public static final String WORKFLOW_CREATED_TIME = "createdTime";
    public static final String WORKFLOW_START_TIME = "startTime";
    public static final String WORKFLOW_END_TIME = "endTime";
    public static final String WORKFLOW_USER = "user";
    public static final String WORKFLOW_GROUP = "group";
    public static final String WORKFLOW_RUN = "run";
    public static final String WORKFLOW_CONSOLE_URL = "consoleUrl";
    public static final String WORKFLOW_ACTIONS = "actions";

    public static final String WORKFLOWS_JOBS = "workflows";
    public static final String WORKFLOWS_TOTAL = "total";
    public static final String WORKFLOWS_OFFSET = "offset";
    public static final String WORKFLOWS_LEN = "len";

    public static final String ACTION_ID = "id";
    public static final String ACTION_NAME = "name";
    public static final String ACTION_TYPE = "type";
    public static final String ACTION_CONF = "conf";
    public static final String ACTION_RETRIES = "retries";
    public static final String ACTION_START_TIME = "startTime";
    public static final String ACTION_END_TIME = "endTime";
    public static final String ACTION_STATUS = "status";
    public static final String ACTION_TRANSITION = "transition";
    public static final String ACTION_DATA = "data";
    public static final String ACTION_EXTERNAL_ID = "externalId";
    public static final String ACTION_EXTERNAL_STATUS = "externalStatus";
    public static final String ACTION_TRACKER_URI = "trackerUri";
    public static final String ACTION_CONSOLE_URL = "consoleUrl";
    public static final String ACTION_ERROR_CODE = "errorCode";
    public static final String ACTION_ERROR_MESSAGE = "errorMessage";

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

}