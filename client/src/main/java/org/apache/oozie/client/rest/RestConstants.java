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
 * Constansts used by Oozie REST WS API
 */
public interface RestConstants {

    public static final String VERSIONS = "versions";

    public static final String JOB = "job";

    public static final String JOBS = "jobs";

    public static final String ADMIN = "admin";

    public static final String JSON_CONTENT_TYPE = "application/json";

    public static final String XML_CONTENT_TYPE = "application/xml";

    public static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";

    public static final String TEXT_CONTENT_TYPE = "text/plain";

    public static final String PNG_IMAGE_CONTENT_TYPE = "image/png";

    public static final String ACTION_PARAM = "action";

    public static final String OFFSET_PARAM = "offset";

    public static final String LEN_PARAM = "len";

    public static final String ORDER_PARAM = "order";

    public static final String JOB_FILTER_PARAM = "filter";

    public static final String JOB_RESOURCE = "/job";

    public static final String JOB_ACTION_START = "start";

    public static final String JOB_ACTION_DRYRUN = "dryrun";

    public static final String JOB_ACTION_SHOWDIFF = "diff";

    public static final String JOB_ACTION_SUSPEND = "suspend";

    public static final String JOB_ACTION_RESUME = "resume";

    public static final String JOB_ACTION_KILL = "kill";

    public static final String JOB_ACTION_CHANGE = "change";

    public static final String JOB_CHANGE_VALUE = "value";

    public static final String JOB_ACTION_RERUN = "rerun";

    public static final String JOB_ACTION_IGNORE = "ignore";

    public static final String JOB_COORD_ACTION_RERUN = "coord-rerun";

    public static final String JOB_COORD_UPDATE = "update";

    public static final String JOB_BUNDLE_ACTION_RERUN = "bundle-rerun";

    public static final String JOB_SHOW_PARAM = "show";

    public static final String JOB_SHOW_CONFIG = "config";

    public static final String JOB_SHOW_INFO = "info";

    public static final String JOB_SHOW_LOG = "log";

    public static final String JOB_SHOW_ERROR_LOG = "errorlog";

    public static final String JOB_SHOW_DEFINITION = "definition";

    public static final String JOB_SHOW_GRAPH = "graph";

    public static final String JOB_SHOW_KILL_PARAM = "show-kill";

    public static final String JOB_SHOW_STATUS = "status";

    public static final String JOB_BUNDLE_RERUN_COORD_SCOPE_PARAM = "coord-scope";

    public static final String JOB_BUNDLE_RERUN_DATE_SCOPE_PARAM = "date-scope";

    public static final String JOB_COORD_RANGE_TYPE_PARAM = "type";

    public static final String JOB_COORD_SCOPE_DATE = "date";

    public static final String JOB_COORD_SCOPE_ACTION = "action";

    public static final String JOB_COORD_SCOPE_PARAM = "scope";

    public static final String JOB_COORD_RERUN_REFRESH_PARAM = "refresh";

    public static final String JOB_COORD_RERUN_NOCLEANUP_PARAM = "nocleanup";

    public static final String JOB_LOG_ACTION = "action";

    public static final String JOB_LOG_DATE = "date";

    public static final String JOB_LOG_SCOPE_PARAM = "scope";

    public static final String JOB_LOG_TYPE_PARAM = "type";

    public static final String JOBS_FILTER_PARAM = "filter";

    public static final String JOBS_BULK_PARAM = "bulk";

    public static final String JOBS_EXTERNAL_ID_PARAM = "external-id";

    public static final String ADMIN_STATUS_RESOURCE = "status";

    public static final String ADMIN_SAFE_MODE_PARAM = "safemode";

    public static final String ADMIN_SYSTEM_MODE_PARAM = "systemmode";

    public static final String ADMIN_LOG_RESOURCE = "log";

    public static final String ADMIN_OS_ENV_RESOURCE = "os-env";

    public static final String ADMIN_JAVA_SYS_PROPS_RESOURCE = "java-sys-properties";

    public static final String ADMIN_CONFIG_RESOURCE = "configuration";

    public static final String ADMIN_INSTRUMENTATION_RESOURCE = "instrumentation";

    public static final String ADMIN_BUILD_VERSION_RESOURCE = "build-version";

    public static final String ADMIN_QUEUE_DUMP_RESOURCE = "queue-dump";

    public static final String ADMIN_METRICS_RESOURCE = "metrics";

    public static final String OOZIE_ERROR_CODE = "oozie-error-code";

    public static final String OOZIE_ERROR_MESSAGE = "oozie-error-message";

    public static final String JOBTYPE_PARAM = "jobtype";

    public static final String SLA_GT_SEQUENCE_ID = "gt-sequence-id";

    public static final String MAX_EVENTS = "max-events";

    public static final String SLA = "sla";

    public static final String DO_AS_PARAM = "doAs";

    public static final String TIME_ZONE_PARAM = "timezone";

    public static final String ADMIN_TIME_ZONES_RESOURCE = "available-timezones";

    public static final String ADMIN_JMS_INFO = "jmsinfo";

    public static final String JOB_SHOW_JMS_TOPIC = "jmstopic";

    public static final String ADMIN_AVAILABLE_OOZIE_SERVERS_RESOURCE = "available-oozie-servers";

    public static final String ADMIN_UPDATE_SHARELIB = "update_sharelib";

    public static final String ADMIN_LIST_SHARELIB = "list_sharelib";

    public static final String SHARE_LIB_REQUEST_KEY = "lib";

    public static final String ALL_SERVER_REQUEST = "allservers";

    public static final String ALL_WORKFLOWS_FOR_COORD_ACTION = "allruns";

    public static final String LOG_FILTER_OPTION = "logfilter";

    public static final String JOB_COORD_RERUN_FAILED_PARAM = "failed";
}
