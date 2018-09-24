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

    String VERSIONS = "versions";

    String JOB = "job";

    String JOBS = "jobs";

    String ADMIN = "admin";

    String JSON_CONTENT_TYPE = "application/json";

    String XML_CONTENT_TYPE = "application/xml";

    String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";

    String TEXT_CONTENT_TYPE = "text/plain";

    String PNG_IMAGE_CONTENT_TYPE = "image/png";

    String SVG_IMAGE_CONTENT_TYPE = "image/svg+xml";

    String ACTION_PARAM = "action";

    String OFFSET_PARAM = "offset";

    String LEN_PARAM = "len";

    String ORDER_PARAM = "order";

    String SORTBY_PARAM = "sortby";

    String ACTION_NAME_PARAM = "action-name";

    String JOB_FILTER_PARAM = "filter";

    String JOB_RESOURCE = "/job";

    String JOB_ACTION_START = "start";

    String JOB_ACTION_SUBMIT = "submit";

    String JOB_ACTION_DRYRUN = "dryrun";

    String JOB_ACTION_SHOWDIFF = "diff";

    String JOB_ACTION_SUSPEND = "suspend";

    String JOB_ACTION_RESUME = "resume";

    String JOB_ACTION_KILL = "kill";

    String JOB_ACTION_CHANGE = "change";

    String JOB_CHANGE_VALUE = "value";

    String JOB_ACTION_RERUN = "rerun";

    String JOB_ACTION_IGNORE = "ignore";

    String JOB_COORD_ACTION_RERUN = "coord-rerun";

    String JOB_COORD_UPDATE = "update";

    String JOB_BUNDLE_ACTION_RERUN = "bundle-rerun";

    String JOB_SHOW_PARAM = "show";

    String JOB_SHOW_ACTION_RETRIES_PARAM = "retries";

    String JOB_SHOW_CONFIG = "config";

    String JOB_SHOW_INFO = "info";

    String JOB_SHOW_LOG = "log";

    String JOB_SHOW_ERROR_LOG = "errorlog";

    String JOB_SHOW_AUDIT_LOG = "auditlog";

    String JOB_SHOW_DEFINITION = "definition";

    String JOB_SHOW_GRAPH = "graph";

    String JOB_SHOW_KILL_PARAM = "show-kill";

    String JOB_FORMAT_PARAM = "format";

    String JOB_SHOW_STATUS = "status";

    String JOB_SHOW_WF_ACTIONS_IN_COORD = "wf-actions";

    String JOB_BUNDLE_RERUN_COORD_SCOPE_PARAM = "coord-scope";

    String JOB_BUNDLE_RERUN_DATE_SCOPE_PARAM = "date-scope";

    String JOB_COORD_RANGE_TYPE_PARAM = "type";

    String JOB_COORD_SCOPE_DATE = "date";

    String JOB_COORD_SCOPE_ACTION = "action";

    String JOB_COORD_SCOPE_PARAM = "scope";

    String JOB_COORD_RERUN_REFRESH_PARAM = "refresh";

    String JOB_COORD_RERUN_NOCLEANUP_PARAM = "nocleanup";

    String JOB_LOG_ACTION = "action";

    String JOB_LOG_DATE = "date";

    String JOB_LOG_SCOPE_PARAM = "scope";

    String JOB_LOG_TYPE_PARAM = "type";

    String JOBS_FILTER_PARAM = "filter";

    String JOBS_BULK_PARAM = "bulk";

    String JOBS_EXTERNAL_ID_PARAM = "external-id";

    String ADMIN_STATUS_RESOURCE = "status";

    String ADMIN_SAFE_MODE_PARAM = "safemode";

    String ADMIN_SYSTEM_MODE_PARAM = "systemmode";

    String ADMIN_LOG_RESOURCE = "log";

    String ADMIN_OS_ENV_RESOURCE = "os-env";

    String ADMIN_JAVA_SYS_PROPS_RESOURCE = "java-sys-properties";

    String ADMIN_CONFIG_RESOURCE = "configuration";

    String ADMIN_INSTRUMENTATION_RESOURCE = "instrumentation";

    String ADMIN_BUILD_VERSION_RESOURCE = "build-version";

    String ADMIN_QUEUE_DUMP_RESOURCE = "queue-dump";

    String ADMIN_METRICS_RESOURCE = "metrics";

    String OOZIE_ERROR_CODE = "oozie-error-code";

    String OOZIE_ERROR_MESSAGE = "oozie-error-message";

    String JOBTYPE_PARAM = "jobtype";

    String SLA_GT_SEQUENCE_ID = "gt-sequence-id";

    String MAX_EVENTS = "max-events";

    String SLA = "sla";

    String DO_AS_PARAM = "doAs";

    String TIME_ZONE_PARAM = "timezone";

    String ADMIN_TIME_ZONES_RESOURCE = "available-timezones";

    String ADMIN_JMS_INFO = "jmsinfo";

    String JOB_SHOW_JMS_TOPIC = "jmstopic";

    String ADMIN_AVAILABLE_OOZIE_SERVERS_RESOURCE = "available-oozie-servers";

    String ADMIN_UPDATE_SHARELIB = "update_sharelib";

    String ADMIN_LIST_SHARELIB = "list_sharelib";

    String SHARE_LIB_REQUEST_KEY = "lib";

    String ALL_SERVER_REQUEST = "allservers";

    String ALL_WORKFLOWS_FOR_COORD_ACTION = "allruns";

    String LOG_FILTER_OPTION = "logfilter";

    String JOB_COORD_RERUN_FAILED_PARAM = "failed";

    String SLA_DISABLE_ALERT = "sla-disable";

    String SLA_ENABLE_ALERT = "sla-enable";

    String SLA_CHANGE = "sla-change";

    String SLA_ALERT_RANGE = "sla-alert-range";

    String COORDINATORS_PARAM = "coordinators";

    String SLA_NOMINAL_TIME = "sla-nominal-time";

    String SLA_SHOULD_START = "sla-should-start";

    String SLA_SHOULD_END = "sla-should-end";

    String SLA_MAX_DURATION = "sla-max-duration";

    String JOB_COORD_SCOPE_ACTION_LIST = "action-list";

    String VALIDATE = "validate";

    String FILE_PARAM = "file";

    String USER_PARAM = "user";

    String COORD_ACTION_MISSING_DEPENDENCIES = "missing-dependencies";

    String ADMIN_PURGE = "purge";
    String PURGE_WF_AGE = "wf";
    String PURGE_COORD_AGE = "coord";
    String PURGE_BUNDLE_AGE = "bundle";
    String PURGE_LIMIT = "limit";
    String PURGE_OLD_COORD_ACTION = "oldcoordaction";
}
