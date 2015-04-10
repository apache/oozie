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

package org.apache.oozie.cli;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.BuildInfo;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.BulkResponse;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonToBean;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Oozie command line utility.
 */
public class OozieCLI {
    public static final String ENV_OOZIE_URL = "OOZIE_URL";
    public static final String ENV_OOZIE_DEBUG = "OOZIE_DEBUG";
    public static final String ENV_OOZIE_TIME_ZONE = "OOZIE_TIMEZONE";
    public static final String ENV_OOZIE_AUTH = "OOZIE_AUTH";
    public static final String OOZIE_RETRY_COUNT = "oozie.connection.retry.count";
    public static final String WS_HEADER_PREFIX = "header:";

    public static final String HELP_CMD = "help";
    public static final String VERSION_CMD = "version";
    public static final String JOB_CMD = "job";
    public static final String JOBS_CMD = "jobs";
    public static final String ADMIN_CMD = "admin";
    public static final String VALIDATE_CMD = "validate";
    public static final String SLA_CMD = "sla";
    public static final String PIG_CMD = "pig";
    public static final String HIVE_CMD = "hive";
    public static final String SQOOP_CMD = "sqoop";
    public static final String MR_CMD = "mapreduce";
    public static final String INFO_CMD = "info";

    public static final String OOZIE_OPTION = "oozie";
    public static final String CONFIG_OPTION = "config";
    public static final String SUBMIT_OPTION = "submit";
    public static final String OFFSET_OPTION = "offset";
    public static final String START_OPTION = "start";
    public static final String RUN_OPTION = "run";
    public static final String DRYRUN_OPTION = "dryrun";
    public static final String SUSPEND_OPTION = "suspend";
    public static final String RESUME_OPTION = "resume";
    public static final String KILL_OPTION = "kill";
    public static final String CHANGE_OPTION = "change";
    public static final String CHANGE_VALUE_OPTION = "value";
    public static final String RERUN_OPTION = "rerun";
    public static final String INFO_OPTION = "info";
    public static final String LOG_OPTION = "log";
    public static final String ERROR_LOG_OPTION = "errorlog";
    public static final String AUDIT_LOG_OPTION = "auditlog";

    public static final String ACTION_OPTION = "action";
    public static final String DEFINITION_OPTION = "definition";
    public static final String CONFIG_CONTENT_OPTION = "configcontent";
    public static final String SQOOP_COMMAND_OPTION = "command";
    public static final String SHOWDIFF_OPTION = "diff";
    public static final String UPDATE_OPTION = "update";
    public static final String IGNORE_OPTION = "ignore";
    public static final String POLL_OPTION = "poll";
    public static final String TIMEOUT_OPTION = "timeout";
    public static final String INTERVAL_OPTION = "interval";

    public static final String DO_AS_OPTION = "doas";

    public static final String LEN_OPTION = "len";
    public static final String FILTER_OPTION = "filter";
    public static final String JOBTYPE_OPTION = "jobtype";
    public static final String SYSTEM_MODE_OPTION = "systemmode";
    public static final String VERSION_OPTION = "version";
    public static final String STATUS_OPTION = "status";
    public static final String LOCAL_TIME_OPTION = "localtime";
    public static final String TIME_ZONE_OPTION = "timezone";
    public static final String QUEUE_DUMP_OPTION = "queuedump";
    public static final String DATE_OPTION = "date";
    public static final String RERUN_REFRESH_OPTION = "refresh";
    public static final String RERUN_NOCLEANUP_OPTION = "nocleanup";
    public static final String RERUN_FAILED_OPTION = "failed";
    public static final String ORDER_OPTION = "order";
    public static final String COORD_OPTION = "coordinator";

    public static final String UPDATE_SHARELIB_OPTION = "sharelibupdate";

    public static final String LIST_SHARELIB_LIB_OPTION = "shareliblist";

    public static final String SLA_DISABLE_ALERT = "sladisable";
    public static final String SLA_ENABLE_ALERT = "slaenable";
    public static final String SLA_CHANGE = "slachange";

    public static final String SERVER_CONFIGURATION_OPTION = "configuration";
    public static final String SERVER_OS_ENV_OPTION = "osenv";
    public static final String SERVER_JAVA_SYSTEM_PROPERTIES_OPTION = "javasysprops";

    public static final String METRICS_OPTION = "metrics";
    public static final String INSTRUMENTATION_OPTION = "instrumentation";

    public static final String AUTH_OPTION = "auth";

    public static final String VERBOSE_OPTION = "verbose";
    public static final String VERBOSE_DELIMITER = "\t";
    public static final String DEBUG_OPTION = "debug";

    public static final String SCRIPTFILE_OPTION = "file";

    public static final String INFO_TIME_ZONES_OPTION = "timezones";

    public static final String BULK_OPTION = "bulk";

    public static final String AVAILABLE_SERVERS_OPTION = "servers";

    public static final String ALL_WORKFLOWS_FOR_COORD_ACTION = "allruns";

    private static final String[] OOZIE_HELP = {
            "the env variable '" + ENV_OOZIE_URL + "' is used as default value for the '-" + OOZIE_OPTION + "' option",
            "the env variable '" + ENV_OOZIE_TIME_ZONE + "' is used as default value for the '-" + TIME_ZONE_OPTION + "' option",
            "the env variable '" + ENV_OOZIE_AUTH + "' is used as default value for the '-" + AUTH_OPTION + "' option",
            "custom headers for Oozie web services can be specified using '-D" + WS_HEADER_PREFIX + "NAME=VALUE'" };

    private static final String RULER;
    private static final int LINE_WIDTH = 132;

    private static final int RETRY_COUNT = 4;

    private boolean used;

    private static final String INSTANCE_SEPARATOR = "#";

    private static final String MAPRED_MAPPER = "mapred.mapper.class";
    private static final String MAPRED_MAPPER_2 = "mapreduce.map.class";
    private static final String MAPRED_REDUCER = "mapred.reducer.class";
    private static final String MAPRED_REDUCER_2 = "mapreduce.reduce.class";
    private static final String MAPRED_INPUT = "mapred.input.dir";
    private static final String MAPRED_OUTPUT = "mapred.output.dir";

    private static final Pattern GMT_OFFSET_SHORTEN_PATTERN = Pattern.compile("(.* )GMT((?:-|\\+)\\d{2}:\\d{2})");

    static {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < LINE_WIDTH; i++) {
            sb.append("-");
        }
        RULER = sb.toString();
    }

    /**
     * Entry point for the Oozie CLI when invoked from the command line.
     * <p/>
     * Upon completion this method exits the JVM with '0' (success) or '-1' (failure).
     *
     * @param args options and arguments for the Oozie CLI.
     */
    public static void main(String[] args) {
        if (!System.getProperties().containsKey(AuthOozieClient.USE_AUTH_TOKEN_CACHE_SYS_PROP)) {
            System.setProperty(AuthOozieClient.USE_AUTH_TOKEN_CACHE_SYS_PROP, "true");
        }
        System.exit(new OozieCLI().run(args));
    }

    /**
     * Create an Oozie CLI instance.
     */
    public OozieCLI() {
        used = false;
    }

    /**
     * Return Oozie CLI top help lines.
     *
     * @return help lines.
     */
    protected String[] getCLIHelp() {
        return OOZIE_HELP;
    }

    /**
     * Add authentication specific options to oozie cli
     *
     * @param options the collection of options to add auth options
     */
    protected void addAuthOptions(Options options) {
        Option auth = new Option(AUTH_OPTION, true, "select authentication type [SIMPLE|KERBEROS]");
        options.addOption(auth);
    }

    /**
     * Create option for command line option 'admin'
     * @return admin options
     */
    protected Options createAdminOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option system_mode = new Option(SYSTEM_MODE_OPTION, true,
                "Supported in Oozie-2.0 or later versions ONLY. Change oozie system mode [NORMAL|NOWEBSERVICE|SAFEMODE]");
        Option status = new Option(STATUS_OPTION, false, "show the current system status");
        Option version = new Option(VERSION_OPTION, false, "show Oozie server build version");
        Option queuedump = new Option(QUEUE_DUMP_OPTION, false, "show Oozie server queue elements");
        Option doAs = new Option(DO_AS_OPTION, true, "doAs user, impersonates as the specified user");
        Option availServers = new Option(AVAILABLE_SERVERS_OPTION, false, "list available Oozie servers"
                + " (more than one only if HA is enabled)");
        Option sharelibUpdate = new Option(UPDATE_SHARELIB_OPTION, false, "Update server to use a newer version of sharelib");
        Option serverConfiguration = new Option(SERVER_CONFIGURATION_OPTION, false, "show Oozie system configuration");
        Option osEnv = new Option(SERVER_OS_ENV_OPTION, false, "show Oozie system OS environment");
        Option javaSysProps = new Option(SERVER_JAVA_SYSTEM_PROPERTIES_OPTION, false, "show Oozie Java system properties");
        Option metrics = new Option(METRICS_OPTION, false, "show Oozie system metrics");
        Option instrumentation = new Option(INSTRUMENTATION_OPTION, false, "show Oozie system instrumentation");

        Option sharelib = new Option(LIST_SHARELIB_LIB_OPTION, false,
                "List available sharelib that can be specified in a workflow action");
        sharelib.setOptionalArg(true);

        Options adminOptions = new Options();
        adminOptions.addOption(oozie);
        adminOptions.addOption(doAs);
        OptionGroup group = new OptionGroup();
        group.addOption(system_mode);
        group.addOption(status);
        group.addOption(version);
        group.addOption(queuedump);
        group.addOption(availServers);
        group.addOption(sharelibUpdate);
        group.addOption(sharelib);
        group.addOption(serverConfiguration);
        group.addOption(osEnv);
        group.addOption(javaSysProps);
        group.addOption(metrics);
        group.addOption(instrumentation);
        adminOptions.addOptionGroup(group);
        addAuthOptions(adminOptions);
        return adminOptions;
    }

    /**
     * Create option for command line option 'job'
     * @return job options
     */
    protected Options createJobOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true, "job configuration file '.xml' or '.properties'");
        Option submit = new Option(SUBMIT_OPTION, false, "submit a job");
        Option run = new Option(RUN_OPTION, false, "run a job");
        Option debug = new Option(DEBUG_OPTION, false, "Use debug mode to see debugging statements on stdout");
        Option rerun = new Option(RERUN_OPTION, true,
                "rerun a job  (coordinator requires -action or -date, bundle requires -coordinator or -date)");
        Option dryrun = new Option(DRYRUN_OPTION, false, "Dryrun a workflow (since 3.3.2) or coordinator (since 2.0) job without"
                + " actually executing it");
        Option update = new Option(UPDATE_OPTION, true, "Update coord definition and properties");
        Option showdiff = new Option(SHOWDIFF_OPTION, true,
                "Show diff of the new coord definition and properties with the existing one (default true)");
        Option start = new Option(START_OPTION, true, "start a job");
        Option suspend = new Option(SUSPEND_OPTION, true, "suspend a job");
        Option resume = new Option(RESUME_OPTION, true, "resume a job");
        Option kill = new Option(KILL_OPTION, true, "kill a job (coordinator can mention -action or -date)");
        Option change = new Option(CHANGE_OPTION, true, "change a coordinator or bundle job");
        Option changeValue = new Option(CHANGE_VALUE_OPTION, true,
                "new endtime/concurrency/pausetime value for changing a coordinator job");
        Option info = new Option(INFO_OPTION, true, "info of a job");
        Option poll = new Option(POLL_OPTION, true, "poll Oozie until a job reaches a terminal state or a timeout occurs");
        Option offset = new Option(OFFSET_OPTION, true, "job info offset of actions (default '1', requires -info)");
        Option len = new Option(LEN_OPTION, true, "number of actions (default TOTAL ACTIONS, requires -info)");
        Option filter = new Option(FILTER_OPTION, true,
                "<key><comparator><value>[;<key><comparator><value>]*\n"
                    + "(All Coordinator actions satisfying the filters will be retreived).\n"
                    + "key: status or nominaltime\n"
                    + "comparator: =, !=, <, <=, >, >=. = is used as OR and others as AND\n"
                    + "status: values are valid status like SUCCEEDED, KILLED etc. Only = and != apply for status\n"
                    + "nominaltime: time of format yyyy-MM-dd'T'HH:mm'Z'");
        Option order = new Option(ORDER_OPTION, true,
                "order to show coord actions (default ascending order, 'desc' for descending order, requires -info)");
        Option localtime = new Option(LOCAL_TIME_OPTION, false, "use local time (same as passing your time zone to -" +
                TIME_ZONE_OPTION + "). Overrides -" + TIME_ZONE_OPTION + " option");
        Option timezone = new Option(TIME_ZONE_OPTION, true,
                "use time zone with the specified ID (default GMT).\nSee 'oozie info -timezones' for a list");
        Option log = new Option(LOG_OPTION, true, "job log");
        Option errorlog = new Option(ERROR_LOG_OPTION, true, "job error log");
        Option auditlog = new Option(AUDIT_LOG_OPTION, true, "job audit log");
        Option logFilter = new Option(
                RestConstants.LOG_FILTER_OPTION, true,
                "job log search parameter. Can be specified as -logfilter opt1=val1;opt2=val1;opt3=val1. "
                + "Supported options are recent, start, end, loglevel, text, limit and debug");
        Option definition = new Option(DEFINITION_OPTION, true, "job definition");
        Option config_content = new Option(CONFIG_CONTENT_OPTION, true, "job configuration");
        Option verbose = new Option(VERBOSE_OPTION, false, "verbose mode");
        Option action = new Option(ACTION_OPTION, true,
                "coordinator rerun/kill on action ids (requires -rerun/-kill); coordinator log retrieval on action ids"
                        + "(requires -log)");
        Option date = new Option(DATE_OPTION, true,
                "coordinator/bundle rerun on action dates (requires -rerun); coordinator log retrieval on action dates (requires -log)");
        Option rerun_coord = new Option(COORD_OPTION, true, "bundle rerun on coordinator names (requires -rerun)");
        Option rerun_refresh = new Option(RERUN_REFRESH_OPTION, false,
                "re-materialize the coordinator rerun actions (requires -rerun)");
        Option rerun_nocleanup = new Option(RERUN_NOCLEANUP_OPTION, false,
                "do not clean up output-events of the coordiantor rerun actions (requires -rerun)");
        Option rerun_failed = new Option(RERUN_FAILED_OPTION, false,
                "runs the failed workflow actions of the coordinator actions (requires -rerun)");
        Option property = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription(
                "set/override value for given property").create("D");
        Option getAllWorkflows = new Option(ALL_WORKFLOWS_FOR_COORD_ACTION, false,
                "Get workflow jobs corresponding to a coordinator action including all the reruns");
        Option ignore = new Option(IGNORE_OPTION, true,
                "change status of a coordinator job or action to IGNORED"
                + " (-action required to ignore coord actions)");
        Option timeout = new Option(TIMEOUT_OPTION, true, "timeout in minutes (default is 30, negative values indicate no "
                + "timeout, requires -poll)");
        timeout.setType(Integer.class);
        Option interval = new Option(INTERVAL_OPTION, true, "polling interval in minutes (default is 5, requires -poll)");
        interval.setType(Integer.class);

        Option slaDisableAlert = new Option(SLA_DISABLE_ALERT, true,
                "disables sla alerts for the job and its children");
        Option slaEnableAlert = new Option(SLA_ENABLE_ALERT, true,
                "enables sla alerts for the job and its children");
        Option slaChange = new Option(SLA_CHANGE, true,
                "Update sla param for jobs, supported param are should-start, should-end, nominal-time and max-duration");


        Option doAs = new Option(DO_AS_OPTION, true, "doAs user, impersonates as the specified user");

        OptionGroup actions = new OptionGroup();
        actions.addOption(submit);
        actions.addOption(start);
        actions.addOption(run);
        actions.addOption(dryrun);
        actions.addOption(suspend);
        actions.addOption(resume);
        actions.addOption(kill);
        actions.addOption(change);
        actions.addOption(update);
        actions.addOption(info);
        actions.addOption(rerun);
        actions.addOption(log);
        actions.addOption(errorlog);
        actions.addOption(auditlog);
        actions.addOption(definition);
        actions.addOption(config_content);
        actions.addOption(ignore);
        actions.addOption(poll);
        actions.addOption(slaDisableAlert);
        actions.addOption(slaEnableAlert);
        actions.addOption(slaChange);

        actions.setRequired(true);
        Options jobOptions = new Options();
        jobOptions.addOption(oozie);
        jobOptions.addOption(doAs);
        jobOptions.addOption(config);
        jobOptions.addOption(property);
        jobOptions.addOption(changeValue);
        jobOptions.addOption(localtime);
        jobOptions.addOption(timezone);
        jobOptions.addOption(verbose);
        jobOptions.addOption(debug);
        jobOptions.addOption(offset);
        jobOptions.addOption(len);
        jobOptions.addOption(filter);
        jobOptions.addOption(order);
        jobOptions.addOption(action);
        jobOptions.addOption(date);
        jobOptions.addOption(rerun_coord);
        jobOptions.addOption(rerun_refresh);
        jobOptions.addOption(rerun_nocleanup);
        jobOptions.addOption(rerun_failed);
        jobOptions.addOption(getAllWorkflows);
        jobOptions.addOptionGroup(actions);
        jobOptions.addOption(logFilter);
        jobOptions.addOption(timeout);
        jobOptions.addOption(interval);
        addAuthOptions(jobOptions);
        jobOptions.addOption(showdiff);

        //Needed to make dryrun and update mutually exclusive options
        OptionGroup updateOption = new OptionGroup();
        updateOption.addOption(dryrun);
        jobOptions.addOptionGroup(updateOption);

        return jobOptions;
    }

    /**
     * Create option for command line option 'jobs'
     * @return jobs options
     */
    protected Options createJobsOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option start = new Option(OFFSET_OPTION, true, "jobs offset (default '1')");
        Option jobtype = new Option(JOBTYPE_OPTION, true,
                "job type ('Supported in Oozie-2.0 or later versions ONLY - 'coordinator' or 'bundle' or 'wf'(default))");
        Option len = new Option(LEN_OPTION, true, "number of jobs (default '100')");
        Option filter = new Option(FILTER_OPTION, true,
                "user=<U>\\;name=<N>\\;group=<G>\\;status=<S>\\;frequency=<F>\\;unit=<M>" +
                        "\\;startcreatedtime=<SC>\\;endcreatedtime=<EC> " +
                        "(valid unit values are 'months', 'days', 'hours' or 'minutes'. " +
                        "startcreatedtime, endcreatedtime: time of format yyyy-MM-dd'T'HH:mm'Z')");
        Option localtime = new Option(LOCAL_TIME_OPTION, false, "use local time (same as passing your time zone to -" +
                TIME_ZONE_OPTION + "). Overrides -" + TIME_ZONE_OPTION + " option");
        Option kill = new Option(KILL_OPTION, false, "bulk kill operation");
        Option suspend = new Option(SUSPEND_OPTION, false, "bulk suspend operation");
        Option resume = new Option(RESUME_OPTION, false, "bulk resume operation");
        Option timezone = new Option(TIME_ZONE_OPTION, true,
                "use time zone with the specified ID (default GMT).\nSee 'oozie info -timezones' for a list");
        Option verbose = new Option(VERBOSE_OPTION, false, "verbose mode");
        Option doAs = new Option(DO_AS_OPTION, true, "doAs user, impersonates as the specified user");
        Option bulkMonitor = new Option(BULK_OPTION, true, "key-value pairs to filter bulk jobs response. e.g. bundle=<B>\\;" +
                "coordinators=<C>\\;actionstatus=<S>\\;startcreatedtime=<SC>\\;endcreatedtime=<EC>\\;" +
                "startscheduledtime=<SS>\\;endscheduledtime=<ES>\\; bundle, coordinators and actionstatus can be multiple comma separated values" +
                "bundle and coordinators can be id(s) or appName(s) of those jobs. Specifying bundle is mandatory, other params are optional");
        start.setType(Integer.class);
        len.setType(Integer.class);
        Options jobsOptions = new Options();
        jobsOptions.addOption(oozie);
        jobsOptions.addOption(doAs);
        jobsOptions.addOption(localtime);
        jobsOptions.addOption(kill);
        jobsOptions.addOption(suspend);
        jobsOptions.addOption(resume);
        jobsOptions.addOption(timezone);
        jobsOptions.addOption(start);
        jobsOptions.addOption(len);
        jobsOptions.addOption(oozie);
        jobsOptions.addOption(filter);
        jobsOptions.addOption(jobtype);
        jobsOptions.addOption(verbose);
        jobsOptions.addOption(bulkMonitor);
        addAuthOptions(jobsOptions);
        return jobsOptions;
    }

    /**
     * Create option for command line option 'sla'
     *
     * @return sla options
     */
    protected Options createSlaOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option start = new Option(OFFSET_OPTION, true, "start offset (default '0')");
        Option len = new Option(LEN_OPTION, true, "number of results (default '100', max '1000')");
        Option filter = new Option(FILTER_OPTION, true, "filter of SLA events. e.g., jobid=<J>\\;appname=<A>");
        start.setType(Integer.class);
        len.setType(Integer.class);
        Options slaOptions = new Options();
        slaOptions.addOption(start);
        slaOptions.addOption(len);
        slaOptions.addOption(filter);
        slaOptions.addOption(oozie);
        addAuthOptions(slaOptions);
        return slaOptions;
    }

    /**
     * Create option for command line option 'pig' or 'hive'
     * @return pig or hive options
     */
    @SuppressWarnings("static-access")
    protected Options createScriptLanguageOptions(String jobType) {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true, "job configuration file '.properties'");
        Option file = new Option(SCRIPTFILE_OPTION, true, jobType + " script");
        Option property = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription(
                "set/override value for given property").create("D");
        Option params = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription(
                "set parameters for script").create("P");
        Option doAs = new Option(DO_AS_OPTION, true, "doAs user, impersonates as the specified user");
        Options Options = new Options();
        Options.addOption(oozie);
        Options.addOption(doAs);
        Options.addOption(config);
        Options.addOption(property);
        Options.addOption(params);
        Options.addOption(file);
        addAuthOptions(Options);
        return Options;
    }

    /**
     * Create option for command line option 'sqoop'
     * @return sqoop options
     */
    @SuppressWarnings("static-access")
    protected Options createSqoopCLIOptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true, "job configuration file '.properties'");
        Option command = OptionBuilder.withArgName(SQOOP_COMMAND_OPTION).hasArgs().withValueSeparator().withDescription(
                "sqoop command").create(SQOOP_COMMAND_OPTION);
        Option property = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription(
                "set/override value for given property").create("D");
        Option doAs = new Option(DO_AS_OPTION, true, "doAs user, impersonates as the specified user");
        Options Options = new Options();
        Options.addOption(oozie);
        Options.addOption(doAs);
        Options.addOption(config);
        Options.addOption(property);
        Options.addOption(command);
        addAuthOptions(Options);
        return Options;
    }

    /**
     * Create option for command line option 'info'
     * @return info options
     */
    protected Options createInfoOptions() {
        Option timezones = new Option(INFO_TIME_ZONES_OPTION, false, "display a list of available time zones");
        Options infoOptions = new Options();
        infoOptions.addOption(timezones);
        return infoOptions;
    }

    /**
     * Create option for command line option 'mapreduce'
     * @return mapreduce options
     */
    @SuppressWarnings("static-access")
    protected Options createMROptions() {
        Option oozie = new Option(OOZIE_OPTION, true, "Oozie URL");
        Option config = new Option(CONFIG_OPTION, true, "job configuration file '.properties'");
        Option property = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator().withDescription(
                "set/override value for given property").create("D");
        Option doAs = new Option(DO_AS_OPTION, true, "doAs user, impersonates as the specified user");
        Options mrOptions = new Options();
        mrOptions.addOption(oozie);
        mrOptions.addOption(doAs);
        mrOptions.addOption(config);
        mrOptions.addOption(property);
        addAuthOptions(mrOptions);
        return mrOptions;
    }

    /**
     * Run a CLI programmatically.
     * <p/>
     * It does not exit the JVM.
     * <p/>
     * A CLI instance can be used only once.
     *
     * @param args options and arguments for the Oozie CLI.
     * @return '0' (success), '-1' (failure).
     */
    public synchronized int run(String[] args) {
        if (used) {
            throw new IllegalStateException("CLI instance already used");
        }
        used = true;
        final CLIParser parser = getCLIParser();
        try {
            final CLIParser.Command command = parser.parse(args);

            String doAsUser = command.getCommandLine().getOptionValue(DO_AS_OPTION);

            if (doAsUser != null) {
                OozieClient.doAs(doAsUser, new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        processCommand(parser, command);
                        return null;
                    }
                });
            }
            else {
                processCommand(parser, command);
            }
            return 0;
        }
        catch (OozieCLIException ex) {
            System.err.println("Error: " + ex.getMessage());
            return -1;
        }
        catch (ParseException ex) {
            System.err.println("Invalid sub-command: " + ex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            return -1;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
            return -1;
        }
    }

    @VisibleForTesting
    public CLIParser getCLIParser(){
        CLIParser parser = new CLIParser(OOZIE_OPTION, getCLIHelp());
        parser.addCommand(HELP_CMD, "", "display usage for all commands or specified command", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show client version", new Options(), false);
        parser.addCommand(JOB_CMD, "", "job operations", createJobOptions(), false);
        parser.addCommand(JOBS_CMD, "", "jobs status", createJobsOptions(), false);
        parser.addCommand(ADMIN_CMD, "", "admin operations", createAdminOptions(), false);
        parser.addCommand(VALIDATE_CMD, "", "validate a workflow XML file", new Options(), true);
        parser.addCommand(SLA_CMD, "", "sla operations (Deprecated with Oozie 4.0)", createSlaOptions(), false);
        parser.addCommand(PIG_CMD, "-X ", "submit a pig job, everything after '-X' are pass-through parameters to pig, any '-D' "
                + "arguments after '-X' are put in <configuration>", createScriptLanguageOptions(PIG_CMD), true);
        parser.addCommand(HIVE_CMD, "-X ", "submit a hive job, everything after '-X' are pass-through parameters to hive, any '-D' "
                + "arguments after '-X' are put in <configuration>", createScriptLanguageOptions(HIVE_CMD), true);
        parser.addCommand(SQOOP_CMD, "-X ", "submit a sqoop job, everything after '-X' are pass-through parameters " +
                "to sqoop, any '-D' arguments after '-X' are put in <configuration>", createSqoopCLIOptions(), true);
        parser.addCommand(INFO_CMD, "", "get more detailed info about specific topics", createInfoOptions(), false);
        parser.addCommand(MR_CMD, "", "submit a mapreduce job", createMROptions(), false);
        return parser;
    }

    public void processCommand(CLIParser parser, CLIParser.Command command) throws Exception {
        if (command.getName().equals(HELP_CMD)) {
            parser.showHelp(command.getCommandLine());
        }
        else if (command.getName().equals(JOB_CMD)) {
            jobCommand(command.getCommandLine());
        }
        else if (command.getName().equals(JOBS_CMD)) {
            jobsCommand(command.getCommandLine());
        }
        else if (command.getName().equals(ADMIN_CMD)) {
            adminCommand(command.getCommandLine());
        }
        else if (command.getName().equals(VERSION_CMD)) {
            versionCommand();
        }
        else if (command.getName().equals(VALIDATE_CMD)) {
            validateCommand(command.getCommandLine());
        }
        else if (command.getName().equals(SLA_CMD)) {
            slaCommand(command.getCommandLine());
        }
        else if (command.getName().equals(PIG_CMD)) {
            scriptLanguageCommand(command.getCommandLine(), PIG_CMD);
        }
        else if (command.getName().equals(HIVE_CMD)) {
            scriptLanguageCommand(command.getCommandLine(), HIVE_CMD);
        }
        else if (command.getName().equals(SQOOP_CMD)) {
            sqoopCommand(command.getCommandLine());
        }
        else if (command.getName().equals(INFO_CMD)) {
            infoCommand(command.getCommandLine());
        }
        else if (command.getName().equals(MR_CMD)){
            mrCommand(command.getCommandLine());
        }
    }
    protected String getOozieUrl(CommandLine commandLine) {
        String url = commandLine.getOptionValue(OOZIE_OPTION);
        if (url == null) {
            url = System.getenv(ENV_OOZIE_URL);
            if (url == null) {
                throw new IllegalArgumentException(
                        "Oozie URL is not available neither in command option or in the environment");
            }
        }
        return url;
    }

    private String getTimeZoneId(CommandLine commandLine)
    {
        if (commandLine.hasOption(LOCAL_TIME_OPTION)) {
            return null;
        }
        if (commandLine.hasOption(TIME_ZONE_OPTION)) {
            return commandLine.getOptionValue(TIME_ZONE_OPTION);
        }
        String timeZoneId = System.getenv(ENV_OOZIE_TIME_ZONE);
        if (timeZoneId != null) {
            return timeZoneId;
        }
        return "GMT";
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private Properties parse(InputStream is, Properties conf) throws IOException {
        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            // ignore all comments inside the xml file
            docBuilderFactory.setIgnoringComments(true);
            DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
            Document doc = builder.parse(is);
            return parseDocument(doc, conf);
        }
        catch (SAXException e) {
            throw new IOException(e);
        }
        catch (ParserConfigurationException e) {
            throw new IOException(e);
        }
    }

    // Canibalized from Hadoop <code>Configuration.loadResource()</code>.
    private Properties parseDocument(Document doc, Properties conf) throws IOException {
        try {
            Element root = doc.getDocumentElement();
            if (!"configuration".equals(root.getTagName())) {
                throw new RuntimeException("bad conf file: top-level element not <configuration>");
            }
            NodeList props = root.getChildNodes();
            for (int i = 0; i < props.getLength(); i++) {
                Node propNode = props.item(i);
                if (!(propNode instanceof Element)) {
                    continue;
                }
                Element prop = (Element) propNode;
                if (!"property".equals(prop.getTagName())) {
                    throw new RuntimeException("bad conf file: element not <property>");
                }
                NodeList fields = prop.getChildNodes();
                String attr = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node fieldNode = fields.item(j);
                    if (!(fieldNode instanceof Element)) {
                        continue;
                    }
                    Element field = (Element) fieldNode;
                    if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
                        attr = ((Text) field.getFirstChild()).getData();
                    }
                    if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
                        value = ((Text) field.getFirstChild()).getData();
                    }
                }

                if (attr != null && value != null) {
                    conf.setProperty(attr, value);
                }
            }
            return conf;
        }
        catch (DOMException e) {
            throw new IOException(e);
        }
    }

    private Properties getConfiguration(OozieClient wc, CommandLine commandLine) throws IOException {
        if (!isConfigurationSpecified(wc, commandLine)) {
            throw new IOException("configuration is not specified");
        }
        Properties conf = wc.createConfiguration();
        String configFile = commandLine.getOptionValue(CONFIG_OPTION);
        if (configFile != null) {
            File file = new File(configFile);
            if (!file.exists()) {
                throw new IOException("configuration file [" + configFile + "] not found");
            }
            if (configFile.endsWith(".properties")) {
                conf.load(new FileReader(file));
            }
            else if (configFile.endsWith(".xml")) {
                parse(new FileInputStream(configFile), conf);
            }
            else {
                throw new IllegalArgumentException("configuration must be a '.properties' or a '.xml' file");
            }
        }
        if (commandLine.hasOption("D")) {
            Properties commandLineProperties = commandLine.getOptionProperties("D");
            conf.putAll(commandLineProperties);
        }
        return conf;
    }

    /**
     * Check if configuration has specified
     * @param wc
     * @param commandLine
     * @return
     * @throws IOException
     */
    private boolean isConfigurationSpecified(OozieClient wc, CommandLine commandLine) throws IOException {
        boolean isConf = false;
        String configFile = commandLine.getOptionValue(CONFIG_OPTION);
        if (configFile == null) {
            isConf = false;
        }
        else {
            isConf = new File(configFile).exists();
        }
        if (commandLine.hasOption("D")) {
            isConf = true;
        }
        return isConf;
    }

    /**
     * @param commandLine command line string.
     * @return change value specified by -value.
     * @throws OozieCLIException
     */
	private String getChangeValue(CommandLine commandLine) throws OozieCLIException {
        String changeValue = commandLine.getOptionValue(CHANGE_VALUE_OPTION);

        if (changeValue == null) {
            throw new OozieCLIException("-value option needs to be specified for -change option");
        }

        return changeValue;
    }

    protected void addHeader(OozieClient wc) {
        for (Map.Entry entry : System.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(WS_HEADER_PREFIX)) {
                String header = key.substring(WS_HEADER_PREFIX.length());
                wc.setHeader(header, (String) entry.getValue());
            }
        }
    }

    /**
     * Get auth option from command line
     *
     * @param commandLine the command line object
     * @return auth option
     */
    protected String getAuthOption(CommandLine commandLine) {
        String authOpt = commandLine.getOptionValue(AUTH_OPTION);
        if (authOpt == null) {
            authOpt = System.getenv(ENV_OOZIE_AUTH);
        }
        if (commandLine.hasOption(DEBUG_OPTION)) {
            System.out.println(" Auth type : " + authOpt);
        }
        return authOpt;
    }

    /**
     * Create a OozieClient.
     * <p/>
     * It injects any '-Dheader:' as header to the the {@link org.apache.oozie.client.OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured eXtended workflow client.
     * @throws OozieCLIException thrown if the OozieClient could not be configured.
     */
    protected OozieClient createOozieClient(CommandLine commandLine) throws OozieCLIException {
        return createXOozieClient(commandLine);
    }

    /**
     * Create a XOozieClient.
     * <p/>
     * It injects any '-Dheader:' as header to the the {@link org.apache.oozie.client.OozieClient}.
     *
     * @param commandLine the parsed command line options.
     * @return a pre configured eXtended workflow client.
     * @throws OozieCLIException thrown if the XOozieClient could not be configured.
     */
    protected XOozieClient createXOozieClient(CommandLine commandLine) throws OozieCLIException {
        XOozieClient wc = new AuthOozieClient(getOozieUrl(commandLine), getAuthOption(commandLine));
        addHeader(wc);
        setDebugMode(wc,commandLine.hasOption(DEBUG_OPTION));
        setRetryCount(wc);
        return wc;
    }

    protected void setDebugMode(OozieClient wc, boolean debugOpt) {

        String debug = System.getenv(ENV_OOZIE_DEBUG);
        if (debug != null && !debug.isEmpty()) {
            int debugVal = 0;
            try {
                debugVal = Integer.parseInt(debug.trim());
            }
            catch (Exception ex) {
                System.out.println("Unable to parse the debug settings. May be not an integer [" + debug + "]");
                ex.printStackTrace();
            }
            wc.setDebugMode(debugVal);
        }
        else if(debugOpt){  // CLI argument "-debug" used
            wc.setDebugMode(1);
        }
    }

    protected void setRetryCount(OozieClient wc) {
        String retryCount = System.getProperty(OOZIE_RETRY_COUNT);
        if (retryCount != null && !retryCount.isEmpty()) {
            try {
                int retry = Integer.parseInt(retryCount.trim());
                wc.setRetryCount(retry);
            }
            catch (Exception ex) {
                System.err.println("Unable to parse the retry settings. May be not an integer [" + retryCount + "]");
                ex.printStackTrace();
            }
        }
    }

    private static String JOB_ID_PREFIX = "job: ";

    private void jobCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        try {
            if (options.contains(SUBMIT_OPTION)) {
                System.out.println(JOB_ID_PREFIX + wc.submit(getConfiguration(wc, commandLine)));
            }
            else if (options.contains(START_OPTION)) {
                wc.start(commandLine.getOptionValue(START_OPTION));
            }
            else if (options.contains(DRYRUN_OPTION) && !options.contains(UPDATE_OPTION)) {
                String dryrunStr = wc.dryrun(getConfiguration(wc, commandLine));
                if (dryrunStr.equals("OK")) {  // workflow
                    System.out.println("OK");
                } else {                        // coordinator
                    String[] dryrunStrs = dryrunStr.split("action for new instance");
                    int arraysize = dryrunStrs.length;
                    System.out.println("***coordJob after parsing: ***");
                    System.out.println(dryrunStrs[0]);
                    int aLen = dryrunStrs.length - 1;
                    if (aLen < 0) {
                        aLen = 0;
                    }
                    System.out.println("***total coord actions is " + aLen + " ***");
                    for (int i = 1; i <= arraysize - 1; i++) {
                        System.out.println(RULER);
                        System.out.println("coordAction instance: " + i + ":");
                        System.out.println(dryrunStrs[i]);
                    }
                }
            }
            else if (options.contains(SUSPEND_OPTION)) {
                wc.suspend(commandLine.getOptionValue(SUSPEND_OPTION));
            }
            else if (options.contains(RESUME_OPTION)) {
                wc.resume(commandLine.getOptionValue(RESUME_OPTION));
            }
            else if (options.contains(IGNORE_OPTION)) {
                String ignoreScope = null;
                if (options.contains(ACTION_OPTION)) {
                    ignoreScope = commandLine.getOptionValue(ACTION_OPTION);
                    if (ignoreScope == null || ignoreScope.isEmpty()) {
                        throw new OozieCLIException("-" + ACTION_OPTION + " is empty");
                    }
                }
                printCoordActionsStatus(wc.ignore(commandLine.getOptionValue(IGNORE_OPTION), ignoreScope));
            }
            else if (options.contains(KILL_OPTION)) {
                if (commandLine.getOptionValue(KILL_OPTION).contains("-C")
                        && (options.contains(DATE_OPTION) || options.contains(ACTION_OPTION))) {
                    String coordJobId = commandLine.getOptionValue(KILL_OPTION);
                    String scope = null;
                    String rangeType = null;
                    if (options.contains(DATE_OPTION) && options.contains(ACTION_OPTION)) {
                        throw new OozieCLIException("Invalid options provided for rerun: either" + DATE_OPTION + " or "
                                + ACTION_OPTION + " expected. Don't use both at the same time.");
                    }
                    if (options.contains(DATE_OPTION)) {
                        rangeType = RestConstants.JOB_COORD_SCOPE_DATE;
                        scope = commandLine.getOptionValue(DATE_OPTION);
                    }
                    else if (options.contains(ACTION_OPTION)) {
                        rangeType = RestConstants.JOB_COORD_SCOPE_ACTION;
                        scope = commandLine.getOptionValue(ACTION_OPTION);
                    }
                    else {
                        throw new OozieCLIException("Invalid options provided for rerun: " + DATE_OPTION + " or "
                                + ACTION_OPTION + " expected.");
                    }
                    printCoordActions(wc.kill(coordJobId, rangeType, scope));
                }
                else {
                    wc.kill(commandLine.getOptionValue(KILL_OPTION));
                }
            }
            else if (options.contains(CHANGE_OPTION)) {
                wc.change(commandLine.getOptionValue(CHANGE_OPTION), getChangeValue(commandLine));
            }
            else if (options.contains(RUN_OPTION)) {
                System.out.println(JOB_ID_PREFIX + wc.run(getConfiguration(wc, commandLine)));
            }
            else if (options.contains(RERUN_OPTION)) {
                if (commandLine.getOptionValue(RERUN_OPTION).contains("-W")) {
                    if (isConfigurationSpecified(wc, commandLine)) {
                        wc.reRun(commandLine.getOptionValue(RERUN_OPTION), getConfiguration(wc, commandLine));
                    }
                    else {
                        wc.reRun(commandLine.getOptionValue(RERUN_OPTION), new Properties());
                    }
                }
                else if (commandLine.getOptionValue(RERUN_OPTION).contains("-B")) {
                    String bundleJobId = commandLine.getOptionValue(RERUN_OPTION);
                    String coordScope = null;
                    String dateScope = null;
                    boolean refresh = false;
                    boolean noCleanup = false;
                    if (options.contains(ACTION_OPTION)) {
                        throw new OozieCLIException("Invalid options provided for bundle rerun. " + ACTION_OPTION
                                + " is not valid for bundle rerun");
                    }
                    if (options.contains(DATE_OPTION)) {
                        dateScope = commandLine.getOptionValue(DATE_OPTION);
                    }

                    if (options.contains(COORD_OPTION)) {
                        coordScope = commandLine.getOptionValue(COORD_OPTION);
                    }

                    if (options.contains(RERUN_REFRESH_OPTION)) {
                        refresh = true;
                    }
                    if (options.contains(RERUN_NOCLEANUP_OPTION)) {
                        noCleanup = true;
                    }
                    wc.reRunBundle(bundleJobId, coordScope, dateScope, refresh, noCleanup);
                    if (coordScope != null && !coordScope.isEmpty()) {
                        System.out.println("Coordinators [" + coordScope + "] of bundle " + bundleJobId
                                + " are scheduled to rerun on date ranges [" + dateScope + "].");
                    }
                    else {
                        System.out.println("All coordinators of bundle " + bundleJobId
                                + " are scheduled to rerun on the date ranges [" + dateScope + "].");
                    }
                }
                else {
                    String coordJobId = commandLine.getOptionValue(RERUN_OPTION);
                    String scope = null;
                    String rerunType = null;
                    boolean refresh = false;
                    boolean noCleanup = false;
                    boolean failed = false;
                    if (options.contains(DATE_OPTION) && options.contains(ACTION_OPTION)) {
                        throw new OozieCLIException("Invalid options provided for rerun: either" + DATE_OPTION + " or "
                                + ACTION_OPTION + " expected. Don't use both at the same time.");
                    }
                    if (options.contains(DATE_OPTION)) {
                        rerunType = RestConstants.JOB_COORD_SCOPE_DATE;
                        scope = commandLine.getOptionValue(DATE_OPTION);
                    }
                    else if (options.contains(ACTION_OPTION)) {
                        rerunType = RestConstants.JOB_COORD_SCOPE_ACTION;
                        scope = commandLine.getOptionValue(ACTION_OPTION);
                    }
                    else {
                        throw new OozieCLIException("Invalid options provided for rerun: " + DATE_OPTION + " or "
                                + ACTION_OPTION + " expected.");
                    }
                    if (options.contains(RERUN_REFRESH_OPTION)) {
                        refresh = true;
                    }
                    if (options.contains(RERUN_NOCLEANUP_OPTION)) {
                        noCleanup = true;
                    }

                    Properties props = null;
                    if(isConfigurationSpecified(wc, commandLine)) {
                        props = getConfiguration(wc, commandLine);
                    }

                    if (options.contains(RERUN_FAILED_OPTION)) {
                        failed = true;
                    }

                    printCoordActions(wc.reRunCoord(coordJobId, rerunType, scope, refresh, noCleanup, failed, props));
                }
            }
            else if (options.contains(INFO_OPTION)) {
                String timeZoneId = getTimeZoneId(commandLine);
                final String optionValue = commandLine.getOptionValue(INFO_OPTION);
                if (optionValue.endsWith("-B")) {
                    String filter = commandLine.getOptionValue(FILTER_OPTION);
                    if (filter != null) {
                        throw new OozieCLIException("Filter option is currently not supported for a Bundle job");
                    }
                    printBundleJob(wc.getBundleJobInfo(optionValue), timeZoneId,
                            options.contains(VERBOSE_OPTION));
                }
                else if (optionValue.endsWith("-C")) {
                    String s = commandLine.getOptionValue(OFFSET_OPTION);
                    int start = Integer.parseInt((s != null) ? s : "-1");
                    s = commandLine.getOptionValue(LEN_OPTION);
                    int len = Integer.parseInt((s != null) ? s : "-1");
                    String filter = commandLine.getOptionValue(FILTER_OPTION);
                    String order = commandLine.getOptionValue(ORDER_OPTION);
                    printCoordJob(wc.getCoordJobInfo(optionValue, filter, start, len, order), timeZoneId,
                            options.contains(VERBOSE_OPTION));
                }
                else if (optionValue.contains("-C@")) {
                    if (options.contains(ALL_WORKFLOWS_FOR_COORD_ACTION)) {
                        printWfsForCoordAction(wc.getWfsForCoordAction(optionValue), timeZoneId);
                    }
                    else {
                        String filter = commandLine.getOptionValue(FILTER_OPTION);
                        if (filter != null) {
                            throw new OozieCLIException("Filter option is not supported for a Coordinator action");
                        }
                        printCoordAction(wc.getCoordActionInfo(optionValue), timeZoneId);
                    }
                }
                else if (optionValue.contains("-W@")) {
                    String filter = commandLine.getOptionValue(FILTER_OPTION);
                    if (filter != null) {
                        throw new OozieCLIException("Filter option is not supported for a Workflow action");
                    }
                    printWorkflowAction(wc.getWorkflowActionInfo(optionValue), timeZoneId,
                            options.contains(VERBOSE_OPTION));
                }
                else {
                    String filter = commandLine.getOptionValue(FILTER_OPTION);
                    if (filter != null) {
                        throw new OozieCLIException("Filter option is currently not supported for a Workflow job");
                    }
                    String s = commandLine.getOptionValue(OFFSET_OPTION);
                    int start = Integer.parseInt((s != null) ? s : "0");
                    s = commandLine.getOptionValue(LEN_OPTION);
                    String jobtype = commandLine.getOptionValue(JOBTYPE_OPTION);
                    jobtype = (jobtype != null) ? jobtype : "wf";
                    int len = Integer.parseInt((s != null) ? s : "0");
                    printJob(wc.getJobInfo(optionValue, start, len), timeZoneId,
                            options.contains(VERBOSE_OPTION));
                }
            }
            else if (options.contains(LOG_OPTION)) {
                PrintStream ps = System.out;
                String logFilter = null;
                if (options.contains(RestConstants.LOG_FILTER_OPTION)) {
                    logFilter = commandLine.getOptionValue(RestConstants.LOG_FILTER_OPTION);
                }
                if (commandLine.getOptionValue(LOG_OPTION).contains("-C")) {
                    String logRetrievalScope = null;
                    String logRetrievalType = null;
                    if (options.contains(ACTION_OPTION)) {
                        logRetrievalType = RestConstants.JOB_LOG_ACTION;
                        logRetrievalScope = commandLine.getOptionValue(ACTION_OPTION);
                    }
                    if (options.contains(DATE_OPTION)) {
                        logRetrievalType = RestConstants.JOB_LOG_DATE;
                        logRetrievalScope = commandLine.getOptionValue(DATE_OPTION);
                    }
                    try {
                        wc.getJobLog(commandLine.getOptionValue(LOG_OPTION), logRetrievalType, logRetrievalScope,
                                logFilter, ps);
                    }
                    finally {
                        ps.close();
                    }
                }
                else {
                    if (!options.contains(ACTION_OPTION) && !options.contains(DATE_OPTION)) {
                        wc.getJobLog(commandLine.getOptionValue(LOG_OPTION), null, null, logFilter, ps);
                    }
                    else {
                        throw new OozieCLIException("Invalid options provided for log retrieval. " + ACTION_OPTION
                                + " and " + DATE_OPTION + " are valid only for coordinator job log retrieval");
                    }
                }
            }
            else if (options.contains(ERROR_LOG_OPTION)) {
                PrintStream ps = System.out;
                try {
                    wc.getJobErrorLog(commandLine.getOptionValue(ERROR_LOG_OPTION), ps);
                }
                finally {
                    ps.close();
                }
            }
            else if (options.contains(AUDIT_LOG_OPTION)) {
                PrintStream ps = System.out;
                try {
                    wc.getJobAuditLog(commandLine.getOptionValue(AUDIT_LOG_OPTION), ps);
                }
                finally {
                    ps.close();
                }
            }
            else if (options.contains(DEFINITION_OPTION)) {
                System.out.println(wc.getJobDefinition(commandLine.getOptionValue(DEFINITION_OPTION)));
            }
            else if (options.contains(CONFIG_CONTENT_OPTION)) {
                if (commandLine.getOptionValue(CONFIG_CONTENT_OPTION).endsWith("-C")) {
                    System.out.println(wc.getCoordJobInfo(commandLine.getOptionValue(CONFIG_CONTENT_OPTION)).getConf());
                }
                else if (commandLine.getOptionValue(CONFIG_CONTENT_OPTION).endsWith("-W")) {
                    System.out.println(wc.getJobInfo(commandLine.getOptionValue(CONFIG_CONTENT_OPTION)).getConf());
                }
                else if (commandLine.getOptionValue(CONFIG_CONTENT_OPTION).endsWith("-B")) {
                    System.out
                            .println(wc.getBundleJobInfo(commandLine.getOptionValue(CONFIG_CONTENT_OPTION)).getConf());
                }
                else {
                    System.out.println("ERROR:  job id [" + commandLine.getOptionValue(CONFIG_CONTENT_OPTION)
                            + "] doesn't end with either C or W or B");
                }
            }
            else if (options.contains(UPDATE_OPTION)) {
                String coordJobId = commandLine.getOptionValue(UPDATE_OPTION);
                Properties conf = null;

                String dryrun = "";
                String showdiff = "";

                if (commandLine.getOptionValue(CONFIG_OPTION) != null) {
                    conf = getConfiguration(wc, commandLine);
                }
                if (options.contains(DRYRUN_OPTION)) {
                    dryrun = "true";
                }
                if (commandLine.getOptionValue(SHOWDIFF_OPTION) != null) {
                    showdiff = commandLine.getOptionValue(SHOWDIFF_OPTION);
                }
                if (conf == null) {
                    System.out.println(wc.updateCoord(coordJobId, dryrun, showdiff));
                }
                else {
                    System.out.println(wc.updateCoord(coordJobId, conf, dryrun, showdiff));
                }
            }
            else if (options.contains(POLL_OPTION)) {
                String jobId = commandLine.getOptionValue(POLL_OPTION);
                int timeout = 30;
                int interval = 5;
                String timeoutS = commandLine.getOptionValue(TIMEOUT_OPTION);
                if (timeoutS != null) {
                    timeout = Integer.parseInt(timeoutS);
                }
                String intervalS = commandLine.getOptionValue(INTERVAL_OPTION);
                if (intervalS != null) {
                    interval = Integer.parseInt(intervalS);
                }
                boolean verbose = commandLine.hasOption(VERBOSE_OPTION);
                wc.pollJob(jobId, timeout, interval, verbose);
            }
            else if (options.contains(SLA_ENABLE_ALERT)) {
                slaAlertCommand(commandLine.getOptionValue(SLA_ENABLE_ALERT), wc, commandLine, options);
            }
            else if (options.contains(SLA_DISABLE_ALERT)) {
                slaAlertCommand(commandLine.getOptionValue(SLA_DISABLE_ALERT), wc, commandLine, options);
            }
            else if (options.contains(SLA_CHANGE)) {
                slaAlertCommand(commandLine.getOptionValue(SLA_CHANGE), wc, commandLine, options);
            }
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    @VisibleForTesting
    void printCoordJob(CoordinatorJob coordJob, String timeZoneId, boolean verbose) {
        System.out.println("Job ID : " + coordJob.getId());

        System.out.println(RULER);

        List<CoordinatorAction> actions = coordJob.getActions();
        System.out.println("Job Name    : " + maskIfNull(coordJob.getAppName()));
        System.out.println("App Path    : " + maskIfNull(coordJob.getAppPath()));
        System.out.println("Status      : " + coordJob.getStatus());
        System.out.println("Start Time  : " + maskDate(coordJob.getStartTime(), timeZoneId, false));
        System.out.println("End Time    : " + maskDate(coordJob.getEndTime(), timeZoneId, false));
        System.out.println("Pause Time  : " + maskDate(coordJob.getPauseTime(), timeZoneId, false));
        System.out.println("Concurrency : " + coordJob.getConcurrency());
        System.out.println(RULER);

        if (verbose) {
            System.out.println("ID" + VERBOSE_DELIMITER + "Action Number" + VERBOSE_DELIMITER + "Console URL"
                    + VERBOSE_DELIMITER + "Error Code" + VERBOSE_DELIMITER + "Error Message" + VERBOSE_DELIMITER
                    + "External ID" + VERBOSE_DELIMITER + "External Status" + VERBOSE_DELIMITER + "Job ID"
                    + VERBOSE_DELIMITER + "Tracker URI" + VERBOSE_DELIMITER + "Created" + VERBOSE_DELIMITER
                    + "Nominal Time" + VERBOSE_DELIMITER + "Status" + VERBOSE_DELIMITER + "Last Modified"
                    + VERBOSE_DELIMITER + "Missing Dependencies");
            System.out.println(RULER);

            for (CoordinatorAction action : actions) {
                System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER + action.getActionNumber()
                        + VERBOSE_DELIMITER + maskIfNull(action.getConsoleUrl()) + VERBOSE_DELIMITER
                        + maskIfNull(action.getErrorCode()) + VERBOSE_DELIMITER + maskIfNull(action.getErrorMessage())
                        + VERBOSE_DELIMITER + maskIfNull(action.getExternalId()) + VERBOSE_DELIMITER
                        + maskIfNull(action.getExternalStatus()) + VERBOSE_DELIMITER + maskIfNull(action.getJobId())
                        + VERBOSE_DELIMITER + maskIfNull(action.getTrackerUri()) + VERBOSE_DELIMITER
                        + maskDate(action.getCreatedTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                        + maskDate(action.getNominalTime(), timeZoneId, verbose) + action.getStatus() + VERBOSE_DELIMITER
                        + maskDate(action.getLastModifiedTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                        + maskIfNull(getFirstMissingDependencies(action)));

                System.out.println(RULER);
            }
        }
        else {
            System.out.println(String.format(COORD_ACTION_FORMATTER, "ID", "Status", "Ext ID", "Err Code", "Created",
                    "Nominal Time", "Last Mod"));

            for (CoordinatorAction action : actions) {
                System.out.println(String.format(COORD_ACTION_FORMATTER, maskIfNull(action.getId()),
                        action.getStatus(), maskIfNull(action.getExternalId()), maskIfNull(action.getErrorCode()),
                        maskDate(action.getCreatedTime(), timeZoneId, verbose), maskDate(action.getNominalTime(), timeZoneId, verbose),
                        maskDate(action.getLastModifiedTime(), timeZoneId, verbose)));

                System.out.println(RULER);
            }
        }
    }

    @VisibleForTesting
    void printBundleJob(BundleJob bundleJob, String timeZoneId, boolean verbose) {
        System.out.println("Job ID : " + bundleJob.getId());

        System.out.println(RULER);

        List<CoordinatorJob> coordinators = bundleJob.getCoordinators();
        System.out.println("Job Name : " + maskIfNull(bundleJob.getAppName()));
        System.out.println("App Path : " + maskIfNull(bundleJob.getAppPath()));
        System.out.println("Status   : " + bundleJob.getStatus());
        System.out.println("Kickoff time   : " + bundleJob.getKickoffTime());
        System.out.println(RULER);

        System.out.println(String.format(BUNDLE_COORD_JOBS_FORMATTER, "Job ID", "Status", "Freq", "Unit", "Started",
                "Next Materialized"));
        System.out.println(RULER);

        for (CoordinatorJob job : coordinators) {
            System.out.println(String.format(BUNDLE_COORD_JOBS_FORMATTER, maskIfNull(job.getId()), job.getStatus(),
                    job.getFrequency(), job.getTimeUnit(), maskDate(job.getStartTime(), timeZoneId, verbose),
                    maskDate(job.getNextMaterializedTime(), timeZoneId, verbose)));

            System.out.println(RULER);
        }
    }

    @VisibleForTesting
    void printCoordAction(CoordinatorAction coordAction, String timeZoneId) {
        System.out.println("ID : " + maskIfNull(coordAction.getId()));

        System.out.println(RULER);

        System.out.println("Action Number        : " + coordAction.getActionNumber());
        System.out.println("Console URL          : " + maskIfNull(coordAction.getConsoleUrl()));
        System.out.println("Error Code           : " + maskIfNull(coordAction.getErrorCode()));
        System.out.println("Error Message        : " + maskIfNull(coordAction.getErrorMessage()));
        System.out.println("External ID          : " + maskIfNull(coordAction.getExternalId()));
        System.out.println("External Status      : " + maskIfNull(coordAction.getExternalStatus()));
        System.out.println("Job ID               : " + maskIfNull(coordAction.getJobId()));
        System.out.println("Tracker URI          : " + maskIfNull(coordAction.getTrackerUri()));
        System.out.println("Created              : " + maskDate(coordAction.getCreatedTime(), timeZoneId, false));
        System.out.println("Nominal Time         : " + maskDate(coordAction.getNominalTime(), timeZoneId, false));
        System.out.println("Status               : " + coordAction.getStatus());
        System.out.println("Last Modified        : " + maskDate(coordAction.getLastModifiedTime(), timeZoneId, false));
        System.out.println("First Missing Dependency : " + maskIfNull(getFirstMissingDependencies(coordAction)));

        System.out.println(RULER);
    }

    private void printCoordActions(List<CoordinatorAction> actions) {
        if (actions != null && actions.size() > 0) {
            System.out.println("Action ID" + VERBOSE_DELIMITER + "Nominal Time");
            System.out.println(RULER);
            for (CoordinatorAction action : actions) {
                System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER
                        + maskDate(action.getNominalTime(), null,false));
            }
        }
        else {
            System.out.println("No Actions match your criteria!");
        }
    }

    private void printCoordActionsStatus(List<CoordinatorAction> actions) {
        if (actions != null && actions.size() > 0) {
            System.out.println("Action ID" + VERBOSE_DELIMITER + "Nominal Time" + VERBOSE_DELIMITER + "Status");
            System.out.println(RULER);
            for (CoordinatorAction action : actions) {
                System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER
                        + maskDate(action.getNominalTime(), null, false) + VERBOSE_DELIMITER
                        + maskIfNull(action.getStatus().name()));
            }
        }
    }

    @VisibleForTesting
    void printWorkflowAction(WorkflowAction action, String timeZoneId, boolean verbose) {

        System.out.println("ID : " + maskIfNull(action.getId()));

        System.out.println(RULER);

        System.out.println("Console URL       : " + maskIfNull(action.getConsoleUrl()));
        System.out.println("Error Code        : " + maskIfNull(action.getErrorCode()));
        System.out.println("Error Message     : " + maskIfNull(action.getErrorMessage()));
        System.out.println("External ID       : " + maskIfNull(action.getExternalId()));
        System.out.println("External Status   : " + maskIfNull(action.getExternalStatus()));
        System.out.println("Name              : " + maskIfNull(action.getName()));
        System.out.println("Retries           : " + action.getRetries());
        System.out.println("Tracker URI       : " + maskIfNull(action.getTrackerUri()));
        System.out.println("Type              : " + maskIfNull(action.getType()));
        System.out.println("Started           : " + maskDate(action.getStartTime(), timeZoneId, verbose));
        System.out.println("Status            : " + action.getStatus());
        System.out.println("Ended             : " + maskDate(action.getEndTime(), timeZoneId, verbose));

        if (verbose) {
            System.out.println("External Stats    : " + action.getStats());
            System.out.println("External ChildIDs : " + action.getExternalChildIDs());
        }

        System.out.println(RULER);
    }

    private static final String WORKFLOW_JOBS_FORMATTER = "%-41s%-13s%-10s%-10s%-10s%-24s%-24s";
    private static final String COORD_JOBS_FORMATTER = "%-41s%-15s%-10s%-5s%-13s%-24s%-24s";
    private static final String BUNDLE_JOBS_FORMATTER = "%-41s%-15s%-10s%-20s%-20s%-13s%-13s";
    private static final String BUNDLE_COORD_JOBS_FORMATTER = "%-41s%-15s%-5s%-13s%-24s%-24s";

    private static final String WORKFLOW_ACTION_FORMATTER = "%-78s%-10s%-23s%-11s%-10s";
    private static final String COORD_ACTION_FORMATTER = "%-43s%-10s%-37s%-10s%-21s%-21s";
    private static final String BULK_RESPONSE_FORMATTER = "%-13s%-38s%-13s%-41s%-10s%-38s%-21s%-38s";

    @VisibleForTesting
    void printJob(WorkflowJob job, String timeZoneId, boolean verbose) throws IOException {
        System.out.println("Job ID : " + maskIfNull(job.getId()));

        System.out.println(RULER);

        System.out.println("Workflow Name : " + maskIfNull(job.getAppName()));
        System.out.println("App Path      : " + maskIfNull(job.getAppPath()));
        System.out.println("Status        : " + job.getStatus());
        System.out.println("Run           : " + job.getRun());
        System.out.println("User          : " + maskIfNull(job.getUser()));
        System.out.println("Group         : " + maskIfNull(job.getGroup()));
        System.out.println("Created       : " + maskDate(job.getCreatedTime(), timeZoneId, verbose));
        System.out.println("Started       : " + maskDate(job.getStartTime(), timeZoneId, verbose));
        System.out.println("Last Modified : " + maskDate(job.getLastModifiedTime(), timeZoneId, verbose));
        System.out.println("Ended         : " + maskDate(job.getEndTime(), timeZoneId, verbose));
        System.out.println("CoordAction ID: " + maskIfNull(job.getParentId()));

        List<WorkflowAction> actions = job.getActions();

        if (actions != null && actions.size() > 0) {
            System.out.println();
            System.out.println("Actions");
            System.out.println(RULER);

            if (verbose) {
                System.out.println("ID" + VERBOSE_DELIMITER + "Console URL" + VERBOSE_DELIMITER + "Error Code"
                        + VERBOSE_DELIMITER + "Error Message" + VERBOSE_DELIMITER + "External ID" + VERBOSE_DELIMITER
                        + "External Status" + VERBOSE_DELIMITER + "Name" + VERBOSE_DELIMITER + "Retries"
                        + VERBOSE_DELIMITER + "Tracker URI" + VERBOSE_DELIMITER + "Type" + VERBOSE_DELIMITER
                        + "Started" + VERBOSE_DELIMITER + "Status" + VERBOSE_DELIMITER + "Ended");
                System.out.println(RULER);

                for (WorkflowAction action : job.getActions()) {
                    System.out.println(maskIfNull(action.getId()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getConsoleUrl()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getErrorCode()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getErrorMessage()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getExternalId()) + VERBOSE_DELIMITER
                            + maskIfNull(action.getExternalStatus()) + VERBOSE_DELIMITER + maskIfNull(action.getName())
                            + VERBOSE_DELIMITER + action.getRetries() + VERBOSE_DELIMITER
                            + maskIfNull(action.getTrackerUri()) + VERBOSE_DELIMITER + maskIfNull(action.getType())
                            + VERBOSE_DELIMITER + maskDate(action.getStartTime(), timeZoneId, verbose)
                            + VERBOSE_DELIMITER + action.getStatus() + VERBOSE_DELIMITER
                            + maskDate(action.getEndTime(), timeZoneId, verbose));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(WORKFLOW_ACTION_FORMATTER, "ID", "Status", "Ext ID", "Ext Status",
                        "Err Code"));

                System.out.println(RULER);

                for (WorkflowAction action : job.getActions()) {
                    System.out.println(String.format(WORKFLOW_ACTION_FORMATTER, maskIfNull(action.getId()), action
                            .getStatus(), maskIfNull(action.getExternalId()), maskIfNull(action.getExternalStatus()),
                            maskIfNull(action.getErrorCode())));

                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println(RULER);
        }

        System.out.println();
    }

    private void jobsCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        String filter = commandLine.getOptionValue(FILTER_OPTION);
        String s = commandLine.getOptionValue(OFFSET_OPTION);
        int start = Integer.parseInt((s != null) ? s : "0");
        s = commandLine.getOptionValue(LEN_OPTION);
        String jobtype = commandLine.getOptionValue(JOBTYPE_OPTION);
        String timeZoneId = getTimeZoneId(commandLine);
        jobtype = (jobtype != null) ? jobtype : "wf";
        int len = Integer.parseInt((s != null) ? s : "0");
        String bulkFilterString = commandLine.getOptionValue(BULK_OPTION);

        try {
            if (options.contains(KILL_OPTION)) {
                printBulkModifiedJobs(wc.killJobs(filter, jobtype, start, len), timeZoneId, "killed");
            }
            else if (options.contains(SUSPEND_OPTION)) {
                printBulkModifiedJobs(wc.suspendJobs(filter, jobtype, start, len), timeZoneId, "suspended");
            }
            else if (options.contains(RESUME_OPTION)) {
                printBulkModifiedJobs(wc.resumeJobs(filter, jobtype, start, len), timeZoneId, "resumed");
            }
            else if (bulkFilterString != null) {
                printBulkJobs(wc.getBulkInfo(bulkFilterString, start, len), timeZoneId, commandLine.hasOption(VERBOSE_OPTION));
            }
            else if (jobtype.toLowerCase().contains("wf")) {
                printJobs(wc.getJobsInfo(filter, start, len), timeZoneId, commandLine.hasOption(VERBOSE_OPTION));
            }
            else if (jobtype.toLowerCase().startsWith("coord")) {
                printCoordJobs(wc.getCoordJobsInfo(filter, start, len), timeZoneId, commandLine.hasOption(VERBOSE_OPTION));
            }
            else if (jobtype.toLowerCase().startsWith("bundle")) {
                printBundleJobs(wc.getBundleJobsInfo(filter, start, len), timeZoneId, commandLine.hasOption(VERBOSE_OPTION));
            }

        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    @VisibleForTesting
    void printBulkModifiedJobs(JSONObject json, String timeZoneId, String action) throws IOException {
        if (json.containsKey(JsonTags.WORKFLOWS_JOBS)) {
            JSONArray workflows = (JSONArray) json.get(JsonTags.WORKFLOWS_JOBS);
            if (workflows == null) {
                workflows = new JSONArray();
            }
            List<WorkflowJob> wfs = JsonToBean.createWorkflowJobList(workflows);
            if (wfs.isEmpty()) {
                System.out.println("bulk modify command did not modify any jobs");
            }
            else {
                System.out.println("the following jobs have been " + action);
                printJobs(wfs, timeZoneId, false);
            }
        }
        else if (json.containsKey(JsonTags.COORDINATOR_JOBS)) {
            JSONArray coordinators = (JSONArray) json.get(JsonTags.COORDINATOR_JOBS);
            if (coordinators == null) {
                coordinators = new JSONArray();
            }
            List<CoordinatorJob> coords = JsonToBean.createCoordinatorJobList(coordinators);
            if (coords.isEmpty()) {
                System.out.println("bulk modify command did not modify any jobs");
            }
            else {
                System.out.println("the following jobs have been " + action);
                printCoordJobs(coords, timeZoneId, false);
            }
        }
        else {
            JSONArray bundles = (JSONArray) json.get(JsonTags.BUNDLE_JOBS);
            if (bundles == null) {
                bundles = new JSONArray();
            }
            List<BundleJob> bundleJobs = JsonToBean.createBundleJobList(bundles);
            if (bundleJobs.isEmpty()) {
                System.out.println("bulk modify command did not modify any jobs");
            }
            else {
                System.out.println("the following jobs have been " + action);
                printBundleJobs(bundleJobs, timeZoneId, false);
            }
        }
    }

    @VisibleForTesting
    void printCoordJobs(List<CoordinatorJob> jobs, String timeZoneId, boolean verbose) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            if (verbose) {
                System.out.println("Job ID" + VERBOSE_DELIMITER + "App Name" + VERBOSE_DELIMITER + "App Path"
                        + VERBOSE_DELIMITER + "Console URL" + VERBOSE_DELIMITER + "User" + VERBOSE_DELIMITER + "Group"
                        + VERBOSE_DELIMITER + "Concurrency" + VERBOSE_DELIMITER + "Frequency" + VERBOSE_DELIMITER
                        + "Time Unit" + VERBOSE_DELIMITER + "Time Zone" + VERBOSE_DELIMITER + "Time Out"
                        + VERBOSE_DELIMITER + "Started" + VERBOSE_DELIMITER + "Next Materialize" + VERBOSE_DELIMITER
                        + "Status" + VERBOSE_DELIMITER + "Last Action" + VERBOSE_DELIMITER + "Ended");
                System.out.println(RULER);

                for (CoordinatorJob job : jobs) {
                    System.out.println(maskIfNull(job.getId()) + VERBOSE_DELIMITER + maskIfNull(job.getAppName())
                            + VERBOSE_DELIMITER + maskIfNull(job.getAppPath()) + VERBOSE_DELIMITER
                            + maskIfNull(job.getConsoleUrl()) + VERBOSE_DELIMITER + maskIfNull(job.getUser())
                            + VERBOSE_DELIMITER + maskIfNull(job.getGroup()) + VERBOSE_DELIMITER + job.getConcurrency()
                            + VERBOSE_DELIMITER + job.getFrequency() + VERBOSE_DELIMITER + job.getTimeUnit()
                            + VERBOSE_DELIMITER + maskIfNull(job.getTimeZone()) + VERBOSE_DELIMITER + job.getTimeout()
                            + VERBOSE_DELIMITER + maskDate(job.getStartTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + maskDate(job.getNextMaterializedTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + job.getStatus() + VERBOSE_DELIMITER
                            + maskDate(job.getLastActionTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + maskDate(job.getEndTime(), timeZoneId, verbose));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(COORD_JOBS_FORMATTER, "Job ID", "App Name", "Status", "Freq", "Unit",
                        "Started", "Next Materialized"));
                System.out.println(RULER);

                for (CoordinatorJob job : jobs) {
                    System.out.println(String.format(COORD_JOBS_FORMATTER, maskIfNull(job.getId()), maskIfNull(job
                            .getAppName()), job.getStatus(), job.getFrequency(), job.getTimeUnit(), maskDate(job
                            .getStartTime(), timeZoneId, verbose), maskDate(job.getNextMaterializedTime(), timeZoneId, verbose)));

                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println("No Jobs match your criteria!");
        }
    }

    @VisibleForTesting
    void printBulkJobs(List<BulkResponse> jobs, String timeZoneId, boolean verbose) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            for (BulkResponse response : jobs) {
                BundleJob bundle = response.getBundle();
                CoordinatorJob coord = response.getCoordinator();
                CoordinatorAction action = response.getAction();
                if (verbose) {
                    System.out.println();
                    System.out.println("Bundle Name : " + maskIfNull(bundle.getAppName()));

                    System.out.println(RULER);

                    System.out.println("Bundle ID        : " + maskIfNull(bundle.getId()));
                    System.out.println("Coordinator Name : " + maskIfNull(coord.getAppName()));
                    System.out.println("Coord Action ID  : " + maskIfNull(action.getId()));
                    System.out.println("Action Status    : " + action.getStatus());
                    System.out.println("External ID      : " + maskIfNull(action.getExternalId()));
                    System.out.println("Created Time     : " + maskDate(action.getCreatedTime(), timeZoneId, false));
                    System.out.println("User             : " + maskIfNull(bundle.getUser()));
                    System.out.println("Error Message    : " + maskIfNull(action.getErrorMessage()));
                    System.out.println(RULER);
                }
                else {
                    System.out.println(String.format(BULK_RESPONSE_FORMATTER, "Bundle Name", "Bundle ID", "Coord Name",
                            "Coord Action ID", "Status", "External ID", "Created Time", "Error Message"));
                    System.out.println(RULER);
                    System.out
                            .println(String.format(BULK_RESPONSE_FORMATTER, maskIfNull(bundle.getAppName()),
                                    maskIfNull(bundle.getId()), maskIfNull(coord.getAppName()),
                                    maskIfNull(action.getId()), action.getStatus(), maskIfNull(action.getExternalId()),
                                    maskDate(action.getCreatedTime(), timeZoneId, false),
                                    maskIfNull(action.getErrorMessage())));
                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println("Bulk request criteria did not match any coordinator actions");
        }
    }

    @VisibleForTesting
    void printBundleJobs(List<BundleJob> jobs, String timeZoneId, boolean verbose) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            if (verbose) {
                System.out.println("Job ID" + VERBOSE_DELIMITER + "Bundle Name" + VERBOSE_DELIMITER + "Bundle Path"
                        + VERBOSE_DELIMITER + "User" + VERBOSE_DELIMITER + "Group" + VERBOSE_DELIMITER + "Status"
                        + VERBOSE_DELIMITER + "Kickoff" + VERBOSE_DELIMITER + "Pause" + VERBOSE_DELIMITER + "Created"
                        + VERBOSE_DELIMITER + "Console URL");
                System.out.println(RULER);

                for (BundleJob job : jobs) {
                    System.out.println(maskIfNull(job.getId()) + VERBOSE_DELIMITER + maskIfNull(job.getAppName())
                            + VERBOSE_DELIMITER + maskIfNull(job.getAppPath()) + VERBOSE_DELIMITER
                            + maskIfNull(job.getUser()) + VERBOSE_DELIMITER + maskIfNull(job.getGroup())
                            + VERBOSE_DELIMITER + job.getStatus() + VERBOSE_DELIMITER
                            + maskDate(job.getKickoffTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + maskDate(job.getPauseTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + maskDate(job.getCreatedTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + maskIfNull(job.getConsoleUrl()));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(BUNDLE_JOBS_FORMATTER, "Job ID", "Bundle Name", "Status", "Kickoff",
                        "Created", "User", "Group"));
                System.out.println(RULER);

                for (BundleJob job : jobs) {
                    System.out.println(String.format(BUNDLE_JOBS_FORMATTER, maskIfNull(job.getId()),
                            maskIfNull(job.getAppName()), job.getStatus(),
                            maskDate(job.getKickoffTime(), timeZoneId, verbose),
                            maskDate(job.getCreatedTime(), timeZoneId, verbose), maskIfNull(job.getUser()),
                            maskIfNull(job.getGroup())));
                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println("No Jobs match your criteria!");
        }
    }

    private void slaCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);
        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        String s = commandLine.getOptionValue(OFFSET_OPTION);
        int start = Integer.parseInt((s != null) ? s : "0");
        s = commandLine.getOptionValue(LEN_OPTION);
        int len = Integer.parseInt((s != null) ? s : "100");
        String filter = commandLine.getOptionValue(FILTER_OPTION);

        try {
            wc.getSlaInfo(start, len, filter);
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void adminCommand(CommandLine commandLine) throws OozieCLIException {
        XOozieClient wc = createXOozieClient(commandLine);

        List<String> options = new ArrayList<String>();
        for (Option option : commandLine.getOptions()) {
            options.add(option.getOpt());
        }

        try {
            SYSTEM_MODE status = SYSTEM_MODE.NORMAL;
            if (options.contains(VERSION_OPTION)) {
                System.out.println("Oozie server build version: " + wc.getServerBuildVersion());
            }
            else if (options.contains(SYSTEM_MODE_OPTION)) {
                String systemModeOption = commandLine.getOptionValue(SYSTEM_MODE_OPTION).toUpperCase();
                try {
                    status = SYSTEM_MODE.valueOf(systemModeOption);
                }
                catch (Exception e) {
                    throw new OozieCLIException("Invalid input provided for option: " + SYSTEM_MODE_OPTION
                            + " value given :" + systemModeOption
                            + " Expected values are: NORMAL/NOWEBSERVICE/SAFEMODE ");
                }
                wc.setSystemMode(status);
                System.out.println("System mode: " + status);
            }
            else if (options.contains(STATUS_OPTION)) {
                status = wc.getSystemMode();
                System.out.println("System mode: " + status);
            }

            else if (options.contains(UPDATE_SHARELIB_OPTION)) {
                System.out.println(wc.updateShareLib());
            }

            else if (options.contains(LIST_SHARELIB_LIB_OPTION)) {
                String sharelibKey = null;
                if (commandLine.getArgList().size() > 0) {
                    sharelibKey = (String) commandLine.getArgList().get(0);
                }
                System.out.println(wc.listShareLib(sharelibKey));
            }

            else if (options.contains(QUEUE_DUMP_OPTION)) {

                List<String> list = wc.getQueueDump();
                if (list != null && list.size() != 0) {
                    for (String str : list) {
                        System.out.println(str);
                    }
                }
                else {
                    System.out.println("QueueDump is null!");
                }
            }
            else if (options.contains(AVAILABLE_SERVERS_OPTION)) {
                Map<String, String> availableOozieServers = new TreeMap<String, String>(wc.getAvailableOozieServers());
                for (Map.Entry<String, String> ent : availableOozieServers.entrySet()) {
                    System.out.println(ent.getKey() + " : " + ent.getValue());
                }
            } else if (options.contains(SERVER_CONFIGURATION_OPTION)) {
                Map<String, String> serverConfig = new TreeMap<String, String>(wc.getServerConfiguration());
                for (Map.Entry<String, String> ent : serverConfig.entrySet()) {
                    System.out.println(ent.getKey() + " : " + ent.getValue());
                }
            } else if (options.contains(SERVER_OS_ENV_OPTION)) {
                Map<String, String> osEnv = new TreeMap<String, String>(wc.getOSEnv());
                for (Map.Entry<String, String> ent : osEnv.entrySet()) {
                    System.out.println(ent.getKey() + " : " + ent.getValue());
                }
            } else if (options.contains(SERVER_JAVA_SYSTEM_PROPERTIES_OPTION)) {
                Map<String, String> javaSysProps = new TreeMap<String, String>(wc.getJavaSystemProperties());
                for (Map.Entry<String, String> ent : javaSysProps.entrySet()) {
                    System.out.println(ent.getKey() + " : " + ent.getValue());
                }
            } else if (options.contains(METRICS_OPTION)) {
                OozieClient.Metrics metrics = wc.getMetrics();
                if (metrics == null) {
                    System.out.println("Metrics are unavailable.  Try Instrumentation (-" + INSTRUMENTATION_OPTION + ") instead");
                } else {
                    printMetrics(metrics);
                }
            } else if (options.contains(INSTRUMENTATION_OPTION)) {
                OozieClient.Instrumentation instrumentation = wc.getInstrumentation();
                if (instrumentation == null) {
                    System.out.println("Instrumentation is unavailable.  Try Metrics (-" + METRICS_OPTION + ") instead");
                } else {
                    printInstrumentation(instrumentation);
                }
            }
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void versionCommand() throws OozieCLIException {
        System.out.println("Oozie client build version: "
                + BuildInfo.getBuildInfo().getProperty(BuildInfo.BUILD_VERSION));
    }

    @VisibleForTesting
    void printJobs(List<WorkflowJob> jobs, String timeZoneId, boolean verbose) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            if (verbose) {
                System.out.println("Job ID" + VERBOSE_DELIMITER + "App Name" + VERBOSE_DELIMITER + "App Path"
                        + VERBOSE_DELIMITER + "Console URL" + VERBOSE_DELIMITER + "User" + VERBOSE_DELIMITER + "Group"
                        + VERBOSE_DELIMITER + "Run" + VERBOSE_DELIMITER + "Created" + VERBOSE_DELIMITER + "Started"
                        + VERBOSE_DELIMITER + "Status" + VERBOSE_DELIMITER + "Last Modified" + VERBOSE_DELIMITER
                        + "Ended");
                System.out.println(RULER);

                for (WorkflowJob job : jobs) {
                    System.out.println(maskIfNull(job.getId()) + VERBOSE_DELIMITER + maskIfNull(job.getAppName())
                            + VERBOSE_DELIMITER + maskIfNull(job.getAppPath()) + VERBOSE_DELIMITER
                            + maskIfNull(job.getConsoleUrl()) + VERBOSE_DELIMITER + maskIfNull(job.getUser())
                            + VERBOSE_DELIMITER + maskIfNull(job.getGroup()) + VERBOSE_DELIMITER + job.getRun()
                            + VERBOSE_DELIMITER + maskDate(job.getCreatedTime(), timeZoneId, verbose)
                            + VERBOSE_DELIMITER + maskDate(job.getStartTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + job.getStatus() + VERBOSE_DELIMITER
                            + maskDate(job.getLastModifiedTime(), timeZoneId, verbose) + VERBOSE_DELIMITER
                            + maskDate(job.getEndTime(), timeZoneId, verbose));

                    System.out.println(RULER);
                }
            }
            else {
                System.out.println(String.format(WORKFLOW_JOBS_FORMATTER, "Job ID", "App Name", "Status", "User",
                        "Group", "Started", "Ended"));
                System.out.println(RULER);

                for (WorkflowJob job : jobs) {
                    System.out.println(String.format(WORKFLOW_JOBS_FORMATTER, maskIfNull(job.getId()),
                            maskIfNull(job.getAppName()), job.getStatus(), maskIfNull(job.getUser()),
                            maskIfNull(job.getGroup()), maskDate(job.getStartTime(), timeZoneId, verbose),
                            maskDate(job.getEndTime(), timeZoneId, verbose)));

                    System.out.println(RULER);
                }
            }
        }
        else {
            System.out.println("No Jobs match your criteria!");
        }
    }

    void printWfsForCoordAction(List<WorkflowJob> jobs, String timeZoneId) throws IOException {
        if (jobs != null && jobs.size() > 0) {
            System.out.println(String.format("%-41s%-10s%-24s%-24s", "Job ID", "Status", "Started", "Ended"));
            System.out.println(RULER);

            for (WorkflowJob job : jobs) {
                System.out
                        .println(String.format("%-41s%-10s%-24s%-24s", maskIfNull(job.getId()), job.getStatus(),
                                maskDate(job.getStartTime(), timeZoneId, false),
                                maskDate(job.getEndTime(), timeZoneId, false)));
                System.out.println(RULER);
            }
        }
    }

    private String maskIfNull(String value) {
        if (value != null && value.length() > 0) {
            return value;
        }
        return "-";
    }

    private String maskDate(Date date, String timeZoneId, boolean verbose) {
        if (date == null) {
            return "-";
        }

        SimpleDateFormat dateFormater = null;
        if (verbose) {
            dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz", Locale.US);
        }
        else {
            dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm zzz", Locale.US);
        }

        if (timeZoneId != null) {
            dateFormater.setTimeZone(TimeZone.getTimeZone(timeZoneId));
        }
        String dateString = dateFormater.format(date);
        // Most TimeZones are 3 or 4 characters; GMT offsets (e.g. GMT-07:00) are 9, so lets remove the "GMT" part to make it 6
        // to fit better
        Matcher m = GMT_OFFSET_SHORTEN_PATTERN.matcher(dateString);
        if (m.matches() && m.groupCount() == 2) {
            dateString = m.group(1) + m.group(2);
        }
        return dateString;
    }

    private void validateCommand(CommandLine commandLine) throws OozieCLIException {
        String[] args = commandLine.getArgs();
        if (args.length != 1) {
            throw new OozieCLIException("One file must be specified");
        }
        File file = new File(args[0]);
        if (file.exists()) {
            try {
                List<StreamSource> sources = new ArrayList<StreamSource>();
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "shell-action-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "shell-action-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "shell-action-0.3.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "email-action-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "email-action-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "distcp-action-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "distcp-action-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.2.5.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.3.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.4.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.4.5.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-workflow-0.5.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-coordinator-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-coordinator-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-coordinator-0.3.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-coordinator-0.4.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-bundle-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-bundle-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-sla-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "oozie-sla-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "hive-action-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "hive-action-0.3.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "hive-action-0.4.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "hive-action-0.5.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "sqoop-action-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "sqoop-action-0.3.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "sqoop-action-0.4.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "ssh-action-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "ssh-action-0.2.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader().getResourceAsStream(
                        "hive2-action-0.1.xsd")));
                sources.add(new StreamSource(Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("spark-action-0.1.xsd")));
                SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                Schema schema = factory.newSchema(sources.toArray(new StreamSource[sources.size()]));
                Validator validator = schema.newValidator();
                validator.validate(new StreamSource(new FileReader(file)));
                System.out.println("Valid workflow-app");
            }
            catch (Exception ex) {
                throw new OozieCLIException("Invalid app definition, " + ex.toString(), ex);
            }
        }
        else {
            throw new OozieCLIException("File does not exists");
        }
    }

    private void scriptLanguageCommand(CommandLine commandLine, String jobType) throws IOException, OozieCLIException {
        List<String> args = commandLine.getArgList();
        if (args.size() > 0) {
            // checking if args starts with -X (because CLIParser cannot check this)
            if (!args.get(0).equals("-X")) {
                throw new OozieCLIException("Unrecognized option: " + args.get(0) + " Expecting -X");
            }
            args.remove(0);
        }

        if (!commandLine.hasOption(SCRIPTFILE_OPTION)) {
            throw new OozieCLIException("Need to specify -file <scriptfile>");
        }

        if (!commandLine.hasOption(CONFIG_OPTION)) {
            throw new OozieCLIException("Need to specify -config <configfile>");
        }

        try {
            XOozieClient wc = createXOozieClient(commandLine);
            Properties conf = getConfiguration(wc, commandLine);
            String script = commandLine.getOptionValue(SCRIPTFILE_OPTION);
            List<String> paramsList = new ArrayList<String>();
            if (commandLine.hasOption("P")) {
                Properties params = commandLine.getOptionProperties("P");
                for (String key : params.stringPropertyNames()) {
                    paramsList.add(key + "=" + params.getProperty(key));
                }
            }
            System.out.println(JOB_ID_PREFIX + wc.submitScriptLanguage(conf, script, args.toArray(new String[args.size()]),
                    paramsList.toArray(new String[paramsList.size()]), jobType));
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void sqoopCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        List<String> args = commandLine.getArgList();
        if (args.size() > 0) {
            // checking if args starts with -X (because CLIParser cannot check this)
            if (!args.get(0).equals("-X")) {
                throw new OozieCLIException("Unrecognized option: " + args.get(0) + " Expecting -X");
            }
            args.remove(0);
        }

        if (!commandLine.hasOption(SQOOP_COMMAND_OPTION)) {
            throw new OozieCLIException("Need to specify -command");
        }

        if (!commandLine.hasOption(CONFIG_OPTION)) {
            throw new OozieCLIException("Need to specify -config <configfile>");
        }

        try {
            XOozieClient wc = createXOozieClient(commandLine);
            Properties conf = getConfiguration(wc, commandLine);
            String[] command = commandLine.getOptionValues(SQOOP_COMMAND_OPTION);
            System.out.println(JOB_ID_PREFIX + wc.submitSqoop(conf, command, args.toArray(new String[args.size()])));
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private void infoCommand(CommandLine commandLine) throws OozieCLIException {
        for (Option option : commandLine.getOptions()) {
            String opt = option.getOpt();
            if (opt.equals(INFO_TIME_ZONES_OPTION)) {
                printAvailableTimeZones();
            }
        }
    }

    private void printAvailableTimeZones() {
        System.out.println("The format is \"SHORT_NAME (ID)\"\nGive the ID to the -timezone argument");
        System.out.println("GMT offsets can also be used (e.g. GMT-07:00, GMT-0700, GMT+05:30, GMT+0530)");
        System.out.println("Available Time Zones:");
        for (String tzId : TimeZone.getAvailableIDs()) {
            // skip id's that are like "Etc/GMT+01:00" because their display names are like "GMT-01:00", which is confusing
            if (!tzId.startsWith("Etc/GMT")) {
                TimeZone tZone = TimeZone.getTimeZone(tzId);
                System.out.println("      " + tZone.getDisplayName(false, TimeZone.SHORT) + " (" + tzId + ")");
            }
        }
    }


    private void mrCommand(CommandLine commandLine) throws IOException, OozieCLIException {
        try {
            XOozieClient wc = createXOozieClient(commandLine);
            Properties conf = getConfiguration(wc, commandLine);

            String mapper = conf.getProperty(MAPRED_MAPPER, conf.getProperty(MAPRED_MAPPER_2));
            if (mapper == null) {
                throw new OozieCLIException("mapper (" + MAPRED_MAPPER + " or " + MAPRED_MAPPER_2 + ") must be specified in conf");
            }

            String reducer = conf.getProperty(MAPRED_REDUCER, conf.getProperty(MAPRED_REDUCER_2));
            if (reducer == null) {
                throw new OozieCLIException("reducer (" + MAPRED_REDUCER + " or " + MAPRED_REDUCER_2
                        + ") must be specified in conf");
            }

            String inputDir = conf.getProperty(MAPRED_INPUT);
            if (inputDir == null) {
                throw new OozieCLIException("input dir (" + MAPRED_INPUT +") must be specified in conf");
            }

            String outputDir = conf.getProperty(MAPRED_OUTPUT);
            if (outputDir == null) {
                throw new OozieCLIException("output dir (" + MAPRED_OUTPUT +") must be specified in conf");
            }

            System.out.println(JOB_ID_PREFIX + wc.submitMapReduce(conf));
        }
        catch (OozieClientException ex) {
            throw new OozieCLIException(ex.toString(), ex);
        }
    }

    private String getFirstMissingDependencies(CoordinatorAction action) {
        StringBuilder allDeps = new StringBuilder();
        String missingDep = action.getMissingDependencies();
        boolean depExists = false;
        if (missingDep != null && !missingDep.isEmpty()) {
            allDeps.append(missingDep.split(INSTANCE_SEPARATOR)[0]);
            depExists = true;
        }
        String pushDeps = action.getPushMissingDependencies();
        if (pushDeps != null && !pushDeps.isEmpty()) {
            if(depExists) {
                allDeps.append(INSTANCE_SEPARATOR);
            }
            allDeps.append(pushDeps.split(INSTANCE_SEPARATOR)[0]);
        }
        return allDeps.toString();
    }

    private void slaAlertCommand(String jobIds, OozieClient wc, CommandLine commandLine, List<String> options)
            throws OozieCLIException, OozieClientException {
        String actions = null, coordinators = null, dates = null;

        if (options.contains(ACTION_OPTION)) {
            actions = commandLine.getOptionValue(ACTION_OPTION);
        }

        if (options.contains(DATE_OPTION)) {
            dates = commandLine.getOptionValue(DATE_OPTION);
        }

        if (options.contains(COORD_OPTION)) {
            coordinators = commandLine.getOptionValue(COORD_OPTION);
            if (coordinators == null) {
                throw new OozieCLIException("No value specified for -coordinator option");
            }
        }

        if (options.contains(SLA_ENABLE_ALERT)) {
            wc.slaEnableAlert(jobIds, actions, dates, coordinators);
        }
        else if (options.contains(SLA_DISABLE_ALERT)) {
            wc.slaDisableAlert(jobIds, actions, dates, coordinators);
        }
        else if (options.contains(SLA_CHANGE)) {
            String newSlaParams = commandLine.getOptionValue(CHANGE_VALUE_OPTION);
            wc.slaChange(jobIds, actions, dates, coordinators, newSlaParams);
        }
    }

    private void printMetrics(OozieClient.Metrics metrics) {
        System.out.println("COUNTERS");
        System.out.println("--------");
        Map<String, Long> counters = new TreeMap<String, Long>(metrics.getCounters());
        for (Map.Entry<String, Long> ent : counters.entrySet()) {
            System.out.println(ent.getKey() + " : " + ent.getValue());
        }
        System.out.println("\nGAUGES");
        System.out.println("------");
        Map<String, Object> gauges = new TreeMap<String, Object>(metrics.getGauges());
        for (Map.Entry<String, Object> ent : gauges.entrySet()) {
            System.out.println(ent.getKey() + " : " + ent.getValue());
        }
        System.out.println("\nTIMERS");
        System.out.println("------");
        Map<String, OozieClient.Metrics.Timer> timers = new TreeMap<String, OozieClient.Metrics.Timer>(metrics.getTimers());
        for (Map.Entry<String, OozieClient.Metrics.Timer> ent : timers.entrySet()) {
            System.out.println(ent.getKey());
            System.out.println(ent.getValue());
        }
        System.out.println("\nHISTOGRAMS");
        System.out.println("----------");
        Map<String, OozieClient.Metrics.Histogram> histograms =
                new TreeMap<String, OozieClient.Metrics.Histogram>(metrics.getHistograms());
        for (Map.Entry<String, OozieClient.Metrics.Histogram> ent : histograms.entrySet()) {
            System.out.println(ent.getKey());
            System.out.println(ent.getValue());
        }
    }

    private void printInstrumentation(OozieClient.Instrumentation instrumentation) {
        System.out.println("COUNTERS");
        System.out.println("--------");
        Map<String, Long> counters = new TreeMap<String, Long>(instrumentation.getCounters());
        for (Map.Entry<String, Long> ent : counters.entrySet()) {
            System.out.println(ent.getKey() + " : " + ent.getValue());
        }
        System.out.println("\nVARIABLES");
        System.out.println("---------");
        Map<String, Object> variables = new TreeMap<String, Object>(instrumentation.getVariables());
        for (Map.Entry<String, Object> ent : variables.entrySet()) {
            System.out.println(ent.getKey() + " : " + ent.getValue());
        }
        System.out.println("\nSAMPLERS");
        System.out.println("---------");
        Map<String, Double> samplers = new TreeMap<String, Double>(instrumentation.getSamplers());
        for (Map.Entry<String, Double> ent : samplers.entrySet()) {
            System.out.println(ent.getKey() + " : " + ent.getValue());
        }
        System.out.println("\nTIMERS");
        System.out.println("---------");
        Map<String, OozieClient.Instrumentation.Timer> timers =
                new TreeMap<String, OozieClient.Instrumentation.Timer>(instrumentation.getTimers());
        for (Map.Entry<String, OozieClient.Instrumentation.Timer> ent : timers.entrySet()) {
            System.out.println(ent.getKey());
            System.out.println(ent.getValue());
        }
    }
}
