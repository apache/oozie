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

package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.google.common.base.Charsets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import static org.apache.oozie.action.hadoop.LauncherMain.CHILD_MAPREDUCE_JOB_TAGS;

public class MapReduceActionExecutor extends JavaActionExecutor {

    public static final String OOZIE_ACTION_EXTERNAL_STATS_WRITE = "oozie.action.external.stats.write";
    public static final String HADOOP_COUNTERS = "hadoop.counters";
    public static final String OOZIE_MAPREDUCE_UBER_JAR_ENABLE = "oozie.action.mapreduce.uber.jar.enable";
    private static final String STREAMING_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.StreamingMain";
    public static final String JOB_END_NOTIFICATION_URL = "job.end.notification.url";
    private static final String MAPREDUCE_JOB_NAME = "mapreduce.job.name";
    static final String YARN_APPLICATION_TYPE_MAPREDUCE = "MAPREDUCE";
    private XLog log = XLog.getLog(getClass());

    public MapReduceActionExecutor() {
        super("map-reduce");
    }

    @Override
    public List<Class<?>> getLauncherClasses() {
        List<Class<?>> classes = new ArrayList<>();
        try {
            classes.add(Class.forName(STREAMING_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    protected String getActualExternalId(WorkflowAction action) {
        String launcherJobId = action.getExternalId();
        String childId = action.getExternalChildIDs();

        if (childId != null && !childId.isEmpty()) {
            return childId;
        } else {
            return launcherJobId;
        }
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        String mainClass;
        Namespace ns = actionXml.getNamespace();
        if (actionXml.getChild("streaming", ns) != null) {
            mainClass = launcherConf.get(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS, STREAMING_MAIN_CLASS_NAME);
        }
        else {
            if (actionXml.getChild("pipes", ns) != null) {
                mainClass = launcherConf.get(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS, PipesMain.class.getName());
            }
            else {
                mainClass = launcherConf.get(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS, MapReduceMain.class.getName());
            }
        }
        return mainClass;
    }

    @Override
    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);
        conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

        return conf;
    }

    private void injectConfigClass(Configuration conf, Element actionXml) {
        // Inject config-class for launcher to use for action
        Element e = actionXml.getChild("config-class", actionXml.getNamespace());
        if (e != null) {
            conf.set(LauncherAMUtils.OOZIE_ACTION_CONFIG_CLASS, e.getTextTrim());
        }
    }

    @Override
    protected Configuration createBaseHadoopConf(Context context, Element actionXml, boolean loadResources) {
        return super.createBaseHadoopConf(context, actionXml, loadResources);
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        boolean regularMR = false;

        injectConfigClass(actionConf, actionXml);
        Namespace ns = actionXml.getNamespace();
        if (actionXml.getChild("streaming", ns) != null) {
            Element streamingXml = actionXml.getChild("streaming", ns);
            String mapper = streamingXml.getChildTextTrim("mapper", ns);
            String reducer = streamingXml.getChildTextTrim("reducer", ns);
            String recordReader = streamingXml.getChildTextTrim("record-reader", ns);
            List<Element> list = (List<Element>) streamingXml.getChildren("record-reader-mapping", ns);
            String[] recordReaderMapping = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                recordReaderMapping[i] = list.get(i).getTextTrim();
            }
            list = (List<Element>) streamingXml.getChildren("env", ns);
            String[] env = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                env[i] = list.get(i).getTextTrim();
            }
            setStreaming(actionConf, mapper, reducer, recordReader, recordReaderMapping, env);
        }
        else {
            if (actionXml.getChild("pipes", ns) != null) {
                Element pipesXml = actionXml.getChild("pipes", ns);
                String map = pipesXml.getChildTextTrim("map", ns);
                String reduce = pipesXml.getChildTextTrim("reduce", ns);
                String inputFormat = pipesXml.getChildTextTrim("inputformat", ns);
                String partitioner = pipesXml.getChildTextTrim("partitioner", ns);
                String writer = pipesXml.getChildTextTrim("writer", ns);
                String program = pipesXml.getChildTextTrim("program", ns);
                PipesMain.setPipes(actionConf, map, reduce, inputFormat, partitioner, writer, program, appPath);
            }
            else {
                regularMR = true;
            }
        }
        actionConf = super.setupActionConf(actionConf, context, actionXml, appPath);
        setJobName(actionConf, context);

        // For "regular" (not streaming or pipes) MR jobs
        if (regularMR) {
            // Resolve uber jar path (has to be done after super because oozie.mapreduce.uber.jar is under <configuration>)
            String uberJar = actionConf.get(MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR);
            if (uberJar != null) {
                if (!ConfigurationService.getBoolean(OOZIE_MAPREDUCE_UBER_JAR_ENABLE)){
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "MR003",
                            "{0} property is not allowed.  Set {1} to true in oozie-site to enable.",
                            MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR, OOZIE_MAPREDUCE_UBER_JAR_ENABLE);
                }
                String nameNode = actionXml.getChildTextTrim("name-node", ns);
                if (nameNode != null) {
                    Path uberJarPath = new Path(uberJar);
                    if (uberJarPath.toUri().getScheme() == null || uberJarPath.toUri().getAuthority() == null) {
                        if (uberJarPath.isAbsolute()) {     // absolute path without namenode --> prepend namenode
                            Path nameNodePath = new Path(nameNode);
                            String nameNodeSchemeAuthority = nameNodePath.toUri().getScheme()
                                    + "://" + nameNodePath.toUri().getAuthority();
                            actionConf.set(MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR,
                                    new Path(nameNodeSchemeAuthority + uberJarPath).toString());
                        }
                        else {                              // relative path --> prepend app path
                            actionConf.set(MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR, new Path(appPath, uberJarPath).toString());
                        }
                    }
                }
            }
        }
        else {
            if (actionConf.get(MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR) != null) {
                log.warn("The " + MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR + " property is only applicable for MapReduce (not"
                        + "streaming nor pipes) workflows, ignoring");
                actionConf.set(MapReduceMain.OOZIE_MAPREDUCE_UBER_JAR, "");
            }
        }

        // child job cancel delegation token for mapred action
        actionConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", true);

        return actionConf;
    }

    private void setJobName(Configuration actionConf, Context context) {
        String jobName = getAppName(context);
        if (jobName != null) {
            actionConf.set(MAPREDUCE_JOB_NAME, jobName.replace("oozie:launcher", "oozie:action"));
        }
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        super.end(context, action);
        JobClient jobClient = null;
        boolean exception = false;
        try {
            if (action.getStatus() == WorkflowAction.Status.OK) {
                Element actionXml = XmlUtils.parseXml(action.getConf());
                Configuration jobConf = createBaseHadoopConf(context, actionXml);
                jobClient = createJobClient(context, jobConf);
                RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalChildIDs()));
                if (runningJob == null) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "MR002",
                            "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!",
                            action.getExternalChildIDs(), action.getId());
                }

                Counters counters = runningJob.getCounters();
                if (counters != null) {
                    ActionStats stats = new MRStats(counters);
                    String statsJsonString = stats.toJSON();
                    context.setVar(HADOOP_COUNTERS, statsJsonString);

                    // If action stats write property is set to false by user or
                    // size of stats is greater than the maximum allowed size,
                    // do not store the action stats
                    if (Boolean.parseBoolean(evaluateConfigurationProperty(actionXml,
                            OOZIE_ACTION_EXTERNAL_STATS_WRITE, "false"))
                            && (statsJsonString.getBytes(Charsets.UTF_8).length <= getMaxExternalStatsSize())) {
                        context.setExecutionStats(statsJsonString);
                        log.debug(
                                "Printing stats for Map-Reduce action as a JSON string : [{0}]", statsJsonString);
                    }
                }
                else {
                    context.setVar(HADOOP_COUNTERS, "");
                    XLog.getLog(getClass()).warn("Could not find Hadoop Counters for: [{0}]",
                            action.getExternalChildIDs());
                }
            }
        }
        catch (Exception ex) {
            exception = true;
            throw convertException(ex);
        }
        finally {
            if (jobClient != null) {
                try {
                    jobClient.close();
                }
                catch (Exception e) {
                    if (exception) {
                        log.error("JobClient error: ", e);
                    }
                    else {
                        throw convertException(e);
                    }
                }
            }
        }
    }

    // Return the value of the specified configuration property
    private String evaluateConfigurationProperty(Element actionConf, String key, String defaultValue)
            throws ActionExecutorException {
        try {
            String ret = defaultValue;
            if (actionConf != null) {
                Namespace ns = actionConf.getNamespace();
                Element e = actionConf.getChild("configuration", ns);
                if(e != null) {
                    String strConf = XmlUtils.prettyPrint(e).toString();
                    XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                    ret = inlineConf.get(key, defaultValue);
                }
            }
            return ret;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    /**
     * Return the sharelib name for the action.
     *
     * @return returns <code>streaming</code> if mapreduce-streaming action, <code>NULL</code> otherwise.
     * @param actionXml
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        Namespace ns = actionXml.getNamespace();
        return (actionXml.getChild("streaming", ns) != null) ? "mapreduce-streaming" : null;
    }

    public static void setStreaming(Configuration conf, String mapper, String reducer, String recordReader,
                                    String[] recordReaderMapping, String[] env) {
        if (mapper != null) {
            conf.set("oozie.streaming.mapper", mapper);
        }
        if (reducer != null) {
            conf.set("oozie.streaming.reducer", reducer);
        }
        if (recordReader != null) {
            conf.set("oozie.streaming.record-reader", recordReader);
        }
        ActionUtils.setStrings(conf, "oozie.streaming.record-reader-mapping", recordReaderMapping);
        ActionUtils.setStrings(conf, "oozie.streaming.env", env);
    }

    @Override
    protected void injectCallback(Context context, Configuration conf) {
        // add callback for the MapReduce job
        String callback = context.getCallbackUrl("$jobStatus");
        String originalCallbackURL = conf.get(JOB_END_NOTIFICATION_URL);
        if (originalCallbackURL != null) {
            LOG.warn("Overriding the action job end notification URI. Original value: {0}", originalCallbackURL);
        }
        conf.set(JOB_END_NOTIFICATION_URL, callback);

        super.injectCallback(context, conf);
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        Map<String, String> actionData;
        Configuration jobConf;

        // Need to emit jobConf and actionData for later usage
        try {
            final FileSystem actionFs = context.getAppFileSystem();
            final Element actionXml = XmlUtils.parseXml(action.getConf());
            jobConf = createBaseHadoopConf(context, actionXml);
            final Path actionDir = context.getActionDir();
            actionData = LauncherHelper.getActionData(actionFs, actionDir, jobConf);
        } catch (final Exception e) {
            LOG.warn("Exception in check(). Message[{0}]", e.getMessage(), e);
            throw convertException(e);
        }

        final String newJobId = findNewHadoopJobId(context, action);

        // check the Hadoop job if newID is defined (which should be the case here) - otherwise perform the normal check()
        if (newJobId != null) {
            boolean jobCompleted;
            JobClient jobClient = null;
            boolean exception = false;

            try {
                jobClient = createJobClient(context, new JobConf(jobConf));
                final JobID jobid = JobID.forName(newJobId);
                final RunningJob runningJob = jobClient.getJob(jobid);

                if (runningJob == null) {
                    context.setExternalStatus(FAILED);
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA017",
                            "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", newJobId,
                            action.getId());
                }

                jobCompleted = runningJob.isComplete();
            } catch (Exception e) {
                LOG.warn("Unable to check the state of a running MapReduce job -"
                        + " please check the health of the Job History Server!", e);
                exception = true;
                throw convertException(e);
            } finally {
                if (jobClient != null) {
                    try {
                        jobClient.close();
                    } catch (Exception e) {
                        if (exception) {
                            LOG.error("JobClient error (not re-throwing due to a previous error): ", e);
                        } else {
                            throw convertException(e);
                        }
                    }
                }
            }

            // run original check() if the MR action is completed or there are errors - otherwise mark it as RUNNING
            if (jobCompleted || actionData.containsKey(LauncherAMUtils.ACTION_DATA_ERROR_PROPS)) {
                super.check(context, action);
            } else {
                context.setExternalStatus(RUNNING);
                final String externalAppId = TypeConverter.toYarn(JobID.forName(newJobId)).getAppId().toString();
                context.setExternalChildIDs(externalAppId);
            }
        } else {
            super.check(context, action);
        }
    }

    @Override
    void injectActionCallback(Context context, Configuration actionConf) {
        injectCallback(context, actionConf);
    }

    private String findNewHadoopJobId(final Context context, final WorkflowAction action) throws ActionExecutorException {
        try {
            final Configuration jobConf = createJobConfFromActionConf(context, action);

            return new HadoopJobIdFinder(jobConf, context).find();
        } catch (final HadoopAccessorException | IOException | JDOMException | URISyntaxException | InterruptedException |
                NoSuchAlgorithmException e) {
            LOG.warn("Exception while trying to find new Hadoop job id(). Message[{0}]", e.getMessage(), e);
            throw convertException(e);
        }
    }

    private Configuration createJobConfFromActionConf(final Context context, final WorkflowAction action)
            throws JDOMException, NoSuchAlgorithmException {
        final Element actionXml = XmlUtils.parseXml(action.getConf());
        final Configuration jobConf = createBaseHadoopConf(context, actionXml);

        final String launcherTag = getActionYarnTag(context, action);
        jobConf.set(CHILD_MAPREDUCE_JOB_TAGS, LauncherHelper.getTag(launcherTag));

        return jobConf;
    }

    /**
     * Find YARN application ID only for {@link MapReduceActionExecutor} delegating to {@link YarnApplicationIdFinder}.
     * @param context the execution context
     * @param action the workflow action
     * @return the YARN application ID as a {@code String}
     * @throws ActionExecutorException when the YARN application ID could not be found
     */
    @Override
    protected String findYarnApplicationId(final Context context, final WorkflowAction action) throws ActionExecutorException {
        try {
            final Configuration jobConf = createJobConfFromActionConf(context, action);
            final HadoopJobIdFinder hadoopJobIdFinder = new HadoopJobIdFinder(jobConf, context);

            return new YarnApplicationIdFinder(hadoopJobIdFinder,
                    new YarnApplicationReportReader(jobConf), (WorkflowActionBean) action).find();
        }
        catch (final  IOException | HadoopAccessorException | JDOMException | InterruptedException | URISyntaxException |
                NoSuchAlgorithmException e) {
            LOG.warn("Exception while finding YARN application id. Message[{0}]", e.getMessage(), e);
            throw convertException(e);
        }
    }

    /**
     * Finds a Hadoop job ID based on {@code action-data.seq} file stored on HDFS by {@link MapReduceMain}.
     */
    @VisibleForTesting
    static class HadoopJobIdFinder {
        private final Configuration jobConf;
        private final Context executorContext;

        HadoopJobIdFinder(final Configuration jobConf, final Context executorContext) {
            this.jobConf = jobConf;
            this.executorContext = executorContext;
        }

        String find() throws HadoopAccessorException, IOException, URISyntaxException, InterruptedException {
            final FileSystem actionFs = executorContext.getAppFileSystem();
            final Path actionDir = executorContext.getActionDir();
            final Map<String, String> actionData = LauncherHelper.getActionData(actionFs, actionDir, jobConf);

            return actionData.get(LauncherAMUtils.ACTION_DATA_NEW_ID);
        }
    }

    /**
     * Find YARN application ID in three stages:
     * <ul>
     *     <li>based on {@code action-data.seq} written by {@link MapReduceMain}, if already present. If present and is not the
     *     Oozie Launcher's application ID ({@link WorkflowAction#getExternalId()}), gets used. Else, fall back to following:</li>
     *     <li>if not found, look up the appropriate YARN child ID</li>
     *     <li>if an appropriate YARN application ID is not found, go with Oozie Launcher's application ID
     *     ({@link WorkflowAction#getExternalId()})</li>
     * </ul>
     */
    @VisibleForTesting
    static class YarnApplicationIdFinder {
        private static final XLog LOG = XLog.getLog(YarnApplicationIdFinder.class);

        private final HadoopJobIdFinder hadoopJobIdFinder;
        private final YarnApplicationReportReader reader;
        private final WorkflowActionBean workflowActionBean;

        YarnApplicationIdFinder(final HadoopJobIdFinder hadoopJobIdFinder,
                                final YarnApplicationReportReader reader,
                                final WorkflowActionBean workflowActionBean) {
            this.hadoopJobIdFinder = hadoopJobIdFinder;
            this.reader = reader;
            this.workflowActionBean = workflowActionBean;
        }

        String find() throws IOException, HadoopAccessorException, URISyntaxException, InterruptedException {
            final String newJobId = hadoopJobIdFinder.find();
            if (Strings.isNullOrEmpty(newJobId) && !isHadoopJobId(newJobId)) {
                LOG.trace("Is not a Hadoop Job Id, falling back.");
                return fallbackToYarnChildOrExternalId();
            }

            final String effectiveApplicationId;
            final String newApplicationId = TypeConverter.toYarn(JobID.forName(newJobId)).getAppId().toString();

            if (workflowActionBean.getExternalId().equals(newApplicationId) || newApplicationId == null) {
                LOG.trace("New YARN application ID {0} is empty or is the same as {1}, falling back.",
                        newApplicationId, workflowActionBean.getExternalId());
                effectiveApplicationId = fallbackToYarnChildOrExternalId();
            }
            else {
                LOG.trace("New YARN application ID {0} is different, using it.", newApplicationId);
                effectiveApplicationId = newApplicationId;
            }

            return effectiveApplicationId;
        }

        /**
         * When a Hadoop could not be found, fall back finding the YARN child application ID, or the workflow's {@code externalId}:
         * <ul>
         *     <li>look for YARN children of the actual {@code WorkflowActionBean}</li>
         *     <li>filter for type {@code MAPREDUCE}. Note that those will be the YARN application children of the original
         *     {@code Oozie Launcher} type. Filter also for the ones not in YARN applications' terminal states. What remains is the
         *     one we call YARN child ID</li>
         *     <li>if not found, go with {@link WorkflowActionBean#externalId}</li>
         *     <li>if the found one is not newer than the one already stored, go with {@link WorkflowActionBean#externalId}</li>
         *     <li>if found and there is no {@link WorkflowActionBean#externalId}, go with the YARN child ID</li>
         *     <li>else, go with the YARN child ID</li>
         * </ul>
         * @return the YARN child application's ID, or the workflow action's external ID
         */
        private String fallbackToYarnChildOrExternalId() {
            final List<ApplicationReport> childYarnApplications = reader.read();
            childYarnApplications.removeIf(new Predicate<ApplicationReport>() {
                @Override
                public boolean test(ApplicationReport applicationReport) {
                    return !applicationReport.getApplicationType().equals(YARN_APPLICATION_TYPE_MAPREDUCE);
                }
            });

            if (childYarnApplications.isEmpty()) {
                LOG.trace("No child YARN applications present, returning {0} instead", workflowActionBean.getExternalId());
                return workflowActionBean.getExternalId();
            }

            final String yarnChildId = getLastYarnId(childYarnApplications);

            if (Strings.isNullOrEmpty(yarnChildId)) {
                LOG.trace("yarnChildId is empty, returning {0} instead", workflowActionBean.getExternalId());
                return workflowActionBean.getExternalId();
            }

            if (Strings.isNullOrEmpty(workflowActionBean.getExternalId())) {
                LOG.trace("workflowActionBean.externalId is empty, returning {0} instead", yarnChildId);
                return yarnChildId;
            }

            if (new YarnApplicationIdComparator().compare(yarnChildId, workflowActionBean.getExternalId()) > 0) {
                LOG.trace("yarnChildId is newer, returning {0}", yarnChildId);
                return yarnChildId;
            }

            LOG.trace("yarnChildId is not newer, returning {0}", workflowActionBean.getExternalId());
            return workflowActionBean.getExternalId();
        }

        /**
         * Get the biggest YARN application ID given {@link YarnApplicationIdComparator}.
         * @param yarnApplications the YARN application reports
         * @return the biggest {@link ApplicationReport#getApplicationId()#toString()}
         */
        @VisibleForTesting
        protected String getLastYarnId(final List<ApplicationReport> yarnApplications) {
            Preconditions.checkNotNull(yarnApplications, "YARN application list should be filled");
            Preconditions.checkArgument(!yarnApplications.isEmpty(), "no YARN applications in the list");

            final Iterable<String> unorderedApplicationIds =
                    Iterables.transform(yarnApplications, new Function<ApplicationReport, String>() {
                        @Override
                        public String apply(final ApplicationReport input) {
                            Preconditions.checkNotNull(input, "YARN application should be filled");
                            return input.getApplicationId().toString();
                        }
                    });

            return Ordering.from(new YarnApplicationIdComparator()).max(unorderedApplicationIds);
        }

        private boolean isHadoopJobId(final String jobIdCandidate) {
            try {
                return JobID.forName(jobIdCandidate) != null;
            } catch (final IllegalArgumentException e) {
                LOG.warn("Job ID candidate is not a Hadoop Job ID.", e);
                return false;
            }
        }
    }

    /**
     * Compares two YARN application IDs in the sense:
     * <ul>
     *     <li>originating from different cluster timestamps the one with the bigger timestamp is considered greater</li>
     *     <li>originating from the same cluster timestamp the one with the higher sequence number is considered greater</li>
     *     <li>originating from the same cluster timestamp and with the same sequence number both are considered equal</li>
     * </ul>
     */
    @VisibleForTesting
    @SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE", justification = "instances will never be serialized")
    static class YarnApplicationIdComparator implements Comparator<String> {
        private static final String PREFIX = "application_";
        private static final String SEPARATOR = "_";

        @Override
        public int compare(final String left, final String right) {
            // Let's say two application IDs with different cluster timestamps are equal
            final int middleLongPartComparisonResult = Long.compare(getMiddleLongPart(left), getMiddleLongPart(right));
            if (middleLongPartComparisonResult != 0) {
                return middleLongPartComparisonResult;
            }

            // Else we compare the sequence number
            return Integer.compare(getLastIntegerPart(left), getLastIntegerPart(right));
        }

        private long getMiddleLongPart(final String applicationId) {
            return Long.parseLong(applicationId.substring(applicationId.indexOf(PREFIX) + PREFIX.length(),
                    applicationId.lastIndexOf(SEPARATOR)));
        }

        private int getLastIntegerPart(final String applicationId) {
            return Integer.parseInt(applicationId.substring(applicationId.lastIndexOf(SEPARATOR) + SEPARATOR.length()));
        }
    }

    /**
     * Encapsulates call to the static method
     * {@link LauncherMain#getChildYarnApplications(Configuration, ApplicationsRequestScope, long)} for better testability.
     */
    @VisibleForTesting
    static class YarnApplicationReportReader {
        private final Configuration jobConf;

        YarnApplicationReportReader(final Configuration jobConf) {
            this.jobConf = jobConf;
        }

        List<ApplicationReport> read() {
            return LauncherMain.getChildYarnApplications(jobConf, ApplicationsRequestScope.OWN, 0L);
        }
    }
}
