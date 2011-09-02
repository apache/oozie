/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.DiskChecker;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class JavaActionExecutor extends ActionExecutor {

    private static final String HADOOP_USER = "user.name";
    private static final String HADOOP_UGI = "hadoop.job.ugi";
    private static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    private static final String HADOOP_NAME_NODE = "fs.default.name";

    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();

    private static int maxActionOutputLen;

    private static final String SUCCEEDED = "SUCCEEDED";
    private static final String KILLED = "KILLED";
    private static final String FAILED = "FAILED";
    private static final String FAILED_KILLED = "FAILED/KILLED";
    private static final String RUNNING = "RUNNING";
    private XLog log = XLog.getLog(getClass());

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_UGI);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_NAME_NODE);
        DISALLOWED_PROPERTIES.add(WorkflowAppService.HADOOP_JT_KERBEROS_NAME);
        DISALLOWED_PROPERTIES.add(WorkflowAppService.HADOOP_NN_KERBEROS_NAME);
    }

    public JavaActionExecutor() {
        this("java");
    }

    protected JavaActionExecutor(String type) {
        super(type);
    }

    protected String getLauncherJarName() {
        return getType() + "-launcher.jar";
    }

    protected List<Class> getLauncherClasses() {
        List<Class> classes = new ArrayList<Class>();
        classes.add(LauncherMapper.class);
        classes.add(LauncherSecurityManager.class);
        classes.add(LauncherException.class);
        return classes;
    }

    @Override
    public void initActionType() {
        super.initActionType();
        maxActionOutputLen = getOozieConf().getInt(CallbackServlet.CONF_MAX_DATA_LEN, 2 * 1024);
        try {
            List<Class> classes = getLauncherClasses();
            Class[] launcherClasses = classes.toArray(new Class[classes.size()]);
            IOUtils.createJar(new File(getOozieRuntimeDir()), getLauncherJarName(), launcherClasses);

            registerError(UnknownHostException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "JA001");
            registerError(AccessControlException.class.getName(), ActionExecutorException.ErrorType.NON_TRANSIENT,
                          "JA002");
            registerError(DiskChecker.DiskOutOfSpaceException.class.getName(),
                          ActionExecutorException.ErrorType.NON_TRANSIENT, "JA003");
            registerError(org.apache.hadoop.hdfs.protocol.QuotaExceededException.class.getName(),
                          ActionExecutorException.ErrorType.NON_TRANSIENT, "JA004");
            registerError(org.apache.hadoop.hdfs.server.namenode.SafeModeException.class.getName(),
                          ActionExecutorException.ErrorType.NON_TRANSIENT, "JA005");
            registerError(ConnectException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "JA006");
            registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "JA007");
            registerError(FileNotFoundException.class.getName(), ActionExecutorException.ErrorType.ERROR, "JA008");
            registerError(IOException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "JA009");
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    void checkForDisallowedProps(Configuration conf, String confName) throws ActionExecutorException {
        for (String prop : DISALLOWED_PROPERTIES) {
            if (conf.get(prop) != null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                                                  "Property [{0}] not allowed in action [{1}] configuration", prop, confName);
            }
        }
    }

    Configuration createBaseHadoopConf(Context context, Element actionXml) {
        Configuration conf = new XConfiguration();
        conf.set(HADOOP_USER, context.getProtoActionConf().get(WorkflowAppService.HADOOP_USER));
        conf.set(HADOOP_UGI, context.getProtoActionConf().get(WorkflowAppService.HADOOP_UGI));
        if (context.getProtoActionConf().get(WorkflowAppService.HADOOP_JT_KERBEROS_NAME) != null) {
            conf.set(WorkflowAppService.HADOOP_JT_KERBEROS_NAME, context.getProtoActionConf().get(
                    WorkflowAppService.HADOOP_JT_KERBEROS_NAME));
        }
        if (context.getProtoActionConf().get(WorkflowAppService.HADOOP_NN_KERBEROS_NAME) != null) {
            conf.set(WorkflowAppService.HADOOP_NN_KERBEROS_NAME, context.getProtoActionConf().get(
                    WorkflowAppService.HADOOP_NN_KERBEROS_NAME));
        }
        conf.set(OozieClient.GROUP_NAME, context.getProtoActionConf().get(OozieClient.GROUP_NAME));
        Namespace ns = actionXml.getNamespace();
        String jobTracker = actionXml.getChild("job-tracker", ns).getTextTrim();
        String nameNode = actionXml.getChild("name-node", ns).getTextTrim();
        conf.set(HADOOP_JOB_TRACKER, jobTracker);
        conf.set(HADOOP_NAME_NODE, nameNode);
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true");
        return conf;
    }

    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context) throws ActionExecutorException {
        try {
            Namespace ns = actionXml.getNamespace();
            Element e = actionXml.getChild("configuration", ns);
            if (e != null) {
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));

                XConfiguration launcherConf = new XConfiguration();
                for (Map.Entry<String, String> entry : inlineConf) {
                    if (entry.getKey().startsWith("oozie.launcher.")) {
                        String name = entry.getKey().substring("oozie.launcher.".length());
                        String value = entry.getValue();
                        // setting original KEY
                        launcherConf.set(entry.getKey(), value);
                        // setting un-prefixed key (to allow Hadoop job config
                        // for the launcher job
                        launcherConf.set(name, value);
                    }
                }
                checkForDisallowedProps(launcherConf, "inline launcher configuration");
                XConfiguration.copy(launcherConf, conf);
            }
            return conf;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    protected FileSystem getActionFileSystem(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            return getActionFileSystem(context, actionXml);
        }
        catch (JDOMException ex) {
            throw convertException(ex);
        }
    }

    protected FileSystem getActionFileSystem(Context context, Element actionXml) throws ActionExecutorException {
        try {
            return context.getAppFileSystem();
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        try {
            Namespace ns = actionXml.getNamespace();
            Element e = actionXml.getChild("job-xml", ns);
            if (e != null) {
                String jobXml = e.getTextTrim();
                Path path = new Path(appPath, jobXml);
                FileSystem fs = getActionFileSystem(context, actionXml);
                Configuration jobXmlConf = new XConfiguration(fs.open(path));
                checkForDisallowedProps(jobXmlConf, "job-xml");
                XConfiguration.copy(jobXmlConf, actionConf);
            }
            e = actionXml.getChild("configuration", ns);
            if (e != null) {
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                checkForDisallowedProps(inlineConf, "inline configuration");
                XConfiguration.copy(inlineConf, actionConf);
            }
            return actionConf;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    Configuration addToCache(Configuration conf, Path appPath, String filePath, boolean archive)
            throws ActionExecutorException {
        Path path = null;
        try {
            if (filePath.startsWith("/")) {
                path = new Path(filePath);
            }
            else {
                path = new Path(appPath, filePath);
            }
            URI uri = new URI(path.toUri().getPath());
            if (archive) {
                DistributedCache.addCacheArchive(uri, conf);
            }
            else {
                String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
                if (fileName.endsWith(".so") || fileName.contains(".so.")) {  // .so files
                    if (!fileName.endsWith(".so")) {
                        int extAt = fileName.indexOf(".so.");
                        fileName = fileName.substring(0, extAt + 3);
                    }
                    uri = new Path(path.toString() + "#" + fileName).toUri();
                    uri = new URI(uri.getPath());
                }
                else if (fileName.endsWith(".jar")){  // .jar files
                    if (!fileName.contains("#")) {
                        path = new Path(uri.toString());

                        String user = conf.get("user.name");
                        String group = conf.get("group.name");
                        Services.get().get(HadoopAccessorService.class).addFileToClassPath(user, group, path, conf);
                    }
                }
                else { // regular files
                    if (!fileName.contains("#")) {
                        uri = new Path(path.toString() + "#" + fileName).toUri();
                        uri = new URI(uri.getPath());
                    }
                }
                DistributedCache.addCacheFile(uri, conf);
            }
            DistributedCache.createSymlink(conf);
            return conf;
        }
        catch (Exception ex) {
            XLog.getLog(getClass()).debug(
                    "Errors when add to DistributedCache. Path=" + path + ", archive=" + archive + ", conf="
                            + XmlUtils.prettyPrint(conf).toString());
            throw convertException(ex);
        }
    }

    String getOozieLauncherJar(Context context) throws ActionExecutorException {
        try {
            return new Path(context.getActionDir(), getLauncherJarName()).toString();
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    void prepareActionDir(FileSystem actionFs, Context context) throws ActionExecutorException {
        try {
            Path actionDir = context.getActionDir();
            Path tempActionDir = new Path(actionDir.getParent(), actionDir.getName() + ".tmp");
            if (!actionFs.exists(actionDir)) {
                try {
                    actionFs.copyFromLocalFile(new Path(getOozieRuntimeDir(), getLauncherJarName()), new Path(
                            tempActionDir, getLauncherJarName()));
                    actionFs.rename(tempActionDir, actionDir);
                }
                catch (IOException ex) {
                    actionFs.delete(tempActionDir, true);
                    actionFs.delete(actionDir, true);
                    throw ex;
                }
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    void cleanUpActionDir(FileSystem actionFs, Context context) throws ActionExecutorException {
        try {
            Path actionDir = context.getActionDir();
            if (!context.getProtoActionConf().getBoolean("oozie.action.keep.action.dir", false)
                    && actionFs.exists(actionDir)) {
                actionFs.delete(actionDir, true);
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    void setLibFilesArchives(Context context, Element actionXml, Path appPath, Configuration conf)
            throws ActionExecutorException {
        Configuration proto = context.getProtoActionConf();

        addToCache(conf, appPath, getOozieLauncherJar(context), false);

        String[] paths = proto.getStrings(WorkflowAppService.APP_LIB_PATH_LIST);
        if (paths != null) {
            for (String path : paths) {
                addToCache(conf, appPath, path, false);
            }
        }

        for (Element eProp : (List<Element>) actionXml.getChildren()) {
            if (eProp.getName().equals("file")) {
                String path = eProp.getTextTrim();
                addToCache(conf, appPath, path, false);
            }
            else {
                if (eProp.getName().equals("archive")) {
                    String path = eProp.getTextTrim();
                    addToCache(conf, appPath, path, true);
                }
            }
        }
    }

    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        Namespace ns = actionXml.getNamespace();
        Element e = actionXml.getChild("main-class", ns);
        return e.getTextTrim();
    }

    private static final Set<String> SPECIAL_PROPERTIES = new HashSet<String>();

    static {
        SPECIAL_PROPERTIES.add("mapred.job.queue.name");
        SPECIAL_PROPERTIES.add("mapreduce.jobtracker.kerberos.principal");
        SPECIAL_PROPERTIES.add("dfs.namenode.kerberos.principal");
    }

    @SuppressWarnings("unchecked")
    JobConf createLauncherConf(Context context, WorkflowAction action, Element actionXml, Configuration actionConf)
            throws ActionExecutorException {
        try {
            Path appPath = new Path(context.getWorkflow().getAppPath());

            // launcher job configuration
            Configuration launcherConf = createBaseHadoopConf(context, actionXml);
            setupLauncherConf(launcherConf, actionXml, appPath, context);

            // we are doing init+copy because if not we are getting 'hdfs'
            // scheme not known
            // its seems that new JobConf(Conf) does not load defaults, it
            // assumes parameter Conf does.
            JobConf launcherJobConf = new JobConf();
            XConfiguration.copy(launcherConf, launcherJobConf);
            setLibFilesArchives(context, actionXml, appPath, launcherJobConf);
            String jobName = XLog.format("oozie:launcher:T={0}:W={1}:A={2}:ID={3}", getType(), context.getWorkflow()
                    .getAppName(), action.getName(), context.getWorkflow().getId());
            launcherJobConf.setJobName(jobName);

            String jobId = context.getWorkflow().getId();
            String actionId = action.getId();
            Path actionDir = context.getActionDir();
            String recoveryId = context.getRecoveryId();

            LauncherMapper.setupLauncherInfo(launcherJobConf, jobId, actionId, actionDir, recoveryId, actionConf);

            LauncherMapper.setupMainClass(launcherJobConf, getLauncherMain(launcherConf, actionXml));

            LauncherMapper.setupMaxOutputData(launcherJobConf, maxActionOutputLen);

            Namespace ns = actionXml.getNamespace();
            List<Element> list = actionXml.getChildren("arg", ns);
            String[] args = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                args[i] = list.get(i).getTextTrim();
            }
            LauncherMapper.setupMainArguments(launcherJobConf, args);

            Element opt = actionXml.getChild("java-opts", ns);
            if (opt != null) {
                String opts = launcherConf.get("mapred.child.java.opts", "");
                opts = opts + " " + opt.getTextTrim();
                opts = opts.trim();
                launcherJobConf.set("mapred.child.java.opts", opts);
            }

            // properties from action that are needed by the launcher (QUEUE
            // NAME)
            // maybe we should add queue to the WF schema, below job-tracker
            for (String name : SPECIAL_PROPERTIES) {
                String value = actionConf.get(name);
                if (value != null) {
                    launcherJobConf.set(name, value);
                }
            }

            // to disable cancelation of delegation token on launcher job end
            launcherJobConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

            // setting the group owning the Oozie job to allow anybody in that
            // group to kill the jobs.
            launcherJobConf.set("mapreduce.job.acl-modify-job", context.getWorkflow().getGroup());

            return launcherJobConf;
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private void injectCallback(Context context, Configuration conf) {
        String callback = context.getCallbackUrl("$jobStatus");
        if (conf.get("job.end.notification.url") != null) {
            XLog.getLog(getClass()).warn("Overriding the action job end notification URI");
        }
        conf.set("job.end.notification.url", callback);
    }

    void injectActionCallback(Context context, Configuration actionConf) {
        injectCallback(context, actionConf);
    }

    void injectLauncherCallback(Context context, Configuration launcherConf) {
        injectCallback(context, launcherConf);
    }

    void submitLauncher(Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        try {
            Path appPath = new Path(context.getWorkflow().getAppPath());
            Element actionXml = XmlUtils.parseXml(action.getConf());

            // action job configuration
            Configuration actionConf = createBaseHadoopConf(context, actionXml);
            setupActionConf(actionConf, context, actionXml, appPath);
            XLog.getLog(getClass()).debug("Setting LibFilesArchives ");
            setLibFilesArchives(context, actionXml, appPath, actionConf);
            String jobName = XLog.format("oozie:action:T={0}:W={1}:A={2}:ID={3}", getType(), context.getWorkflow()
                    .getAppName(), action.getName(), context.getWorkflow().getId());
            actionConf.set("mapred.job.name", jobName);
            injectActionCallback(context, actionConf);

            // setting the group owning the Oozie job to allow anybody in that
            // group to kill the jobs.
            actionConf.set("mapreduce.job.acl-modify-job", context.getWorkflow().getGroup());

            JobConf launcherJobConf = createLauncherConf(context, action, actionXml, actionConf);
            injectLauncherCallback(context, launcherJobConf);
            XLog.getLog(getClass()).debug("Creating Job Client for action " + action.getId());
            jobClient = createJobClient(context, launcherJobConf);
            String launcherId = LauncherMapper.getRecoveryId(launcherJobConf, context.getActionDir(), context
                    .getRecoveryId());
            boolean alreadyRunning = launcherId != null;
            RunningJob runningJob;

            if (alreadyRunning) {
                runningJob = jobClient.getJob(JobID.forName(launcherId));
                if (runningJob == null) {
                    String jobTracker = launcherJobConf.get("mapred.job.tracker");
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                                                      "unknown job [{0}@{1}], cannot recover", launcherId, jobTracker);
                }
            }
            else {
                prepare(context, actionXml);
                XLog.getLog(getClass()).debug("Submitting the job through Job Client for action " + action.getId());

                // setting up propagation of the delegation token.
                AuthHelper.get().set(jobClient, launcherJobConf);
                log.debug(WorkflowAppService.HADOOP_JT_KERBEROS_NAME + " = "
                        + launcherJobConf.get(WorkflowAppService.HADOOP_JT_KERBEROS_NAME));
                log.debug(WorkflowAppService.HADOOP_NN_KERBEROS_NAME + " = "
                        + launcherJobConf.get(WorkflowAppService.HADOOP_NN_KERBEROS_NAME));
                runningJob = jobClient.submitJob(launcherJobConf);
                if (runningJob == null) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                                                      "Error submitting launcher for action [{0}]", action.getId());
                }
                launcherId = runningJob.getID().toString();
                XLog.getLog(getClass()).debug("After submission get the launcherId " + launcherId);
            }

            String jobTracker = launcherJobConf.get(HADOOP_JOB_TRACKER);
            String consoleUrl = runningJob.getTrackingURL();
            context.setStartData(launcherId, jobTracker, consoleUrl);
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

    void prepare(Context context, Element actionXml) throws ActionExecutorException {
        Namespace ns = actionXml.getNamespace();
        Element prepare = actionXml.getChild("prepare", ns);
        if (prepare != null) {
            XLog.getLog(getClass()).debug("Preparing the action with FileSystem operation");
            FsActionExecutor fsAe = new FsActionExecutor();
            fsAe.doOperations(context, prepare);
            XLog.getLog(getClass()).debug("FS Operation is completed");
        }
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            XLog.getLog(getClass()).debug("Starting action " + action.getId() + " getting Action File System");
            FileSystem actionFs = getActionFileSystem(context, action);
            XLog.getLog(getClass()).debug("Preparing action Dir through copying " + context.getActionDir());
            prepareActionDir(actionFs, context);
            XLog.getLog(getClass()).debug("Action Dir is ready. Submitting the action ");
            submitLauncher(context, action);
            XLog.getLog(getClass()).debug("Action submit completed. Performing check ");
            check(context, action);
            XLog.getLog(getClass()).debug("Action check is done after submission");
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            String externalStatus = action.getExternalStatus();
            WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ? WorkflowAction.Status.OK
                                           : WorkflowAction.Status.ERROR;
            context.setEndData(status, getActionSignal(status));
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
        finally {
            try {
                FileSystem actionFs = getActionFileSystem(context, action);
                cleanUpActionDir(actionFs, context);
            }
            catch (Exception ex) {
                throw convertException(ex);
            }
        }
    }

    /**
     * Create job client object
     * @param context
     * @param jobConf
     * @return
     * @throws HadoopAccessorException
     */
    protected JobClient createJobClient(Context context, JobConf jobConf) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        String group = context.getWorkflow().getGroup();
        return Services.get().get(HadoopAccessorService.class).createJobClient(user, group, jobConf);
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            FileSystem actionFs = getActionFileSystem(context, actionXml);
            Configuration conf = createBaseHadoopConf(context, actionXml);
            JobConf jobConf = new JobConf();
            XConfiguration.copy(conf, jobConf);
            jobClient = createJobClient(context, jobConf);
            RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalId()));
            if (runningJob == null) {
                context.setExternalStatus(FAILED);
                context.setExecutionData(FAILED, null);
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA017",
                                                  "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", action
                        .getExternalId(), action.getId());
            }
            if (runningJob.isComplete()) {
                Path actionDir = context.getActionDir();

                String user = context.getWorkflow().getUser();
                String group = context.getWorkflow().getGroup();
                if (LauncherMapper.hasIdSwap(runningJob, user, group, actionDir)) {
                    String launcherId = action.getExternalId();
                    Path idSwapPath = LauncherMapper.getIdSwapPath(context.getActionDir());
                    InputStream is = actionFs.open(idSwapPath);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                    Properties props = PropertiesUtils.readProperties(reader, maxActionOutputLen);
                    reader.close();
                    String newId = props.getProperty("id");
                    runningJob = jobClient.getJob(JobID.forName(newId));
                    if (runningJob == null) {
                        context.setExternalStatus(FAILED);
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA017",
                                                          "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", newId,
                                                          action.getId());
                    }

                    context.setStartData(newId, action.getTrackerUri(), runningJob.getTrackingURL());
                    XLog.getLog(getClass()).info(XLog.STD, "External ID swap, old ID [{0}] new ID [{1}]", launcherId,
                                                 newId);
                }
                if (runningJob.isComplete()) {
                    XLog.getLog(getClass()).info(XLog.STD, "action completed, external ID [{0}]",
                                                 action.getExternalId());
                    if (runningJob.isSuccessful() && LauncherMapper.isMainSuccessful(runningJob)) {
                        Properties props = null;
                        if (getCaptureOutput(action)) {
                            props = new Properties();
                            if (LauncherMapper.hasOutputData(runningJob)) {
                                Path actionOutput = LauncherMapper.getOutputDataPath(context.getActionDir());
                                InputStream is = actionFs.open(actionOutput);
                                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                                props = PropertiesUtils.readProperties(reader, maxActionOutputLen);
                                reader.close();
                            }
                        }
                        context.setExecutionData(SUCCEEDED, props);
                        XLog.getLog(getClass()).info(XLog.STD, "action produced output");
                    }
                    else {
                        XLog log = XLog.getLog(getClass());
                        String errorReason;
                        Path actionError = LauncherMapper.getErrorPath(context.getActionDir());
                        if (actionFs.exists(actionError)) {
                            InputStream is = actionFs.open(actionError);
                            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                            Properties props = PropertiesUtils.readProperties(reader, -1);
                            reader.close();
                            errorReason = props.getProperty("error.reason");
                            log.warn("Launcher ERROR, reason: {0}", errorReason);
                            String exMsg = props.getProperty("exception.message");
                            String errorInfo = (exMsg != null) ? exMsg : errorReason;
                            context.setErrorInfo("JA018", errorInfo);
                            String exStackTrace = props.getProperty("exception.stacktrace");
                            if (exMsg != null) {
                                log.warn("Launcher exception: {0}{E}{1}", exMsg, exStackTrace);
                            }
                        }
                        else {
                            errorReason = XLog.format("LauncherMapper died, check Hadoop log for job [{0}:{1}]", action
                                    .getTrackerUri(), action.getExternalId());
                            log.warn(errorReason);
                        }
                        context.setExecutionData(FAILED_KILLED, null);
                    }
                }
                else {
                    context.setExternalStatus(RUNNING);
                    XLog.getLog(getClass()).info(XLog.STD, "checking action, external ID [{0}] status [{1}]",
                                                 action.getExternalId(), action.getExternalStatus());
                }
            }
            else {
                context.setExternalStatus(RUNNING);
                XLog.getLog(getClass()).info(XLog.STD, "checking action, external ID [{0}] status [{1}]",
                                             action.getExternalId(), action.getExternalStatus());
            }
        }
        catch (Exception ex) {
            XLog.getLog(getClass()).warn("Exception in check(). Message[{0}]", ex.getMessage(), ex);
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

    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        Element eConf = XmlUtils.parseXml(action.getConf());
        Namespace ns = eConf.getNamespace();
        Element captureOutput = eConf.getChild("capture-output", ns);
        return captureOutput != null;
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            Configuration conf = createBaseHadoopConf(context, actionXml);
            JobConf jobConf = new JobConf();
            XConfiguration.copy(conf, jobConf);
            jobClient = createJobClient(context, jobConf);
            RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalId()));
            if (runningJob != null) {
                runningJob.killJob();
            }
            context.setExternalStatus(KILLED);
            context.setExecutionData(KILLED, null);
        }
        catch (Exception ex) {
            exception = true;
            throw convertException(ex);
        }
        finally {
            try {
                FileSystem actionFs = getActionFileSystem(context, action);
                cleanUpActionDir(actionFs, context);
                if (jobClient != null) {
                    jobClient.close();
                }
            }
            catch (Exception ex) {
                if (exception) {
                    log.error("Error: ", ex);
                }
                else {
                    throw convertException(ex);
                }
            }
        }
    }

    private static Set<String> FINAL_STATUS = new HashSet<String>();

    static {
        FINAL_STATUS.add(SUCCEEDED);
        FINAL_STATUS.add(KILLED);
        FINAL_STATUS.add(FAILED);
        FINAL_STATUS.add(FAILED_KILLED);
    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return FINAL_STATUS.contains(externalStatus);
    }

}
