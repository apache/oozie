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
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.util.DiskChecker;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.HadoopAccessor;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.JDOMException;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.ConnectException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Properties;

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

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_UGI);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_NAME_NODE);
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

    public void initActionType() {
        super.initActionType();
        maxActionOutputLen = getOozieConf().getInt(CallbackServlet.CONF_MAX_DATA_LEN, 2 * 1024);
        try {
            List<Class> classes = getLauncherClasses();
            classes.add(Services.get().get(HadoopAccessorService.class).getAccessorClass());
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
        Namespace ns = actionXml.getNamespace();
        String jobTracker = actionXml.getChild("job-tracker", ns).getTextTrim();
        String nameNode = actionXml.getChild("name-node", ns).getTextTrim();
        conf.set(HADOOP_JOB_TRACKER, jobTracker);
        conf.set(HADOOP_NAME_NODE, nameNode);

        // set the HadoopAccessor implementation to use from Launcher Main
        String accessorClass = Services.get().get(HadoopAccessorService.class).getAccessorClass().getName();
        conf.set("oozie.hadoop.accessor.class", accessorClass);

        return conf;
    }

    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath) throws ActionExecutorException {
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
        try {
            Path path;
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
                if (fileName.endsWith(".so") || fileName.contains(".so.")) {
                    if (!fileName.endsWith(".so")) {
                        int extAt = fileName.indexOf(".so.");
                        fileName = fileName.substring(0, extAt + 3);
                    }
                    uri = new Path(path.toString() + "#" + fileName).toUri();
                    uri = new URI(uri.getPath());
                }
                else if (!fileName.contains("#")) {
                    path = new Path(uri.toString());
                    DistributedCache.addFileToClassPath(path, conf);
                }
                DistributedCache.addCacheFile(uri, conf);
            }
            DistributedCache.createSymlink(conf);
            return conf;
        }
        catch (Exception ex) {
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

        String[] paths = proto.getStrings(WorkflowAppService.APP_LIB_JAR_PATH_LIST);
        if (paths != null) {
            for (String jarPath : paths) {
                addToCache(conf, appPath, jarPath, false);
            }
        }

        paths = proto.getStrings(WorkflowAppService.APP_LIB_SO_PATH_LIST);
        if (paths != null) {
            for (String soPath : paths) {
                addToCache(conf, appPath, soPath, false);
            }
        }

        for (Element eProp : (List<Element>) actionXml.getChildren()) {
            if (eProp.getName().equals("file")) {
                String path = eProp.getTextTrim();
                addToCache(conf, appPath, path, false);
            }
            else if (eProp.getName().equals("archive")) {
                String path = eProp.getTextTrim();
                addToCache(conf, appPath, path, true);
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
    }

    @SuppressWarnings("unchecked")
    JobConf createLauncherConf(Context context, WorkflowAction action, Element actionXml, Configuration actionConf)
            throws ActionExecutorException {
        try {
            Path appPath = new Path(context.getWorkflow().getAppPath());

            // launcher job configuration
            Configuration launcherConf = createBaseHadoopConf(context, actionXml);
            setupLauncherConf(launcherConf, actionXml, appPath);

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
            List<Element> list = (List<Element>) actionXml.getChildren("arg", ns);
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
        try {
            Path appPath = new Path(context.getWorkflow().getAppPath());
            Element actionXml = XmlUtils.parseXml(action.getConf());

            // action job configuration
            Configuration actionConf = createBaseHadoopConf(context, actionXml);
            setupActionConf(actionConf, context, actionXml, appPath);
            setLibFilesArchives(context, actionXml, appPath, actionConf);
            String jobName = XLog.format("oozie:action:T={0}:W={1}:A={2}:ID={3}", getType(), context.getWorkflow()
                    .getAppName(), action.getName(), context.getWorkflow().getId());
            actionConf.set("mapred.job.name", jobName);
            injectActionCallback(context, actionConf);

            JobConf launcherJobConf = createLauncherConf(context, action, actionXml, actionConf);
            injectLauncherCallback(context, launcherJobConf);

            JobClient jobClient = createJobClient(context, launcherJobConf);
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
                runningJob = jobClient.submitJob(launcherJobConf);
                launcherId = runningJob.getID().toString();
            }

            String jobTracker = launcherJobConf.get(HADOOP_JOB_TRACKER);
            String consoleUrl = runningJob.getTrackingURL();
            context.setStartData(launcherId, jobTracker, consoleUrl);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    void prepare(Context context, Element actionXml) throws ActionExecutorException {
        Namespace ns = actionXml.getNamespace();
        Element prepare = actionXml.getChild("prepare", ns);
        if (prepare != null) {
            FsActionExecutor fsAe = new FsActionExecutor();
            fsAe.doOperations(context, prepare);
        }
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            FileSystem actionFs = getActionFileSystem(context, action);
            prepareActionDir(actionFs, context);
            submitLauncher(context, action);
            check(context, action);
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

    protected JobClient createJobClient(Context context, JobConf jobConf) throws IOException {
        String user = context.getWorkflow().getUser();
        String group = context.getWorkflow().getGroup();
        HadoopAccessor hadoopAccessor = Services.get().get(HadoopAccessorService.class).get(user, group);
        return hadoopAccessor.createJobClient(jobConf);
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            FileSystem actionFs = getActionFileSystem(context, actionXml);
            Configuration conf = createBaseHadoopConf(context, actionXml);
            JobConf jobConf = new JobConf();
            XConfiguration.copy(conf, jobConf);
            JobClient jobClient = createJobClient(context, jobConf);
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
                    context.setStartData(newId, action.getTrackerUri(), runningJob.getTrackingURL());
                    XLog.getLog(getClass()).info(XLog.STD, "External ID swap, old ID [{0}] new ID [{1}]",
                                launcherId, newId);
                }
                if (runningJob.isComplete()) {
                    XLog.getLog(getClass()).info(XLog.STD, "action completed, external ID [{0}]",
                            action.getExternalId());
                    if (runningJob.isSuccessful() && LauncherMapper.isMainSuccessful(runningJob)) {
                        Properties props = null;
                        Element eConf = XmlUtils.parseXml(action.getConf());
                        Namespace ns = eConf.getNamespace();
                        Element captureOutput = eConf.getChild("capture-output", ns);
                        if (captureOutput != null) {
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

            throw convertException(ex);
        }
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            Configuration conf = createBaseHadoopConf(context, actionXml);
            JobConf jobConf = new JobConf();
            XConfiguration.copy(conf, jobConf);
            JobClient jobClient = createJobClient(context, jobConf);
            RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalId()));
            runningJob.killJob();
            context.setExternalStatus(KILLED);
            context.setExecutionData(KILLED, null);
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

    private static Set<String> FINAL_STATUS = new HashSet<String>();

    static {
        FINAL_STATUS.add(SUCCEEDED);
        FINAL_STATUS.add(KILLED);
        FINAL_STATUS.add(FAILED);
        FINAL_STATUS.add(FAILED_KILLED);
    }

    public boolean isCompleted(String externalStatus) {
        return FINAL_STATUS.contains(externalStatus);
    }

}