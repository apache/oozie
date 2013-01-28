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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.util.DiskChecker;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

public class JavaActionExecutor extends ActionExecutor {

    private static final String HADOOP_USER = "user.name";
    public static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    public static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    public static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    public static final String HADOOP_NAME_NODE = "fs.default.name";
    private static final String HADOOP_JOB_NAME = "mapred.job.name";
    public static final String OOZIE_COMMON_LIBDIR = "oozie";
    public static final int MAX_EXTERNAL_STATS_SIZE_DEFAULT = Integer.MAX_VALUE;
    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();
    public final static String MAX_EXTERNAL_STATS_SIZE = "oozie.external.stats.max.size";
    public static final String ACL_VIEW_JOB = "mapreduce.job.acl-view-job";
    public static final String ACL_MODIFY_JOB = "mapreduce.job.acl-modify-job";
    private static int maxActionOutputLen;
    private static int maxExternalStatsSize;

    private static final String SUCCEEDED = "SUCCEEDED";
    private static final String KILLED = "KILLED";
    private static final String FAILED = "FAILED";
    private static final String FAILED_KILLED = "FAILED/KILLED";
    private static final String RUNNING = "RUNNING";
    protected XLog log = XLog.getLog(getClass());

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_NAME_NODE);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER_2);
        DISALLOWED_PROPERTIES.add(HADOOP_YARN_RM);
    }

    public JavaActionExecutor() {
        this("java");
        requiresNNJT = true;
    }

    protected JavaActionExecutor(String type) {
        super(type);
        requiresNNJT = true;
    }

    protected String getLauncherJarName() {
        return getType() + "-launcher.jar";
    }

    protected List<Class> getLauncherClasses() {
        List<Class> classes = new ArrayList<Class>();
        classes.add(LauncherMapper.class);
        classes.add(LauncherSecurityManager.class);
        classes.add(LauncherException.class);
        classes.add(LauncherMainException.class);
        classes.add(FileSystemActions.class);
        classes.add(PrepareActionsDriver.class);
        classes.add(ActionStats.class);
        classes.add(ActionType.class);
        return classes;
    }

    @Override
    public void initActionType() {
        XLog log = XLog.getLog(getClass());
        super.initActionType();
        maxActionOutputLen = getOozieConf()
          .getInt(LauncherMapper.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA,
          // TODO: Remove the below config get in a subsequent release..
          // This other irrelevant property is only used to
          // preserve backwards compatibility cause of a typo.
          // See OOZIE-4.
          getOozieConf().getInt(CallbackServlet.CONF_MAX_DATA_LEN,
            2 * 1024));
        //Get the limit for the maximum allowed size of action stats
        maxExternalStatsSize = getOozieConf().getInt(JavaActionExecutor.MAX_EXTERNAL_STATS_SIZE, MAX_EXTERNAL_STATS_SIZE_DEFAULT);
        maxExternalStatsSize = (maxExternalStatsSize == -1) ? Integer.MAX_VALUE : maxExternalStatsSize;
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
            registerError(ConnectException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "  JA006");
            registerError(JDOMException.class.getName(), ActionExecutorException.ErrorType.ERROR, "JA007");
            registerError(FileNotFoundException.class.getName(), ActionExecutorException.ErrorType.ERROR, "JA008");
            registerError(IOException.class.getName(), ActionExecutorException.ErrorType.TRANSIENT, "JA009");
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        catch (java.lang.NoClassDefFoundError err) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            err.printStackTrace(new PrintStream(baos));
            log.warn(baos.toString());
        }
    }

    /**
     * Get the maximum allowed size of stats
     *
     * @return maximum size of stats
     */
    public static int getMaxExternalStatsSize() {
        return maxExternalStatsSize;
    }

    static void checkForDisallowedProps(Configuration conf, String confName) throws ActionExecutorException {
        for (String prop : DISALLOWED_PROPERTIES) {
            if (conf.get(prop) != null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                        "Property [{0}] not allowed in action [{1}] configuration", prop, confName);
            }
        }
    }

    public JobConf createBaseHadoopConf(Context context, Element actionXml) {
        Namespace ns = actionXml.getNamespace();
        String jobTracker = actionXml.getChild("job-tracker", ns).getTextTrim();
        String nameNode = actionXml.getChild("name-node", ns).getTextTrim();
        JobConf conf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);
        conf.set(HADOOP_USER, context.getProtoActionConf().get(WorkflowAppService.HADOOP_USER));
        conf.set(HADOOP_JOB_TRACKER, jobTracker);
        conf.set(HADOOP_JOB_TRACKER_2, jobTracker);
        conf.set(HADOOP_YARN_RM, jobTracker);
        conf.set(HADOOP_NAME_NODE, nameNode);
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true");
        return conf;
    }

    private void injectLauncherProperties(Configuration srcConf, Configuration launcherConf) {
        for (Map.Entry<String, String> entry : srcConf) {
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
    }

    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        try {
            Namespace ns = actionXml.getNamespace();
            Element e = actionXml.getChild("configuration", ns);
            if (e != null) {
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));

                XConfiguration launcherConf = new XConfiguration();
                HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
                XConfiguration actionDefaultConf = has.createActionDefaultConf(conf.get(HADOOP_JOB_TRACKER), getType());
                injectLauncherProperties(actionDefaultConf, launcherConf);
                injectLauncherProperties(inlineConf, launcherConf);
                checkForDisallowedProps(launcherConf, "launcher configuration");
                XConfiguration.copy(launcherConf, conf);
            }
            return conf;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    public static void parseJobXmlAndConfiguration(Context context, Element element, Path appPath, Configuration conf)
            throws IOException, ActionExecutorException, HadoopAccessorException, URISyntaxException {
        Namespace ns = element.getNamespace();
        Iterator<Element> it = element.getChildren("job-xml", ns).iterator();
        while (it.hasNext()) {
            Element e = it.next();
            String jobXml = e.getTextTrim();
            Path path = new Path(appPath, jobXml);
            FileSystem fs = context.getAppFileSystem();
            Configuration jobXmlConf = new XConfiguration(fs.open(path));
            checkForDisallowedProps(jobXmlConf, "job-xml");
            XConfiguration.copy(jobXmlConf, conf);
        }
        Element e = element.getChild("configuration", ns);
        if (e != null) {
            String strConf = XmlUtils.prettyPrint(e).toString();
            XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
            checkForDisallowedProps(inlineConf, "inline configuration");
            XConfiguration.copy(inlineConf, conf);
        }
    }

    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        try {
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            XConfiguration actionDefaults = has.createActionDefaultConf(actionConf.get(HADOOP_JOB_TRACKER), getType());
            XConfiguration.injectDefaults(actionDefaults, actionConf);

            has.checkSupportedFilesystem(appPath.toUri());

            parseJobXmlAndConfiguration(context, actionXml, appPath, actionConf);
            return actionConf;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
        catch (HadoopAccessorException ex) {
            throw convertException(ex);
        }
        catch (URISyntaxException ex) {
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
                    uri = new Path(path.toString() + "#" + fileName).toUri();
                    uri = new URI(uri.getPath());
                    DistributedCache.addCacheFile(uri, conf);
                }
                else if (fileName.endsWith(".jar")) { // .jar files
                    if (!fileName.contains("#")) {
                        path = new Path(uri.toString());

                        String user = conf.get("user.name");
                        Services.get().get(HadoopAccessorService.class).addFileToClassPath(user, path, conf);
                    }
                    else {
                        DistributedCache.addCacheFile(uri, conf);
                    }
                }
                else { // regular files
                    if (!fileName.contains("#")) {
                        uri = new Path(path.toString() + "#" + fileName).toUri();
                        uri = new URI(uri.getPath());
                    }
                    DistributedCache.addCacheFile(uri, conf);
                }
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

    public void prepareActionDir(FileSystem actionFs, Context context) throws ActionExecutorException {
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

    protected void addShareLib(Path appPath, Configuration conf, String actionShareLibName)
    throws ActionExecutorException {
        if (actionShareLibName != null) {
            try {
                Path systemLibPath = Services.get().get(WorkflowAppService.class).getSystemLibPath();
                if (systemLibPath != null) {
                    Path actionLibPath = new Path(systemLibPath, actionShareLibName);
                    String user = conf.get("user.name");
                    FileSystem fs;
                    // If the actionLibPath has a valid scheme and authority, then use them to determine the filesystem that the
                    // sharelib resides on; otherwise, assume it resides on the same filesystem as the appPath and use the appPath
                    // to determine the filesystem
                    if (actionLibPath.toUri().getScheme() != null && actionLibPath.toUri().getAuthority() != null) {
                        fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, actionLibPath.toUri(), conf);
                    }
                    else {
                        fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, appPath.toUri(), conf);
                    }
                    if (fs.exists(actionLibPath)) {
                        FileStatus[] files = fs.listStatus(actionLibPath);
                        for (FileStatus file : files) {
                            addToCache(conf, actionLibPath, file.getPath().toUri().getPath(), false);
                        }
                    }
                }
            }
            catch (HadoopAccessorException ex){
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                        ex.getErrorCode().toString(), ex.getMessage());
            }
            catch (IOException ex){
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                        "It should never happen", ex.getMessage());
            }
        }
    }

    protected void addActionLibs(Path appPath, Configuration conf) throws ActionExecutorException {
        String[] actionLibsStrArr = conf.getStrings("oozie.launcher.oozie.libpath");
        if (actionLibsStrArr != null) {
            try {
                for (String actionLibsStr : actionLibsStrArr) {
                    actionLibsStr = actionLibsStr.trim();
                    if (actionLibsStr.length() > 0)
                    {
                        Path actionLibsPath = new Path(actionLibsStr);
                        String user = conf.get("user.name");
                        FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, appPath.toUri(), conf);
                        if (fs.exists(actionLibsPath)) {
                            FileStatus[] files = fs.listStatus(actionLibsPath);
                            for (FileStatus file : files) {
                                addToCache(conf, appPath, file.getPath().toUri().getPath(), false);
                            }
                        }
                    }
                }
            }
            catch (HadoopAccessorException ex){
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                        ex.getErrorCode().toString(), ex.getMessage());
            }
            catch (IOException ex){
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                        "It should never happen", ex.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    void setLibFilesArchives(Context context, Element actionXml, Path appPath, Configuration conf)
            throws ActionExecutorException {
        Configuration proto = context.getProtoActionConf();

        // launcher JAR
        addToCache(conf, appPath, getOozieLauncherJar(context), false);

        // Workflow lib/
        String[] paths = proto.getStrings(WorkflowAppService.APP_LIB_PATH_LIST);
        if (paths != null) {
            for (String path : paths) {
                addToCache(conf, appPath, path, false);
            }
        }

        // Action libs
        addActionLibs(appPath, conf);

        // files and archives defined in the action
        for (Element eProp : (List<Element>) actionXml.getChildren()) {
            if (eProp.getName().equals("file")) {
                String[] filePaths = eProp.getTextTrim().split(",");
                for (String path : filePaths) {
                    addToCache(conf, appPath, path.trim(), false);
                }
            }
            else if (eProp.getName().equals("archive")) {
                String[] archivePaths = eProp.getTextTrim().split(",");
                for (String path : archivePaths){
                    addToCache(conf, appPath, path.trim(), true);
                }
            }
        }

        addAllShareLibs(appPath, conf, context, actionXml);
	}

    // Adds action specific share libs and common share libs
    private void addAllShareLibs(Path appPath, Configuration conf, Context context, Element actionXml)
            throws ActionExecutorException {
        // Add action specific share libs
        addActionShareLib(appPath, conf, context, actionXml);
        // Add common sharelibs for Oozie
        addShareLib(appPath, conf, JavaActionExecutor.OOZIE_COMMON_LIBDIR);
    }

    private void addActionShareLib(Path appPath, Configuration conf, Context context, Element actionXml)
            throws ActionExecutorException {
        XConfiguration wfJobConf = null;
        try {
            wfJobConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
        }
        catch (IOException ioe) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "It should never happen",
                    ioe.getMessage());
        }
        // Action sharelibs are only added if user has specified to use system libpath
        if (wfJobConf.getBoolean(OozieClient.USE_SYSTEM_LIBPATH, false)) {
            // add action specific sharelibs
            addShareLib(appPath, conf, getShareLibName(context, actionXml, conf));
        }
    }


    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        Namespace ns = actionXml.getNamespace();
        Element e = actionXml.getChild("main-class", ns);
        return e.getTextTrim();
    }

    private static final String QUEUE_NAME = "mapred.job.queue.name";

    private static final Set<String> SPECIAL_PROPERTIES = new HashSet<String>();

    static {
        SPECIAL_PROPERTIES.add(QUEUE_NAME);
        SPECIAL_PROPERTIES.add(ACL_VIEW_JOB);
        SPECIAL_PROPERTIES.add(ACL_MODIFY_JOB);
    }

    @SuppressWarnings("unchecked")
    JobConf createLauncherConf(FileSystem actionFs, Context context, WorkflowAction action, Element actionXml, Configuration actionConf)
            throws ActionExecutorException {
        try {

            // app path could be a file
            Path appPathRoot = new Path(context.getWorkflow().getAppPath());
            if (actionFs.isFile(appPathRoot)) {
                appPathRoot = appPathRoot.getParent();
            }

            // launcher job configuration
            JobConf launcherJobConf = createBaseHadoopConf(context, actionXml);
            setupLauncherConf(launcherJobConf, actionXml, appPathRoot, context);

            String actionShareLibProperty = actionConf.get(ACTION_SHARELIB_FOR + getType());
            if (actionShareLibProperty != null) {
                launcherJobConf.set(ACTION_SHARELIB_FOR + getType(), actionShareLibProperty);
            }
            setLibFilesArchives(context, actionXml, appPathRoot, launcherJobConf);

            String jobName = launcherJobConf.get(HADOOP_JOB_NAME);
            if (jobName == null || jobName.isEmpty()) {
                jobName = XLog.format(
                        "oozie:launcher:T={0}:W={1}:A={2}:ID={3}", getType(),
                        context.getWorkflow().getAppName(), action.getName(),
                        context.getWorkflow().getId());
            launcherJobConf.setJobName(jobName);
            }

            String jobId = context.getWorkflow().getId();
            String actionId = action.getId();
            Path actionDir = context.getActionDir();
            String recoveryId = context.getRecoveryId();

            // Getting the prepare XML from the action XML
            Namespace ns = actionXml.getNamespace();
            Element prepareElement = actionXml.getChild("prepare", ns);
            String prepareXML = "";
            if (prepareElement != null) {
                if (prepareElement.getChildren().size() > 0) {
                    prepareXML = XmlUtils.prettyPrint(prepareElement).toString().trim();
                }
            }
            LauncherMapper.setupLauncherInfo(launcherJobConf, jobId, actionId, actionDir, recoveryId, actionConf,
                    prepareXML);

            LauncherMapper.setupMainClass(launcherJobConf, getLauncherMain(launcherJobConf, actionXml));
            LauncherMapper.setupSupportedFileSystems(
                launcherJobConf, Services.get().getConf().get(HadoopAccessorService.SUPPORTED_FILESYSTEMS));
            LauncherMapper.setupMaxOutputData(launcherJobConf, maxActionOutputLen);
            LauncherMapper.setupMaxExternalStatsSize(launcherJobConf, maxExternalStatsSize);

            List<Element> list = actionXml.getChildren("arg", ns);
            String[] args = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                args[i] = list.get(i).getTextTrim();
            }
            LauncherMapper.setupMainArguments(launcherJobConf, args);

            List<Element> javaopts = actionXml.getChildren("java-opt", ns);
            for (Element opt: javaopts) {
                String opts = launcherJobConf.get("mapred.child.java.opts", "");
                opts = opts + " " + opt.getTextTrim();
                opts = opts.trim();
                launcherJobConf.set("mapred.child.java.opts", opts);
            }

            Element opt = actionXml.getChild("java-opts", ns);
            if (opt != null) {
                String opts = launcherJobConf.get("mapred.child.java.opts", "");
                opts = opts + " " + opt.getTextTrim();
                opts = opts.trim();
                launcherJobConf.set("mapred.child.java.opts", opts);
            }

            // properties from action that are needed by the launcher (e.g. QUEUE NAME, ACLs)
            // maybe we should add queue to the WF schema, below job-tracker
            actionConfToLauncherConf(actionConf, launcherJobConf);

            // to disable cancelation of delegation token on launcher job end
            launcherJobConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

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

    private void actionConfToLauncherConf(Configuration actionConf, JobConf launcherConf) {
        for (String name : SPECIAL_PROPERTIES) {
            if (actionConf.get(name) != null && launcherConf.get("oozie.launcher." + name) == null) {
                launcherConf.set(name, actionConf.get(name));
            }
        }
    }

    public void submitLauncher(FileSystem actionFs, Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        try {
            Path appPathRoot = new Path(context.getWorkflow().getAppPath());

            // app path could be a file
            if (actionFs.isFile(appPathRoot)) {
                appPathRoot = appPathRoot.getParent();
            }

            Element actionXml = XmlUtils.parseXml(action.getConf());

            // action job configuration
            Configuration actionConf = createBaseHadoopConf(context, actionXml);
            setupActionConf(actionConf, context, actionXml, appPathRoot);
            XLog.getLog(getClass()).debug("Setting LibFilesArchives ");
            setLibFilesArchives(context, actionXml, appPathRoot, actionConf);

            String jobName = actionConf.get(HADOOP_JOB_NAME);
            if (jobName == null || jobName.isEmpty()) {
                jobName = XLog.format("oozie:action:T={0}:W={1}:A={2}:ID={3}",
                        getType(), context.getWorkflow().getAppName(),
                        action.getName(), context.getWorkflow().getId());
                actionConf.set(HADOOP_JOB_NAME, jobName);
            }

            injectActionCallback(context, actionConf);

            if(actionConf.get(ACL_MODIFY_JOB) == null || actionConf.get(ACL_MODIFY_JOB).trim().equals("")) {
                // ONLY in the case where user has not given the
                // modify-job ACL specifically
                if (context.getWorkflow().getAcl() != null) {
                    // setting the group owning the Oozie job to allow anybody in that
                    // group to modify the jobs.
                    actionConf.set(ACL_MODIFY_JOB, context.getWorkflow().getAcl());
                }
            }

            // Setting the credential properties in launcher conf
            HashMap<String, CredentialsProperties> credentialsProperties = setCredentialPropertyToActionConf(context,
                    action, actionConf);

            // Adding if action need to set more credential tokens
            JobConf credentialsConf = new JobConf(false);
            XConfiguration.copy(actionConf, credentialsConf);
            setCredentialTokens(credentialsConf, context, action, credentialsProperties);

            // insert conf to action conf from credentialsConf
            for (Entry<String, String> entry : credentialsConf) {
                if (actionConf.get(entry.getKey()) == null) {
                    actionConf.set(entry.getKey(), entry.getValue());
                }
            }

            JobConf launcherJobConf = createLauncherConf(actionFs, context, action, actionXml, actionConf);
            injectLauncherCallback(context, launcherJobConf);
            XLog.getLog(getClass()).debug("Creating Job Client for action " + action.getId());
            jobClient = createJobClient(context, launcherJobConf);
            String launcherId = LauncherMapper.getRecoveryId(launcherJobConf, context.getActionDir(), context
                    .getRecoveryId());
            boolean alreadyRunning = launcherId != null;
            RunningJob runningJob;

            // if user-retry is on, always submit new launcher
            boolean isUserRetry = ((WorkflowActionBean)action).isUserRetry();

            if (alreadyRunning && !isUserRetry) {
                runningJob = jobClient.getJob(JobID.forName(launcherId));
                if (runningJob == null) {
                    String jobTracker = launcherJobConf.get(HADOOP_JOB_TRACKER);
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                            "unknown job [{0}@{1}], cannot recover", launcherId, jobTracker);
                }
            }
            else {
                XLog.getLog(getClass()).debug("Submitting the job through Job Client for action " + action.getId());

                // setting up propagation of the delegation token.
                Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(HadoopAccessorService
                        .getMRDelegationTokenRenewer(launcherJobConf));
                launcherJobConf.getCredentials().addToken(HadoopAccessorService.MR_TOKEN_ALIAS, mrdt);

                // insert credentials tokens to launcher job conf if needed
                if (needInjectCredentials()) {
                    for (Token<? extends TokenIdentifier> tk : credentialsConf.getCredentials().getAllTokens()) {
                        log.debug("ADDING TOKEN: " + tk.getKind().toString());
                        launcherJobConf.getCredentials().addToken(tk.getKind(), tk);
                    }
                }
                else {
                    log.info("No need to inject credentials.");
                }
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

    private boolean needInjectCredentials() {
        boolean methodExists = true;

        Class klass;
        try {
            klass = Class.forName("org.apache.hadoop.mapred.JobConf");
            klass.getMethod("getCredentials");
        }
        catch (ClassNotFoundException ex) {
            methodExists = false;
        }
        catch (NoSuchMethodException ex) {
            methodExists = false;
        }

        return methodExists;
    }

    protected HashMap<String, CredentialsProperties> setCredentialPropertyToActionConf(Context context,
            WorkflowAction action, Configuration actionConf) throws Exception {
        HashMap<String, CredentialsProperties> credPropertiesMap = null;
        if (context != null && action != null) {
            credPropertiesMap = getActionCredentialsProperties(context, action);
            if (credPropertiesMap != null) {
                for (String key : credPropertiesMap.keySet()) {
                    CredentialsProperties prop = credPropertiesMap.get(key);
                    if (prop != null) {
                        log.debug("Credential Properties set for action : " + action.getId());
                        for (String property : prop.getProperties().keySet()) {
                            actionConf.set(property, prop.getProperties().get(property));
                            log.debug("property : '" + property + "', value : '" + prop.getProperties().get(property) + "'");
                        }
                    }
                }
            }
            else {
                log.warn("No credential properties found for action : " + action.getId() + ", cred : " + action.getCred());
            }
        }
        else {
            log.warn("context or action is null");
        }
        return credPropertiesMap;
    }

    protected void setCredentialTokens(JobConf jobconf, Context context, WorkflowAction action,
            HashMap<String, CredentialsProperties> credPropertiesMap) throws Exception {

        if (context != null && action != null && credPropertiesMap != null) {
            for (Entry<String, CredentialsProperties> entry : credPropertiesMap.entrySet()) {
                String credName = entry.getKey();
                CredentialsProperties credProps = entry.getValue();
                if (credProps != null) {
                    CredentialsProvider credProvider = new CredentialsProvider(credProps.getType());
                    Credentials credentialObject = credProvider.createCredentialObject();
                    if (credentialObject != null) {
                        credentialObject.addtoJobConf(jobconf, credProps, context);
                        log.debug("Retrieved Credential '" + credName + "' for action " + action.getId());
                    }
                    else {
                        log.debug("Credentials object is null for name= " + credName + ", type=" + credProps.getType());
                    }
                }
            }
        }

    }

    protected HashMap<String, CredentialsProperties> getActionCredentialsProperties(Context context,
            WorkflowAction action) throws Exception {
        HashMap<String, CredentialsProperties> props = new HashMap<String, CredentialsProperties>();
        if (context != null && action != null) {
            String credsInAction = action.getCred();
            log.debug("Get credential '" + credsInAction + "' properties for action : " + action.getId());
            String[] credNames = credsInAction.split(",");
            for (String credName : credNames) {
                CredentialsProperties credProps = getCredProperties(context, credName);
                props.put(credName, credProps);
            }
        }
        else {
            log.warn("context or action is null");
        }
        return props;
    }

    @SuppressWarnings("unchecked")
    protected CredentialsProperties getCredProperties(Context context, String credName)
            throws Exception {
        CredentialsProperties credProp = null;
        String workflowXml = ((WorkflowJobBean) context.getWorkflow()).getWorkflowInstance().getApp().getDefinition();
        XConfiguration wfjobConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
        Element elementJob = XmlUtils.parseXml(workflowXml);
        Element credentials = elementJob.getChild("credentials", elementJob.getNamespace());
        if (credentials != null) {
            for (Element credential : (List<Element>) credentials.getChildren("credential", credentials.getNamespace())) {
                String name = credential.getAttributeValue("name");
                String type = credential.getAttributeValue("type");
                log.debug("getCredProperties: Name: " + name + ", Type: " + type);
                if (name.equalsIgnoreCase(credName)) {
                    credProp = new CredentialsProperties(name, type);
                    for (Element property : (List<Element>) credential.getChildren("property",
                            credential.getNamespace())) {
                        String propertyName = property.getChildText("name", property.getNamespace());
                        String propertyValue = property.getChildText("value", property.getNamespace());
                        ELEvaluator eval = new ELEvaluator();
                        for (Map.Entry<String, String> entry : wfjobConf) {
                            eval.setVariable(entry.getKey(), entry.getValue().trim());
                        }
                        propertyName = eval.evaluate(propertyName, String.class);
                        propertyValue = eval.evaluate(propertyValue, String.class);

                        credProp.getProperties().put(propertyName, propertyValue);
                        log.debug("getCredProperties: Properties name :'" + propertyName + "', Value : '"
                                + propertyValue + "'");
                    }
                }
            }
        } else {
            log.warn("credentials is null for the action");
        }
        return credProp;
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            XLog.getLog(getClass()).debug("Starting action " + action.getId() + " getting Action File System");
            FileSystem actionFs = context.getAppFileSystem();
            XLog.getLog(getClass()).debug("Preparing action Dir through copying " + context.getActionDir());
            prepareActionDir(actionFs, context);
            XLog.getLog(getClass()).debug("Action Dir is ready. Submitting the action ");
            submitLauncher(actionFs, context, action);
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
                FileSystem actionFs = context.getAppFileSystem();
                cleanUpActionDir(actionFs, context);
            }
            catch (Exception ex) {
                throw convertException(ex);
            }
        }
    }

    /**
     * Create job client object
     *
     * @param context
     * @param jobConf
     * @return
     * @throws HadoopAccessorException
     */
    protected JobClient createJobClient(Context context, JobConf jobConf) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        String group = context.getWorkflow().getGroup();
        return Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            FileSystem actionFs = context.getAppFileSystem();
            JobConf jobConf = createBaseHadoopConf(context, actionXml);
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
                        getActionData(actionFs, runningJob, action, context);
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
                            String errorCode = props.getProperty("error.code");
                            if (errorCode.equals("0")) {
                                errorCode = "JA018";
                            }
                            if (errorCode.equals("-1")) {
                                errorCode = "JA019";
                            }
                            errorReason = props.getProperty("error.reason");
                            log.warn("Launcher ERROR, reason: {0}", errorReason);
                            String exMsg = props.getProperty("exception.message");
                            String errorInfo = (exMsg != null) ? exMsg : errorReason;
                            context.setErrorInfo(errorCode, errorInfo);
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
                        setActionCompletionData(context, actionFs);
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

    /**
     * Get the output data of an action. Subclasses should override this method
     * to get action specific output data.
     *
     * @param actionFs the FileSystem object
     * @param runningJob the runningJob
     * @param action the Workflow action
     * @param context executor context
     *
     */
    protected void getActionData(FileSystem actionFs, RunningJob runningJob, WorkflowAction action, Context context)
            throws HadoopAccessorException, JDOMException, IOException, URISyntaxException {
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
            JobConf jobConf = createBaseHadoopConf(context, actionXml);
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
                FileSystem actionFs = context.getAppFileSystem();
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


    /**
     * Return the sharelib name for the action.
     * <p/>
     * If <code>NULL</code> or emtpy, it means that the action does not use the action
     * sharelib.
     * <p/>
     * If a non-empty string, i.e. <code>foo</code>, it means the action uses the
     * action sharelib subdirectory <code>foo</code> and all JARs in the sharelib
     * <code>foo</code> directory will be in the action classpath.
     * <p/>
     * The resolution is done using the following precedence order:
     * <ul>
     *     <li><b>action.sharelib.for.#ACTIONTYPE#</b> in the action configuration</li>
     *     <li><b>action.sharelib.for.#ACTIONTYPE#</b> in the job configuration</li>
     *     <li><b>action.sharelib.for.#ACTIONTYPE#</b> in the oozie configuration</li>
     *     <li>Action Executor <code>getDefaultShareLibName()</code> method</li>
     * </ul>
     *
     *
     * @param context executor context.
     * @param actionXml
     *@param conf action configuration.  @return the action sharelib name.
     */
    protected String getShareLibName(Context context, Element actionXml, Configuration conf) {
        String name = conf.get(ACTION_SHARELIB_FOR + getType());
        if (name == null) {
            try {
                XConfiguration jobConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
                name = jobConf.get(ACTION_SHARELIB_FOR + getType());
                if (name == null) {
                    name = Services.get().getConf().get(ACTION_SHARELIB_FOR + getType());
                    if (name == null) {
                        name = getDefaultShareLibName(actionXml);
                    }
                }
            }
            catch (IOException ex) {
                throw new RuntimeException("It cannot happen, " + ex.toString(), ex);
            }
        }
        return name;
    }

    private final static String ACTION_SHARELIB_FOR = "oozie.action.sharelib.for.";


    /**
     * Returns the default sharelib name for the action if any.
     *
     * @param actionXml the action XML fragment.
     * @return the sharelib name for the action, <code>NULL</code> if none.
     */
    protected String getDefaultShareLibName(Element actionXml) {
        return null;
    }

    /**
     * Sets some data for the action on completion
     *
     * @param context executor context
     * @param actionFs the FileSystem object
     */
    protected void setActionCompletionData(Context context, FileSystem actionFs) throws IOException,
            HadoopAccessorException, URISyntaxException {
    }
}
