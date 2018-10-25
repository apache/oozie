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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.coord.CoordActionStartXCommand;
import org.apache.oozie.command.wf.WorkflowXCommand;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.ClasspathUtils;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.FSUtils;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;


public class JavaActionExecutor extends ActionExecutor {
    public static final String RUNNING = "RUNNING";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String KILLED = "KILLED";
    public static final String FAILED = "FAILED";
    public static final String FAILED_KILLED = "FAILED/KILLED";
    public static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    public static final String HADOOP_NAME_NODE = "fs.default.name";
    public static final String OOZIE_COMMON_LIBDIR = "oozie";

    public static final String DEFAULT_LAUNCHER_VCORES = "oozie.launcher.default.vcores";
    public static final String DEFAULT_LAUNCHER_MEMORY_MB = "oozie.launcher.default.memory.mb";
    public static final String DEFAULT_LAUNCHER_PRIORITY = "oozie.launcher.default.priority";
    public static final String DEFAULT_LAUNCHER_QUEUE = "oozie.launcher.default.queue";
    public static final String DEFAULT_LAUNCHER_MAX_ATTEMPTS = "oozie.launcher.default.max.attempts";
    public static final String LAUNCER_MODIFY_ACL = "oozie.launcher.modify.acl";
    public static final String LAUNCER_VIEW_ACL = "oozie.launcher.view.acl";

    public static final String MAPREDUCE_TO_CLASSPATH = "mapreduce.needed.for";
    public static final String OOZIE_LAUNCHER_ADD_MAPREDUCE_TO_CLASSPATH_PROPERTY = ActionExecutor.CONF_PREFIX
            + MAPREDUCE_TO_CLASSPATH;

    public static final String MAX_EXTERNAL_STATS_SIZE = "oozie.external.stats.max.size";
    public static final String ACL_VIEW_JOB = "mapreduce.job.acl-view-job";
    public static final String ACL_MODIFY_JOB = "mapreduce.job.acl-modify-job";
    public static final String HADOOP_YARN_TIMELINE_SERVICE_ENABLED = "yarn.timeline-service.enabled";
    public static final String HADOOP_YARN_UBER_MODE = "mapreduce.job.ubertask.enable";
    public static final String OOZIE_ACTION_LAUNCHER_PREFIX = ActionExecutor.CONF_PREFIX  + "launcher.";
    public static final String HADOOP_YARN_KILL_CHILD_JOBS_ON_AMRESTART =
            OOZIE_ACTION_LAUNCHER_PREFIX + "am.restart.kill.childjobs";
    public static final String HADOOP_MAP_MEMORY_MB = "mapreduce.map.memory.mb";
    public static final String HADOOP_CHILD_JAVA_OPTS = "mapred.child.java.opts";
    public static final String HADOOP_MAP_JAVA_OPTS = "mapreduce.map.java.opts";
    public static final String HADOOP_REDUCE_JAVA_OPTS = "mapreduce.reduce.java.opts";
    public static final String HADOOP_CHILD_JAVA_ENV = "mapred.child.env";
    public static final String HADOOP_MAP_JAVA_ENV = "mapreduce.map.env";
    public static final String HADOOP_JOB_CLASSLOADER = "mapreduce.job.classloader";
    public static final String HADOOP_USER_CLASSPATH_FIRST = "mapreduce.user.classpath.first";
    public static final String OOZIE_CREDENTIALS_SKIP = "oozie.credentials.skip";
    public static final String YARN_AM_RESOURCE_MB = "yarn.app.mapreduce.am.resource.mb";
    public static final String YARN_AM_COMMAND_OPTS = "yarn.app.mapreduce.am.command-opts";
    public static final String YARN_AM_ENV = "yarn.app.mapreduce.am.env";
    public static final int YARN_MEMORY_MB_MIN = 512;

    private static final String JAVA_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.JavaMain";
    private static final String HADOOP_JOB_NAME = "mapred.job.name";
    static final Set<String> DISALLOWED_PROPERTIES = ImmutableSet.of(
            OozieClient.USER_NAME, MRJobConfig.USER_NAME, HADOOP_NAME_NODE, HADOOP_YARN_RM
    );
    private static final String OOZIE_ACTION_NAME = "oozie.action.name";
    private final static String ACTION_SHARELIB_FOR = "oozie.action.sharelib.for.";
    public static final String OOZIE_ACTION_DEPENDENCY_DEDUPLICATE = "oozie.action.dependency.deduplicate";

    /**
     * Heap to physical memory ration for {@link LauncherAM}, in order its YARN container doesn't get killed before physical memory
     * gets exhausted.
     */
    private static final double LAUNCHER_HEAP_PMEM_RATIO = 0.8;

    /**
     * Matches one or more occurrence of {@code Xmx}, {@code Xms}, {@code mx}, {@code ms}, {@code XX:MaxHeapSize}, or
     * {@code XX:MinHeapSize} JVM parameters, mixed with any other content.
     * <p>
     * Examples:
     * <ul>
     *     <li>{@code -Xms384m}</li>
     *     <li>{@code -Xmx:789k}</li>
     *     <li>{@code -XX:MaxHeapSize=123g}</li>
     *     <li>{@code -ms:384m}</li>
     *     <li>{@code -mx789k}</li>
     *     <li>{@code -XX:MinHeapSize=123g}</li>
     * </ul>
     */
    @VisibleForTesting
    @SuppressFBWarnings(value = {"REDOS"}, justification = "Complex regular expression")
    static final Pattern HEAP_MODIFIERS_PATTERN =
            Pattern.compile(".*((\\-X?m[s|x][\\:]?)|(\\-XX\\:(Min|Max)HeapSize\\=))([0-9]+[kKmMgG]?).*");

    private static int maxActionOutputLen;
    private static int maxExternalStatsSize;
    private static int maxFSGlobMax;

    protected static final String HADOOP_USER = "user.name";

    protected XLog LOG = XLog.getLog(getClass());
    private static final String JAVA_TMP_DIR_SETTINGS = "-Djava.io.tmpdir=";

    private static DependencyDeduplicator dependencyDeduplicator = new DependencyDeduplicator();

    public XConfiguration workflowConf = null;

    public JavaActionExecutor() {
        this("java");
    }

    protected JavaActionExecutor(String type) {
        super(type);
    }

    public static List<Class<?>> getCommonLauncherClasses() {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        classes.add(LauncherMain.class);
        classes.addAll(Services.get().get(URIHandlerService.class).getClassesForLauncher());
        classes.add(LauncherAM.class);
        classes.add(LauncherAMCallbackNotifier.class);
        return classes;
    }

    public List<Class<?>> getLauncherClasses() {
       List<Class<?>> classes = new ArrayList<Class<?>>();
        try {
            classes.add(Class.forName(JAVA_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    public void initActionType() {
        super.initActionType();
        maxActionOutputLen = ConfigurationService.getInt(LauncherAM.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA);
        //Get the limit for the maximum allowed size of action stats
        maxExternalStatsSize = ConfigurationService.getInt(JavaActionExecutor.MAX_EXTERNAL_STATS_SIZE);
        maxExternalStatsSize = (maxExternalStatsSize == -1) ? Integer.MAX_VALUE : maxExternalStatsSize;
        //Get the limit for the maximum number of globbed files/dirs for FS operation
        maxFSGlobMax = ConfigurationService.getInt(LauncherAMUtils.CONF_OOZIE_ACTION_FS_GLOB_MAX);

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

    public Configuration createBaseHadoopConf(Context context, Element actionXml) {
        return createBaseHadoopConf(context, actionXml, true);
    }

    protected Configuration createBaseHadoopConf(Context context, Element actionXml, boolean loadResources) {

        Namespace ns = actionXml.getNamespace();
        String resourceManager;
        final Element resourceManagerTag = actionXml.getChild("resource-manager", ns);
        if (resourceManagerTag != null) {
            resourceManager = resourceManagerTag.getTextTrim();
        }
        else {
            resourceManager = actionXml.getChild("job-tracker", ns).getTextTrim();
        }

        String nameNode = actionXml.getChild("name-node", ns).getTextTrim();
        Configuration conf = null;
        if (loadResources) {
            conf = Services.get().get(HadoopAccessorService.class).createConfiguration(resourceManager);
        }
        else {
            conf = new Configuration(false);
        }

        conf.set(HADOOP_USER, context.getProtoActionConf().get(WorkflowAppService.HADOOP_USER));
        conf.set(HADOOP_YARN_RM, resourceManager);
        conf.set(HADOOP_NAME_NODE, nameNode);
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true");

        // FIXME - think about this!
        Element e = actionXml.getChild("config-class", ns);
        if (e != null) {
            conf.set(LauncherAMUtils.OOZIE_ACTION_CONFIG_CLASS, e.getTextTrim());
        }

        return conf;
    }

    protected Configuration loadHadoopDefaultResources(Context context, Element actionXml) {
        return createBaseHadoopConf(context, actionXml);
    }

    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        try {
            Namespace ns = actionXml.getNamespace();
            XConfiguration launcherConf = new XConfiguration();
            // Inject action defaults for launcher
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            XConfiguration actionDefaultConf = has.createActionDefaultConf(conf.get(HADOOP_YARN_RM), getType());

            new LauncherConfigurationInjector(actionDefaultConf).inject(launcherConf);

            // Inject <job-xml> and <configuration> for launcher
            try {
                parseJobXmlAndConfiguration(context, actionXml, appPath, launcherConf, true);
            } catch (HadoopAccessorException ex) {
                throw convertException(ex);
            } catch (URISyntaxException ex) {
                throw convertException(ex);
            }
            XConfiguration.copy(launcherConf, conf);
            // Inject config-class for launcher to use for action
            Element e = actionXml.getChild("config-class", ns);
            if (e != null) {
                conf.set(LauncherAMUtils.OOZIE_ACTION_CONFIG_CLASS, e.getTextTrim());
            }
            checkForDisallowedProps(launcherConf, "launcher configuration");
            return conf;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    void injectLauncherTimelineServiceEnabled(Configuration launcherConf, Configuration actionConf) {
        // Getting delegation token for ATS. If tez-site.xml is present in distributed cache, turn on timeline service.
        if (actionConf.get("oozie.launcher." + HADOOP_YARN_TIMELINE_SERVICE_ENABLED) == null
                && ConfigurationService.getBoolean(OOZIE_ACTION_LAUNCHER_PREFIX + HADOOP_YARN_TIMELINE_SERVICE_ENABLED)) {
            String cacheFiles = launcherConf.get("mapred.cache.files");
            if (cacheFiles != null && cacheFiles.contains("tez-site.xml")) {
                launcherConf.setBoolean(HADOOP_YARN_TIMELINE_SERVICE_ENABLED, true);
            }
        }
    }

    public static void parseJobXmlAndConfiguration(Context context, Element element, Path appPath, Configuration conf)
            throws IOException, ActionExecutorException, HadoopAccessorException, URISyntaxException {
        parseJobXmlAndConfiguration(context, element, appPath, conf, false);
    }

    public static void parseJobXmlAndConfiguration(Context context, Element element, Path appPath, Configuration conf,
            boolean isLauncher) throws IOException, ActionExecutorException, HadoopAccessorException, URISyntaxException {
        Namespace ns = element.getNamespace();
        @SuppressWarnings("unchecked")
        Iterator<Element> it = element.getChildren("job-xml", ns).iterator();
        HashMap<String, FileSystem> filesystemsMap = new HashMap<String, FileSystem>();
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        while (it.hasNext()) {
            Element e = it.next();
            String jobXml = e.getTextTrim();
            Path pathSpecified = new Path(jobXml);
            Path path = pathSpecified.isAbsolute() ? pathSpecified : new Path(appPath, jobXml);
            FileSystem fs;
            if (filesystemsMap.containsKey(path.toUri().getAuthority())) {
              fs = filesystemsMap.get(path.toUri().getAuthority());
            }
            else {
              if (path.toUri().getAuthority() != null) {
                fs = has.createFileSystem(context.getWorkflow().getUser(), path.toUri(),
                        has.createConfiguration(path.toUri().getAuthority()));
              }
              else {
                fs = context.getAppFileSystem();
              }
              filesystemsMap.put(path.toUri().getAuthority(), fs);
            }
            Configuration jobXmlConf = new XConfiguration(fs.open(path));
            try {
                String jobXmlConfString = XmlUtils.prettyPrint(jobXmlConf).toString();
                jobXmlConfString = XmlUtils.removeComments(jobXmlConfString);
                jobXmlConfString = context.getELEvaluator().evaluate(jobXmlConfString, String.class);
                jobXmlConf = new XConfiguration(new StringReader(jobXmlConfString));
            }
            catch (ELEvaluationException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, "EL_EVAL_ERROR", ex
                        .getMessage(), ex);
            }
            catch (Exception ex) {
                context.setErrorInfo("EL_ERROR", ex.getMessage());
            }
            checkForDisallowedProps(jobXmlConf, "job-xml");
            if (isLauncher) {
                new LauncherConfigurationInjector(jobXmlConf).inject(conf);
            } else {
                XConfiguration.copy(jobXmlConf, conf);
            }
        }
        Element e = element.getChild("configuration", ns);
        if (e != null) {
            String strConf = XmlUtils.prettyPrint(e).toString();
            XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
            checkForDisallowedProps(inlineConf, "inline configuration");
            if (isLauncher) {
                new LauncherConfigurationInjector(inlineConf).inject(conf);
            } else {
                XConfiguration.copy(inlineConf, conf);
            }
        }
    }

    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        try {
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            XConfiguration actionDefaults = has.createActionDefaultConf(actionConf.get(HADOOP_YARN_RM), getType());
            XConfiguration.copy(actionDefaults, actionConf);
            has.checkSupportedFilesystem(appPath.toUri());

            // Set the Java Main Class for the Java action to give to the Java launcher
            setJavaMain(actionConf, actionXml);

            parseJobXmlAndConfiguration(context, actionXml, appPath, actionConf);

            // set cancel.delegation.token in actionConf that child job doesn't cancel delegation token
            actionConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
            setRootLoggerLevel(actionConf);
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

    /**
     * Set root log level property in actionConf
     * @param actionConf
     */
    void setRootLoggerLevel(Configuration actionConf) {
        String oozieActionTypeRootLogger = "oozie.action." + getType() + LauncherAMUtils.ROOT_LOGGER_LEVEL;
        String oozieActionRootLogger = "oozie.action." + LauncherAMUtils.ROOT_LOGGER_LEVEL;

        // check if root log level has already mentioned in action configuration
        String rootLogLevel = actionConf.get(oozieActionTypeRootLogger, actionConf.get(oozieActionRootLogger));
        if (rootLogLevel != null) {
            // root log level is mentioned in action configuration
            return;
        }

        // set the root log level which is mentioned in oozie default
        rootLogLevel = ConfigurationService.get(oozieActionTypeRootLogger);
        if (rootLogLevel != null && rootLogLevel.length() > 0) {
            actionConf.set(oozieActionRootLogger, rootLogLevel);
        }
        else {
            rootLogLevel = ConfigurationService.get(oozieActionRootLogger);
            if (rootLogLevel != null && rootLogLevel.length() > 0) {
                actionConf.set(oozieActionRootLogger, rootLogLevel);
            }
        }
    }

    Configuration addToCache(Configuration conf, Path appPath, String filePath, boolean archive)
            throws ActionExecutorException {

        URI uri = null;
        try {
            uri = new URI(getTrimmedEncodedPath(filePath));
            URI baseUri = appPath.toUri();
            if (uri.getScheme() == null) {
                String resolvedPath = uri.getPath();
                if (!resolvedPath.startsWith("/")) {
                    resolvedPath = baseUri.getPath() + "/" + resolvedPath;
                }
                uri = new URI(baseUri.getScheme(), baseUri.getAuthority(), resolvedPath, uri.getQuery(), uri.getFragment());
            }
            if (archive) {
                DistributedCache.addCacheArchive(uri.normalize(), conf);
            }
            else {
                String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
                if (fileName.endsWith(".so") || fileName.contains(".so.")) { // .so files
                    uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), fileName);
                    DistributedCache.addCacheFile(uri.normalize(), conf);
                }
                else if (fileName.endsWith(".jar")) { // .jar files
                    if (!fileName.contains("#")) {
                        String user = conf.get("user.name");

                        if (FSUtils.isNotLocalFile(fileName)) {
                            Path pathToAdd = new Path(uri.normalize());
                            Services.get().get(HadoopAccessorService.class).addFileToClassPath(user, pathToAdd, conf);
                        }
                    }
                    else {
                        DistributedCache.addCacheFile(uri.normalize(), conf);
                    }
                }
                else { // regular files
                    if (!fileName.contains("#")) {
                        uri = new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), fileName);
                    }
                    DistributedCache.addCacheFile(uri.normalize(), conf);
                }
            }
            DistributedCache.createSymlink(conf);
            return conf;
        }
        catch (Exception ex) {
            LOG.debug("Errors when add to DistributedCache. Path=" +
                    Objects.toString(uri, "<null>") + ", archive=" + archive + ", conf=" +
                    XmlUtils.prettyPrint(conf).toString());
            throw convertException(ex);
        }
    }

    public void prepareActionDir(FileSystem actionFs, Context context) throws ActionExecutorException {
        try {
            Path actionDir = context.getActionDir();
            Path tempActionDir = new Path(actionDir.getParent(), actionDir.getName() + ".tmp");
            if (!actionFs.exists(actionDir)) {
                try {
                    actionFs.mkdirs(tempActionDir);
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
            if (!context.getProtoActionConf().getBoolean(WorkflowXCommand.KEEP_WF_ACTION_DIR, false)
                    && actionFs.exists(actionDir)) {
                actionFs.delete(actionDir, true);
            }
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    protected void addShareLib(Configuration conf, String[] actionShareLibNames)
            throws ActionExecutorException {
        Set<String> confSet = new HashSet<String>(Arrays.asList(getShareLibFilesForActionConf() == null ? new String[0]
                : getShareLibFilesForActionConf()));

        Set<Path> sharelibList = new HashSet<Path>();

        if (actionShareLibNames != null) {
            try {
                ShareLibService shareLibService = Services.get().get(ShareLibService.class);
                FileSystem fs = shareLibService.getFileSystem();
                if (fs != null) {
                    for (String actionShareLibName : actionShareLibNames) {
                        List<Path> listOfPaths = shareLibService.getShareLibJars(actionShareLibName);
                        if (listOfPaths != null && !listOfPaths.isEmpty()) {
                            for (Path actionLibPath : listOfPaths) {
                                String fragmentName = new URI(actionLibPath.toString()).getFragment();
                                String fileName = fragmentName == null ? actionLibPath.getName() : fragmentName;
                                if (confSet.contains(fileName)) {
                                    Configuration jobXmlConf = shareLibService.getShareLibConf(actionShareLibName,
                                            actionLibPath);
                                    if (jobXmlConf != null) {
                                        checkForDisallowedProps(jobXmlConf, actionLibPath.getName());
                                        XConfiguration.injectDefaults(jobXmlConf, conf);
                                        LOG.trace("Adding properties of " + actionLibPath + " to job conf");
                                    }
                                }
                                else {
                                    // Filtering out duplicate jars or files
                                    sharelibList.add(new Path(actionLibPath.toUri()) {
                                        @Override
                                        public int hashCode() {
                                            return getName().hashCode();
                                        }
                                        @Override
                                        public String getName() {
                                            try {
                                                return (new URI(toString())).getFragment() == null ? new Path(toUri()).getName()
                                                        : (new URI(toString())).getFragment();
                                            }
                                            catch (URISyntaxException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                        @Override
                                        public boolean equals(Object input) {
                                            if (input == null) {
                                                return false;
                                            }
                                            if (input == this) {
                                                return true;
                                            }
                                            if (!(input instanceof Path)) {
                                                return false;
                                            }
                                            return getName().equals(((Path) input).getName());
                                        }
                                    });
                                }
                            }
                        }
                    }
                }
                addLibPathsToCache(conf, sharelibList);
            }
            catch (URISyntaxException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "Error configuring sharelib",
                        ex.getMessage());
            }
            catch (IOException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "Failed to add libpaths to cache",
                        ex.getMessage());
            }
        }
    }

    protected void addSystemShareLibForAction(Configuration conf) throws ActionExecutorException {
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        // ShareLibService is null for test cases
        if (shareLibService != null) {
            try {
                List<Path> listOfPaths = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
                if (listOfPaths.isEmpty()) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "EJ001",
                            "Could not locate Oozie sharelib");
                }
                addLibPathsToClassPath(conf, listOfPaths);
                addLibPathsToClassPath(conf, shareLibService.getSystemLibJars(getType()));
            }
            catch (IOException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                        "Failed to add action specific sharelib", ex.getMessage());
            }
        }
    }

    private void addLibPathsToClassPath(Configuration conf, List<Path> listOfPaths)
            throws IOException, ActionExecutorException {
        addLibPathsToClassPathOrCache(conf, listOfPaths, false);
    }

    private void addLibPathsToCache(Configuration conf, Set<Path> sharelibList)
            throws IOException, ActionExecutorException {
        addLibPathsToClassPathOrCache(conf, sharelibList, true);
    }


    private void addLibPathsToClassPathOrCache(Configuration conf, Collection<Path> sharelibList, boolean isAddToCache)
            throws IOException, ActionExecutorException {

        for (Path libPath : sharelibList) {
            if (FSUtils.isLocalFile(libPath.toString())) {
                conf = ClasspathUtils.addToClasspathFromLocalShareLib(conf, libPath);
            }
            else {
                if (isAddToCache) {
                    addToCache(conf, libPath, libPath.toUri().getPath(), false);
                }
                else {
                    FileSystem fs = libPath.getFileSystem(conf);
                    JobUtils.addFileToClassPath(libPath, conf, fs);
                    DistributedCache.createSymlink(conf);
                }
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
                        FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user,
                                appPath.toUri(), conf);
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
                        "Failed to add action specific lib", ex.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void setLibFilesArchives(Context context, Element actionXml, Path appPath, Configuration conf)
            throws ActionExecutorException {
        Configuration proto = context.getProtoActionConf();

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
                    addToCache(conf, appPath, path, false);
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

    @VisibleForTesting
    protected static String getTrimmedEncodedPath(String path) {
        return path.trim().replace(" ", "%20");
    }

    // Adds action specific share libs and common share libs
    private void addAllShareLibs(Path appPath, Configuration conf, Context context, Element actionXml)
            throws ActionExecutorException {
        // Add action specific share libs
        addActionShareLib(appPath, conf, context, actionXml);
        // Add common sharelibs for Oozie and launcher jars
        addSystemShareLibForAction(conf);
    }

    private void addActionShareLib(Path appPath, Configuration conf, Context context, Element actionXml)
            throws ActionExecutorException {
        XConfiguration wfJobConf = null;
        try {
            wfJobConf = getWorkflowConf(context);
        }
        catch (IOException ioe) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "Failed to add action specific sharelib",
                    ioe.getMessage());
        }
        // Action sharelibs are only added if user has specified to use system libpath
        if (conf.get(OozieClient.USE_SYSTEM_LIBPATH) == null) {
            if (wfJobConf.getBoolean(OozieClient.USE_SYSTEM_LIBPATH,
                    ConfigurationService.getBoolean(OozieClient.USE_SYSTEM_LIBPATH))) {
                // add action specific sharelibs
                addShareLib(conf, getShareLibNames(context, actionXml, conf));
            }
        }
        else {
            if (conf.getBoolean(OozieClient.USE_SYSTEM_LIBPATH, false)) {
                // add action specific sharelibs
                addShareLib(conf, getShareLibNames(context, actionXml, conf));
            }
        }
    }


    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherAM.CONF_OOZIE_ACTION_MAIN_CLASS, JavaMain.class.getName());
    }

    private void setJavaMain(Configuration actionConf, Element actionXml) {
        Namespace ns = actionXml.getNamespace();
        Element e = actionXml.getChild("main-class", ns);
        if (e != null) {
            actionConf.set(JavaMain.JAVA_MAIN_CLASS, e.getTextTrim());
        }
    }

    private static final String QUEUE_NAME = "mapred.job.queue.name";

    private static final Set<String> SPECIAL_PROPERTIES = new HashSet<String>();

    static {
        SPECIAL_PROPERTIES.add(QUEUE_NAME);
        SPECIAL_PROPERTIES.add(ACL_VIEW_JOB);
        SPECIAL_PROPERTIES.add(ACL_MODIFY_JOB);
    }

    @SuppressWarnings("unchecked")
    Configuration createLauncherConf(FileSystem actionFs, Context context, WorkflowAction action, Element actionXml,
            Configuration actionConf) throws ActionExecutorException {
        try {

            // app path could be a file
            Path appPathRoot = new Path(context.getWorkflow().getAppPath());
            if (actionFs.isFile(appPathRoot)) {
                appPathRoot = appPathRoot.getParent();
            }

            // launcher job configuration
            Configuration launcherJobConf = createBaseHadoopConf(context, actionXml);
            // cancel delegation token on a launcher job which stays alive till child job(s) finishes
            // otherwise (in mapred action), doesn't cancel not to disturb running child job
            launcherJobConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", true);
            setupLauncherConf(launcherJobConf, actionXml, appPathRoot, context);

            // Properties for when a launcher job's AM gets restarted
            if (ConfigurationService.getBoolean(HADOOP_YARN_KILL_CHILD_JOBS_ON_AMRESTART)) {
                // launcher time filter is required to prune the search of launcher tag.
                // Setting coordinator action nominal time as launcher time as it child job cannot launch before nominal
                // time. Workflow created time is good enough when workflow is running independently or workflow is
                // rerunning from failed node.
                long launcherTime = System.currentTimeMillis();
                String coordActionNominalTime = context.getProtoActionConf().get(
                        CoordActionStartXCommand.OOZIE_COORD_ACTION_NOMINAL_TIME);
                if (coordActionNominalTime != null) {
                    launcherTime = Long.parseLong(coordActionNominalTime);
                }
                else if (context.getWorkflow().getCreatedTime() != null) {
                    launcherTime = context.getWorkflow().getCreatedTime().getTime();
                }
                String actionYarnTag = getActionYarnTag(getWorkflowConf(context), context.getWorkflow(), action);
                LauncherHelper.setupYarnRestartHandling(launcherJobConf, actionConf, actionYarnTag, launcherTime);
            }
            else {
                LOG.info(MessageFormat.format("{0} is set to false, not setting YARN restart properties",
                        HADOOP_YARN_KILL_CHILD_JOBS_ON_AMRESTART));
            }

            String actionShareLibProperty = actionConf.get(ACTION_SHARELIB_FOR + getType());
            if (actionShareLibProperty != null) {
                launcherJobConf.set(ACTION_SHARELIB_FOR + getType(), actionShareLibProperty);
            }
            setLibFilesArchives(context, actionXml, appPathRoot, launcherJobConf);

            // Inject Oozie job information if enabled.
            injectJobInfo(launcherJobConf, actionConf, context, action);

            injectLauncherCallback(context, launcherJobConf);

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
            checkAndDeduplicate(actionConf);
            LauncherHelper.setupLauncherInfo(launcherJobConf, jobId, actionId, actionDir, recoveryId, actionConf,
                    prepareXML);

            // Set the launcher Main Class
            LauncherHelper.setupMainClass(launcherJobConf, getLauncherMain(launcherJobConf, actionXml));
            LauncherHelper.setupLauncherURIHandlerConf(launcherJobConf);
            LauncherHelper.setupMaxOutputData(launcherJobConf, getMaxOutputData(actionConf));
            LauncherHelper.setupMaxExternalStatsSize(launcherJobConf, maxExternalStatsSize);
            LauncherHelper.setupMaxFSGlob(launcherJobConf, maxFSGlobMax);

            List<Element> list = actionXml.getChildren("arg", ns);
            String[] args = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                args[i] = list.get(i).getTextTrim();
            }
            LauncherHelper.setupMainArguments(launcherJobConf, args);
            // backward compatibility flag - see OOZIE-2872
            boolean nullArgsAllowed = ConfigurationService.getBoolean(LauncherAMUtils.CONF_OOZIE_NULL_ARGS_ALLOWED);
            launcherJobConf.setBoolean(LauncherAMUtils.CONF_OOZIE_NULL_ARGS_ALLOWED, nullArgsAllowed);

            // Make mapred.child.java.opts and mapreduce.map.java.opts equal, but give values from the latter priority; also append
            // <java-opt> and <java-opts> and give those highest priority
            StringBuilder opts = new StringBuilder(launcherJobConf.get(HADOOP_CHILD_JAVA_OPTS, ""));
            if (launcherJobConf.get(HADOOP_MAP_JAVA_OPTS) != null) {
                opts.append(" ").append(launcherJobConf.get(HADOOP_MAP_JAVA_OPTS));
            }

            List<Element> javaopts = actionXml.getChildren("java-opt", ns);

            // Either one or more <java-opt> element or one <java-opts> can be present since oozie-workflow-0.4
            if (!javaopts.isEmpty()) {
                for (Element opt : javaopts) {
                    opts.append(" ").append(opt.getTextTrim());
                }
            }
            else {
                Element opt = actionXml.getChild("java-opts", ns);
                if (opt != null) {
                    opts.append(" ").append(opt.getTextTrim());
                }
            }
            launcherJobConf.set(HADOOP_CHILD_JAVA_OPTS, opts.toString().trim());
            launcherJobConf.set(HADOOP_MAP_JAVA_OPTS, opts.toString().trim());

            injectLauncherTimelineServiceEnabled(launcherJobConf, actionConf);

            // properties from action that are needed by the launcher (e.g. QUEUE NAME, ACLs)
            // maybe we should add queue to the WF schema, below job-tracker
            actionConfToLauncherConf(actionConf, launcherJobConf);

            checkAndDeduplicate(launcherJobConf);
            return launcherJobConf;
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private void checkAndDeduplicate(final Configuration conf) {
        if(ConfigurationService.getBoolean(OOZIE_ACTION_DEPENDENCY_DEDUPLICATE, false)) {
            dependencyDeduplicator.deduplicate(conf, MRJobConfig.CACHE_FILES);
            dependencyDeduplicator.deduplicate(conf, MRJobConfig.CACHE_ARCHIVES);
        }
    }

    @VisibleForTesting
    protected static int getMaxOutputData(Configuration actionConf) {
        String userMaxActionOutputLen = actionConf.get("oozie.action.max.output.data");
        if (userMaxActionOutputLen != null) {
            Integer i = Ints.tryParse(userMaxActionOutputLen);
            return i != null ? i : maxActionOutputLen;
        }
        return maxActionOutputLen;
    }

    protected void injectCallback(Context context, Configuration conf) {
        String callback = context.getCallbackUrl(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_JOBSTATUS_TOKEN);
        conf.set(LauncherAMCallbackNotifier.OOZIE_LAUNCHER_CALLBACK_URL, callback);
    }

    void injectActionCallback(Context context, Configuration actionConf) {
        // action callback needs to be injected only for mapreduce actions.
    }

    void injectLauncherCallback(Context context, Configuration launcherConf) {
        injectCallback(context, launcherConf);
    }

    private void actionConfToLauncherConf(Configuration actionConf, Configuration launcherConf) {
        for (String name : SPECIAL_PROPERTIES) {
            if (actionConf.get(name) != null && launcherConf.get("oozie.launcher." + name) == null) {
                launcherConf.set(name, actionConf.get(name));
            }
        }
    }

    public void submitLauncher(final FileSystem actionFs, final Context context, final WorkflowAction action)
            throws ActionExecutorException {
        YarnClient yarnClient = null;
        try {
            Path appPathRoot = new Path(context.getWorkflow().getAppPath());

            // app path could be a file
            if (actionFs.isFile(appPathRoot)) {
                appPathRoot = appPathRoot.getParent();
            }

            Element actionXml = XmlUtils.parseXml(action.getConf());
            LOG.debug("ActionXML: {0}", action.getConf());

            // action job configuration
            Configuration actionConf = loadHadoopDefaultResources(context, actionXml);
            setupActionConf(actionConf, context, actionXml, appPathRoot);
            addAppNameContext(context, action);
            LOG.debug("Setting LibFilesArchives ");
            setLibFilesArchives(context, actionXml, appPathRoot, actionConf);

            String jobName = actionConf.get(HADOOP_JOB_NAME);
            if (jobName == null || jobName.isEmpty()) {
                jobName = getYarnApplicationName(context, action, "oozie:action");
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

            Credentials credentials = new Credentials();
            Configuration launcherConf = createLauncherConf(actionFs, context, action, actionXml, actionConf);
            yarnClient = createYarnClient(context, launcherConf);
            Map<String, CredentialsProperties> credentialsProperties = setCredentialPropertyToActionConf(context,
                    action, actionConf);
            if (UserGroupInformation.isSecurityEnabled()) {
                addHadoopCredentialPropertiesToActionConf(credentialsProperties);
            }
            // Adding if action need to set more credential tokens
            Configuration credentialsConf = new Configuration(false);
            XConfiguration.copy(actionConf, credentialsConf);
            setCredentialTokens(credentials, credentialsConf, context, action, credentialsProperties);

            // copy back new entries from credentialsConf
            for (Entry<String, String> entry : credentialsConf) {
                if (actionConf.get(entry.getKey()) == null) {
                    actionConf.set(entry.getKey(), entry.getValue());
                }
            }
            String consoleUrl;
            String launcherId = LauncherHelper.getRecoveryId(launcherConf, context.getActionDir(), context
                    .getRecoveryId());

            removeHBaseSettingFromOozieDefaultResource(launcherConf);
            removeHBaseSettingFromOozieDefaultResource(actionConf);


            boolean alreadyRunning = launcherId != null;

            // if user-retry is on, always submit new launcher
            boolean isUserRetry = ((WorkflowActionBean)action).isUserRetry();
            LOG.debug("Creating yarnClient for action {0}", action.getId());

            if (alreadyRunning && !isUserRetry) {
                try {
                    ApplicationId appId = ConverterUtils.toApplicationId(launcherId);
                    ApplicationReport report = yarnClient.getApplicationReport(appId);
                    consoleUrl = report.getTrackingUrl();
                } catch (RemoteException e) {
                    // caught when the application id does not exist
                    LOG.error("Got RemoteException from YARN", e);
                    String jobTracker = launcherConf.get(HADOOP_YARN_RM);
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                            "unknown job [{0}@{1}], cannot recover", launcherId, jobTracker);
                }
            }
            else {
                YarnClientApplication newApp = yarnClient.createApplication();
                ApplicationId appId = newApp.getNewApplicationResponse().getApplicationId();
                ApplicationSubmissionContext appContext =
                        createAppSubmissionContext(appId, launcherConf, context, actionConf, action, credentials, actionXml);
                yarnClient.submitApplication(appContext);

                launcherId = appId.toString();
                LOG.debug("After submission get the launcherId [{0}]", launcherId);
                ApplicationReport appReport = yarnClient.getApplicationReport(appId);
                consoleUrl = appReport.getTrackingUrl();
            }

            String jobTracker = launcherConf.get(HADOOP_YARN_RM);
            context.setStartData(launcherId, jobTracker, consoleUrl);
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
        finally {
            if (yarnClient != null) {
                Closeables.closeQuietly(yarnClient);
            }
        }
    }

    private String getYarnApplicationName(final Context context, final WorkflowAction action, final String prefix) {
        return XLog.format("{0}:T={1}:W={2}:A={3}:ID={4}",
                prefix,
                getType(),
                context.getWorkflow().getAppName(),
                action.getName(),
                context.getWorkflow().getId());
    }

    private void removeHBaseSettingFromOozieDefaultResource(final Configuration jobConf) {
        final String[] propertySources = jobConf.getPropertySources(HbaseCredentials.HBASE_USE_DYNAMIC_JARS);
        if (propertySources != null && propertySources.length > 0 &&
                propertySources[0].contains(HbaseCredentials.OOZIE_HBASE_CLIENT_SITE_XML)) {
            jobConf.unset(HbaseCredentials.HBASE_USE_DYNAMIC_JARS);
            LOG.debug(String.format("Unset [%s] inserted from default Oozie resource XML [%s]",
                    HbaseCredentials.HBASE_USE_DYNAMIC_JARS, HbaseCredentials.OOZIE_HBASE_CLIENT_SITE_XML));
        }
    }

    private void addAppNameContext(final Context context, final WorkflowAction action) {
        final String oozieActionName = getYarnApplicationName(context, action, "oozie:launcher");
        context.setVar(OOZIE_ACTION_NAME, oozieActionName);
    }

    protected String getAppName(Context context) {
        return context.getVar(OOZIE_ACTION_NAME);
    }

    private void addHadoopCredentialPropertiesToActionConf(Map<String, CredentialsProperties> credentialsProperties) {
        LOG.info("Adding default credentials for action: hdfs, yarn and jhs");
        addHadoopCredentialProperties(credentialsProperties, CredentialsProviderFactory.HDFS);
        addHadoopCredentialProperties(credentialsProperties, CredentialsProviderFactory.YARN);
        addHadoopCredentialProperties(credentialsProperties, CredentialsProviderFactory.JHS);
    }

    private void addHadoopCredentialProperties(Map<String, CredentialsProperties> credentialsProperties, String type) {
        credentialsProperties.put(type, new CredentialsProperties(type, type));
    }

    private ApplicationSubmissionContext createAppSubmissionContext(final ApplicationId appId,
                                                                    final Configuration launcherJobConf,
                                                                    final Context actionContext,
                                                                    final Configuration actionConf,
                                                                    final WorkflowAction action,
                                                                    final Credentials credentials,
                                                                    final Element actionXml)
            throws IOException, HadoopAccessorException, URISyntaxException {

        ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

        setResources(launcherJobConf, appContext);
        setPriority(launcherJobConf, appContext);
        setQueue(launcherJobConf, appContext);
        appContext.setApplicationId(appId);
        setApplicationName(actionContext, action, appContext);
        appContext.setApplicationType("Oozie Launcher");
        setMaxAttempts(launcherJobConf, appContext);

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        YarnACLHandler yarnACL = new YarnACLHandler(launcherJobConf);
        yarnACL.setACLs(amContainer);

        final String user = actionContext.getWorkflow().getUser();
        // Set the resources to localize
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        ClientDistributedCacheManager.determineTimestampsAndCacheVisibilities(launcherJobConf);
        MRApps.setupDistributedCache(launcherJobConf, localResources);
        // Add the Launcher and Action configs as Resources
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        launcherJobConf.set(LauncherAM.OOZIE_SUBMITTER_USER, user);
        LocalResource launcherJobConfLR = has.createLocalResourceForConfigurationFile(LauncherAM.LAUNCHER_JOB_CONF_XML, user,
                launcherJobConf, actionContext.getAppFileSystem().getUri(), actionContext.getActionDir());
        localResources.put(LauncherAM.LAUNCHER_JOB_CONF_XML, launcherJobConfLR);
        LocalResource actionConfLR = has.createLocalResourceForConfigurationFile(LauncherAM.ACTION_CONF_XML, user, actionConf,
                actionContext.getAppFileSystem().getUri(), actionContext.getActionDir());
        localResources.put(LauncherAM.ACTION_CONF_XML, actionConfLR);
        amContainer.setLocalResources(localResources);

        setEnvironmentVariables(launcherJobConf, amContainer);

        List<String> vargs = createCommand(launcherJobConf, actionContext);
        setJavaOpts(launcherJobConf, actionXml, vargs);
        vargs.add(LauncherAM.class.getCanonicalName());
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDOUT);
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDERR);
        StringBuilder mergedCommand = new StringBuilder();
        for (CharSequence str : vargs) {
            mergedCommand.append(str).append(" ");
        }

        List<String> vargsFinal = ImmutableList.of(mergedCommand.toString());
        LOG.debug("Command to launch container for ApplicationMaster is: {0}", mergedCommand);
        amContainer.setCommands(vargsFinal);
        appContext.setAMContainerSpec(amContainer);

        // Set tokens
        if (credentials != null) {
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            amContainer.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
        }

        appContext.setCancelTokensWhenComplete(true);

        return appContext;
    }

    private void setMaxAttempts(Configuration launcherJobConf, ApplicationSubmissionContext appContext) {
        int launcherMaxAttempts;
        final int defaultLauncherMaxAttempts = ConfigurationService.getInt(DEFAULT_LAUNCHER_MAX_ATTEMPTS);
        if (launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS) != null) {
            try {
                launcherMaxAttempts = launcherJobConf.getInt(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS,
                        defaultLauncherMaxAttempts);
            } catch (final NumberFormatException ignored) {
                launcherMaxAttempts = defaultLauncherMaxAttempts;
            }
        } else {
            LOG.warn("Invalid configuration value [{0}] defined for launcher max attempts count, using default [{1}].",
                    launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_MAX_ATTEMPTS),
                    defaultLauncherMaxAttempts);
            launcherMaxAttempts = defaultLauncherMaxAttempts;
        }

        LOG.trace("Reading from configuration max attempts count of the Launcher AM. [launcherMaxAttempts={0}]",
                launcherMaxAttempts);

        if (launcherMaxAttempts > 0) {
            LOG.trace("Setting max attempts of the Launcher AM. [launcherMaxAttempts={0}]", launcherMaxAttempts);
            appContext.setMaxAppAttempts(launcherMaxAttempts);
        }
        else {
            LOG.warn("Not setting max attempts of the Launcher AM, value is invalid. [launcherMaxAttempts={0}]",
                    launcherMaxAttempts);
        }
    }

    private List<String> createCommand(final Configuration launcherJobConf, final Context context) {
        final List<String> vargs = new ArrayList<String>(6);

        String launcherLogLevel = launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_LOG_LEVEL_PROPERTY);
        if (Strings.isNullOrEmpty(launcherLogLevel)) {
            launcherLogLevel = "INFO";
        }

        vargs.add(Apps.crossPlatformify(ApplicationConstants.Environment.JAVA_HOME.toString())
                + "/bin/java");

        vargs.add("-Dlog4j.configuration=container-log4j.properties");
        vargs.add("-Dlog4j.debug=true");
        vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
        vargs.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_SIZE + "=" + 1024 * 1024);
        vargs.add("-Dhadoop.root.logger=" + launcherLogLevel + ",CLA");
        vargs.add("-Dhadoop.root.logfile=" + TaskLog.LogName.SYSLOG);
        vargs.add("-Dsubmitter.user=" + context.getWorkflow().getUser());

        return vargs;
    }

    private void setJavaOpts(Configuration launcherJobConf, Element actionXml, List<String> vargs) {
        // Note: for backward compatibility reasons, we have to support the <java-opts> tag inside the <java> action
        // If both java/java-opt(s) and launcher/java-opts are defined, we pick java/java-opts
        // We also display a warning to let users know that they should migrate their workflow
        StringBuilder javaOpts = new StringBuilder();
        boolean oldJavaOpts = handleJavaOpts(actionXml, javaOpts);
        if (oldJavaOpts) {
            vargs.add(javaOpts.toString());
        }

        final String oozieLauncherJavaOpts = launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_JAVAOPTS_PROPERTY);
        if (oozieLauncherJavaOpts != null) {
            if (oldJavaOpts) {
                LOG.warn("<java-opts> was defined inside the <launcher> tag -- ignored");
            } else {
                vargs.add(oozieLauncherJavaOpts);
            }
        }

        checkAndSetMaxHeap(launcherJobConf, vargs);
    }

    private boolean handleJavaOpts(Element actionXml, StringBuilder javaOpts) {
        Namespace ns = actionXml.getNamespace();
        boolean oldJavaOpts = false;
        @SuppressWarnings("unchecked")
        List<Element> javaopts = actionXml.getChildren("java-opt", ns);
        for (Element opt: javaopts) {
            javaOpts.append(" ").append(opt.getTextTrim());
            oldJavaOpts = true;
        }
        Element opt = actionXml.getChild("java-opts", ns);
        if (opt != null) {
            javaOpts.append(" ").append(opt.getTextTrim());
            oldJavaOpts = true;
        }

        if (oldJavaOpts) {
            LOG.warn("Note: <java-opts> inside the action is used in the workflow. Please move <java-opts> tag under"
                    + " the <launcher> element. See the documentation for details");
        }
        return oldJavaOpts;
    }

    private void checkAndSetMaxHeap(final Configuration launcherJobConf, final List<String> vargs) {
        LOG.debug("Checking and setting max heap for the LauncherAM");

        final int launcherMemoryMb = readMemoryMb(launcherJobConf);
        final int calculatedHeapMaxMb = (int) (launcherMemoryMb * LAUNCHER_HEAP_PMEM_RATIO);

        boolean heapModifiersPresent = false;
        for (final String varg : vargs) {
            if (HEAP_MODIFIERS_PATTERN.matcher(varg).matches()) {
                heapModifiersPresent = true;
            }
        }
        if (heapModifiersPresent) {
            LOG.trace("Some heap modifier JVM options are configured by the user, leaving LauncherAM's maximum heap option");
        }
        else {
            LOG.trace("No heap modifier JVM options are configured by the user, overriding LauncherAM's maximum heap option");

            LOG.debug("Calculated maximum heap option {0} MB set for the LauncherAM", calculatedHeapMaxMb);
            vargs.add(String.format("-Xmx%sm", calculatedHeapMaxMb));
        }
    }

    private void setApplicationName(final Context context,
                                    final WorkflowAction action,
                                    final ApplicationSubmissionContext appContext) {
        final String jobName = getYarnApplicationName(context, action, "oozie:launcher");
        appContext.setApplicationName(jobName);
    }

    private void setEnvironmentVariables(Configuration launcherConf, ContainerLaunchContext amContainer) throws IOException {
        Map<String, String> env = new HashMap<>();

        final String oozieLauncherEnvProperty = launcherConf.get(LauncherAM.OOZIE_LAUNCHER_ENV_PROPERTY);
        if (oozieLauncherEnvProperty != null) {
            Map<String, String> environmentVars = extractEnvVarsFromOozieLauncherProps(oozieLauncherEnvProperty);
            env.putAll(environmentVars);
        }

        // This adds the Hadoop jars to the classpath in the Launcher JVM
        ClasspathUtils.setupClasspath(env, launcherConf);

        if (needToAddMapReduceToClassPath(launcherConf)) {
            ClasspathUtils.addMapReduceToClasspath(env, launcherConf);
        }

        addActionSpecificEnvVars(env);
        amContainer.setEnvironment(ImmutableMap.copyOf(env));
    }

    private void setQueue(Configuration launcherJobConf, ApplicationSubmissionContext appContext) {
        String launcherQueueName;
        if (launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY) != null) {
            launcherQueueName = launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_QUEUE_PROPERTY);
        } else {
            launcherQueueName = Preconditions.checkNotNull(
                    ConfigurationService.get(DEFAULT_LAUNCHER_QUEUE), "Default launcherQueueName is undefined");
        }
        appContext.setQueue(launcherQueueName);
    }

    private void setPriority(Configuration launcherJobConf, ApplicationSubmissionContext appContext) {
        int priority;
        if (launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY) != null) {
            priority = launcherJobConf.getInt(LauncherAM.OOZIE_LAUNCHER_PRIORITY_PROPERTY, -1);
        } else {
            int defaultPriority = ConfigurationService.getInt(DEFAULT_LAUNCHER_PRIORITY);
            priority = defaultPriority;
        }
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(priority);
        appContext.setPriority(pri);
    }

    private void setResources(final Configuration launcherJobConf, final ApplicationSubmissionContext appContext) {
        final Resource resource = Resource.newInstance(readMemoryMb(launcherJobConf), readVCores(launcherJobConf));
        appContext.setResource(resource);
    }

    private int readMemoryMb(final Configuration launcherJobConf) {
        final int memory;
        if (launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY) != null) {
            memory = launcherJobConf.getInt(LauncherAM.OOZIE_LAUNCHER_MEMORY_MB_PROPERTY, -1);
            Preconditions.checkArgument(memory > 0, "Launcher memory is 0 or negative");
        } else {
            final int defaultMemory = ConfigurationService.getInt(DEFAULT_LAUNCHER_MEMORY_MB, -1);
            Preconditions.checkArgument(defaultMemory > 0, "Default launcher memory is 0 or negative");
            memory = defaultMemory;
        }
        return memory;
    }

    private int readVCores(final Configuration launcherJobConf) {
        final int vcores;
        if (launcherJobConf.get(LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY) != null) {
            vcores = launcherJobConf.getInt(LauncherAM.OOZIE_LAUNCHER_VCORES_PROPERTY, -1);
            Preconditions.checkArgument(vcores > 0, "Launcher vcores is 0 or negative");
        } else {
            final int defaultVcores = ConfigurationService.getInt(DEFAULT_LAUNCHER_VCORES);
            Preconditions.checkArgument(defaultVcores > 0, "Default launcher vcores is 0 or negative");
            vcores = defaultVcores;
        }
        return vcores;
    }

    private Map<String, String>  extractEnvVarsFromOozieLauncherProps(String oozieLauncherEnvProperty) {
        Map<String, String> envMap = new LinkedHashMap<>();
        for (String envVar : StringUtils.split(oozieLauncherEnvProperty, File.pathSeparatorChar)) {
            String[] env = StringUtils.split(envVar, '=');
            Preconditions.checkArgument(env.length == 2, "Invalid launcher setting for environment variables: \"%s\". " +
                                "<env> should contain a list of ENV_VAR_NAME=VALUE separated by the '%s' character. " +
                                "Example on Unix: A=foo1:B=foo2", oozieLauncherEnvProperty, File.pathSeparator);
            envMap.put(env[0], env[1]);
        }
        return envMap;
    }

   Map<String, CredentialsProperties> setCredentialPropertyToActionConf(final Context context,
                                                                         final WorkflowAction action,
                                                                         final Configuration actionConf) throws Exception {
        final Map<String, CredentialsProperties> credPropertiesMap = new HashMap<>();
        if (context == null || action == null) {
            LOG.warn("context or action is null");
            return credPropertiesMap;
        }
        final XConfiguration wfJobConf = getWorkflowConf(context);
        final boolean skipCredentials = actionConf.getBoolean(OOZIE_CREDENTIALS_SKIP,
                wfJobConf.getBoolean(OOZIE_CREDENTIALS_SKIP, ConfigurationService.getBoolean(OOZIE_CREDENTIALS_SKIP)));
        if (skipCredentials) {
            LOG.info("Skipping credentials (" + OOZIE_CREDENTIALS_SKIP + "=true)");
        } else {
            credPropertiesMap.putAll(getActionCredentialsProperties(context, action));
            if (credPropertiesMap.isEmpty()) {
                LOG.warn("No credential properties found for action : " + action.getId() + ", cred : " + action.getCred());
                return credPropertiesMap;
            }
            for (final Entry<String, CredentialsProperties> entry : credPropertiesMap.entrySet()) {
                if (entry.getValue() != null) {
                    final CredentialsProperties prop = entry.getValue();
                    LOG.debug("Credential Properties set for action : " + action.getId());
                    for (final Entry<String, String> propEntry : prop.getProperties().entrySet()) {
                        final String key = propEntry.getKey();
                        final String value = propEntry.getValue();
                        actionConf.set(key, value);
                        LOG.debug("property : '" + key + "', value : '" + value + "'");
                    }
                }
            }
        }
        return credPropertiesMap;
    }

    protected void setCredentialTokens(Credentials credentials, Configuration jobconf, Context context, WorkflowAction action,
                                       Map<String, CredentialsProperties> credPropertiesMap) throws Exception {
        if (!isValidCredentialTokensPreconditions(context, action, credPropertiesMap)) {
            LOG.debug("Not obtaining delegation token(s) as preconditions do not hold.");
            return;
        }

        setActionTokenProperties(jobconf);
        // Make sure we're logged into Kerberos; if not, or near expiration, it will relogin
        CredentialsProviderFactory.ensureKerberosLogin();
        for (Entry<String, CredentialsProperties> entry : credPropertiesMap.entrySet()) {
            String credName = entry.getKey();
            CredentialsProperties credProps = entry.getValue();
            if (credProps != null) {
                CredentialsProvider tokenProvider = CredentialsProviderFactory.getInstance()
                        .createCredentialsProvider(credProps.getType());
                if (tokenProvider != null) {
                    tokenProvider.updateCredentials(credentials, jobconf, credProps, context);
                    LOG.debug("Retrieved Credential '" + credName + "' for action " + action.getId());
                } else {
                    LOG.debug("Credentials object is null for name= " + credName + ", type=" + credProps.getType());
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA020",
                            "Could not load credentials of type [{0}] with name [{1}]]; perhaps it was not defined"
                                    + " in oozie-site.xml?", credProps.getType(), credName);
                }
            }
        }
    }

    private boolean isValidCredentialTokensPreconditions(final Context context,
                                                         final WorkflowAction action,
                                                         final Map<String, CredentialsProperties> credPropertiesMap) {
        return context != null && action != null && credPropertiesMap != null;
    }

    /**
     * Subclasses may override this method in order to take additional actions required for obtaining credential token(s).
     *
     * @param jobconf workflow action configuration
     */
    protected void setActionTokenProperties(final Configuration jobconf) {
        // nop
    }

    protected HashMap<String, CredentialsProperties> getActionCredentialsProperties(Context context,
            WorkflowAction action) throws Exception {
        HashMap<String, CredentialsProperties> props = new HashMap<String, CredentialsProperties>();
        if (context != null && action != null) {
            String credsInAction = action.getCred();
            if (credsInAction != null) {
                LOG.debug("Get credential '" + credsInAction + "' properties for action : " + action.getId());
                String[] credNames = credsInAction.split(",");
                for (String credName : credNames) {
                    CredentialsProperties credProps = getCredProperties(context, credName);
                    props.put(credName, credProps);
                }
            }
        }
        else {
            LOG.warn("context or action is null");
        }
        return props;
    }

    @SuppressWarnings("unchecked")
    protected CredentialsProperties getCredProperties(Context context, String credName)
            throws Exception {
        CredentialsProperties credProp = null;
        String workflowXml = ((WorkflowJobBean) context.getWorkflow()).getWorkflowInstance().getApp().getDefinition();
        XConfiguration wfjobConf = getWorkflowConf(context);
        Element elementJob = XmlUtils.parseXml(workflowXml);
        Element credentials = elementJob.getChild("credentials", elementJob.getNamespace());
        if (credentials != null) {
            for (Element credential : (List<Element>) credentials.getChildren("credential", credentials.getNamespace())) {
                String name = credential.getAttributeValue("name");
                String type = credential.getAttributeValue("type");
                LOG.debug("getCredProperties: Name: " + name + ", Type: " + type);
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
                        LOG.debug("getCredProperties: Properties name :'" + propertyName + "', Value : '"
                                + propertyValue + "'");
                    }
                }
            }
            if (credProp == null && credName != null) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA021",
                        "Could not load credentials with name [{0}]].", credName);
            }
        } else {
            LOG.debug("credentials is null for the action");
        }
        return credProp;
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        LogUtils.setLogInfo(action);
        try {
            LOG.info("Starting action. Getting Action File System");
            FileSystem actionFs = context.getAppFileSystem();
            LOG.debug("Preparing action Dir through copying " + context.getActionDir());
            prepareActionDir(actionFs, context);
            LOG.debug("Action Dir is ready. Submitting the action ");
            submitLauncher(actionFs, context, action);
            LOG.debug("Action submit completed. Performing check ");
            check(context, action);
            LOG.debug("Action check is done after submission");
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.info("Action ended with external status [{0}]", action.getExternalStatus());
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
     * @return JobClient
     * @throws HadoopAccessorException
     */
    protected JobClient createJobClient(Context context, Configuration jobConf) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        return Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
    }

    /**
     * Create yarn client object
     *
     * @param context
     * @param jobConf
     * @return YarnClient
     * @throws HadoopAccessorException
     */
    protected YarnClient createYarnClient(Context context, Configuration jobConf) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        return Services.get().get(HadoopAccessorService.class).createYarnClient(user, jobConf);
    }

    /**
     * Useful for overriding in actions that do subsequent job runs
     * such as the MapReduce Action, where the launcher job is not the
     * actual job that then gets monitored.
     * @param action
     * @return external ID.
     */
    protected String getActualExternalId(WorkflowAction action) {
        return action.getExternalId();
    }

    /**
     * If returns true, it means that we have to add Hadoop MR jars to the classpath.
     * Subclasses should override this method if necessary. By default we don't add
     * MR jars to the classpath.
     *
     * <p>Configuration properties are read either from {@code launcherConf}, or, if not present, falling back to
     * {@link ConfigurationService#getBoolean(Configuration, String)}.
     *
     * @return false by default
     * @param launcherConf the launcher {@link Configuration} that is used on first lookup
     */
    private boolean needToAddMapReduceToClassPath(final Configuration launcherConf) {
        LOG.debug("Calculating whether to add MapReduce JARs to classpath. [type={0};this={1}]", getType(), this);

        final boolean defaultValue = ConfigurationService.getBooleanOrDefault(
                launcherConf,
                OOZIE_LAUNCHER_ADD_MAPREDUCE_TO_CLASSPATH_PROPERTY,
                false);
        LOG.debug("Default value for [{0}] is [{1}]", OOZIE_LAUNCHER_ADD_MAPREDUCE_TO_CLASSPATH_PROPERTY, defaultValue);

        final String configurationKey = OOZIE_LAUNCHER_ADD_MAPREDUCE_TO_CLASSPATH_PROPERTY + "." + getType();
        final boolean configuredValue = ConfigurationService.getBooleanOrDefault(launcherConf, configurationKey, defaultValue);
        LOG.debug("Configured value for [{0}] is [{1}]", configurationKey, configuredValue);

        return configuredValue;
    }

    /**
     * Adds action-specific environment variables. Default implementation is no-op.
     * Subclasses should override this method if necessary.
     * @param env action specific environment variables stored as Map of key and value.
     */
    protected void addActionSpecificEnvVars(Map<String, String> env) {
        // nop
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        boolean fallback = false;
        LOG = XLog.resetPrefix(LOG);
        LogUtils.setLogInfo(action);
        YarnClient yarnClient = null;
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            Configuration jobConf = createBaseHadoopConf(context, actionXml);
            FileSystem actionFs = context.getAppFileSystem();
            yarnClient = createYarnClient(context, jobConf);
            FinalApplicationStatus appStatus = null;
            try {
                final String effectiveApplicationId = findYarnApplicationId(context, action);
                final ApplicationId applicationId = ConverterUtils.toApplicationId(effectiveApplicationId);
                final ApplicationReport appReport = yarnClient.getApplicationReport(applicationId);
                final YarnApplicationState appState = appReport.getYarnApplicationState();
                if (appState == YarnApplicationState.FAILED || appState == YarnApplicationState.FINISHED
                        || appState == YarnApplicationState.KILLED) {
                    appStatus = appReport.getFinalApplicationStatus();
                }
            } catch (final ActionExecutorException aae) {
                LOG.warn("Foreseen Exception occurred while action execution; rethrowing ", aae);
                throw aae;
            } catch (final Exception ye) {
                LOG.warn("Exception occurred while checking Launcher AM status; will try checking action data file instead ", ye);
                // Fallback to action data file if we can't find the Launcher AM (maybe it got purged)
                fallback = true;
            }
            if (appStatus != null || fallback) {
                Path actionDir = context.getActionDir();
                // load sequence file into object
                Map<String, String> actionData = LauncherHelper.getActionData(actionFs, actionDir, jobConf);
                if (fallback) {
                    String finalStatus = actionData.get(LauncherAM.ACTION_DATA_FINAL_STATUS);
                    if (finalStatus != null) {
                        appStatus = FinalApplicationStatus.valueOf(finalStatus);
                    } else {
                        context.setExecutionData(FAILED, null);
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA017",
                                "Unknown hadoop job [{0}] associated with action [{1}] and couldn't determine status from" +
                                        " action data.  Failing this action!", action.getExternalId(), action.getId());
                    }
                }

                String externalID = actionData.get(LauncherAM.ACTION_DATA_NEW_ID);  // MapReduce was launched
                if (externalID != null) {
                    context.setExternalChildIDs(externalID);
                    LOG.info(XLog.STD, "Hadoop Job was launched : [{0}]", externalID);
                }

               // Multiple child IDs - Pig or Hive action
                String externalIDs = actionData.get(LauncherAM.ACTION_DATA_EXTERNAL_CHILD_IDS);
                if (externalIDs != null) {
                    context.setExternalChildIDs(externalIDs);
                    LOG.info(XLog.STD, "External Child IDs  : [{0}]", externalIDs);

                }

                LOG.info(XLog.STD, "action completed, external ID [{0}]", action.getExternalId());
                context.setExecutionData(appStatus.toString(), null);
                if (appStatus == FinalApplicationStatus.SUCCEEDED) {
                    if (getCaptureOutput(action) && LauncherHelper.hasOutputData(actionData)) {
                        context.setExecutionData(SUCCEEDED, PropertiesUtils.stringToProperties(actionData
                                .get(LauncherAM.ACTION_DATA_OUTPUT_PROPS)));
                        LOG.info(XLog.STD, "action produced output");
                    }
                    else {
                        context.setExecutionData(SUCCEEDED, null);
                    }
                    if (LauncherHelper.hasStatsData(actionData)) {
                        context.setExecutionStats(actionData.get(LauncherAM.ACTION_DATA_STATS));
                        LOG.info(XLog.STD, "action produced stats");
                    }
                    getActionData(actionFs, action, context);
                }
                else {
                    String errorReason;
                    if (actionData.containsKey(LauncherAM.ACTION_DATA_ERROR_PROPS)) {
                        Properties props = PropertiesUtils.stringToProperties(actionData
                                .get(LauncherAM.ACTION_DATA_ERROR_PROPS));
                        String errorCode = props.getProperty("error.code");
                        if ("0".equals(errorCode)) {
                            errorCode = "JA018";
                        }
                        if ("-1".equals(errorCode)) {
                            errorCode = "JA019";
                        }
                        errorReason = props.getProperty("error.reason");
                        LOG.warn("Launcher ERROR, reason: {0}", errorReason);
                        String exMsg = props.getProperty("exception.message");
                        String errorInfo = (exMsg != null) ? exMsg : errorReason;
                        context.setErrorInfo(errorCode, errorInfo);
                        String exStackTrace = props.getProperty("exception.stacktrace");
                        if (exMsg != null) {
                            LOG.warn("Launcher exception: {0}{E}{1}", exMsg, exStackTrace);
                        }
                    }
                    else {
                        errorReason = XLog.format("Launcher AM died, check Hadoop LOG for job [{0}:{1}]", action
                                .getTrackerUri(), action.getExternalId());
                        LOG.warn(errorReason);
                    }
                    context.setExecutionData(FAILED_KILLED, null);
                }
            }
            else {
                context.setExternalStatus(YarnApplicationState.RUNNING.toString());
                LOG.info(XLog.STD, "checking action, hadoop job ID [{0}] status [RUNNING]",
                        action.getExternalId());
            }
        }
        catch (Exception ex) {
            LOG.warn("Exception in check(). Message[{0}]", ex.getMessage(), ex);
            throw convertException(ex);
        }
        finally {
            if (yarnClient != null) {
                IOUtils.closeQuietly(yarnClient);
            }
        }
    }

    /**
     * For every {@link JavaActionExecutor} that is not {@link MapReduceActionExecutor}, the effective YARN application ID of the
     * action is the one where {@link LauncherAM} is run, hence this default implementation.
     * @param context the execution context
     * @param action the workflow action
     * @return a {@code String} that depicts the application ID of the launcher ApplicationMaster of this action
     * @throws ActionExecutorException
     */
    protected String findYarnApplicationId(final Context context, final WorkflowAction action)
            throws ActionExecutorException {
        return action.getExternalId();
    }

    /**
     * Get the output data of an action. Subclasses should override this method
     * to get action specific output data.
     * @param actionFs the FileSystem object
     * @param action the Workflow action
     * @param context executor context
     * @throws org.apache.oozie.service.HadoopAccessorException
     * @throws org.jdom.JDOMException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     *
     */
    protected void getActionData(FileSystem actionFs, WorkflowAction action, Context context)
            throws HadoopAccessorException, JDOMException, IOException, URISyntaxException {
    }

    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        Element eConf = XmlUtils.parseXml(action.getConf());
        Namespace ns = eConf.getNamespace();
        Element captureOutput = eConf.getChild("capture-output", ns);
        return captureOutput != null;
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        YarnClient yarnClient = null;
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            final Configuration jobConf = createBaseHadoopConf(context, actionXml);
            String launcherTag = getActionYarnTag(context, action);
            jobConf.set(LauncherMain.CHILD_MAPREDUCE_JOB_TAGS, LauncherHelper.getTag(launcherTag));
            yarnClient = createYarnClient(context, jobConf);

            String appExternalId = action.getExternalId();
            killExternalApp(action, yarnClient, appExternalId);
            killExternalChildApp(action, yarnClient, appExternalId);
            killExternalChildAppByTags(action, yarnClient, jobConf, appExternalId);

            context.setExternalStatus(KILLED);
            context.setExecutionData(KILLED, null);
        } catch (Exception ex) {
            LOG.error("Error when killing YARN application", ex);
            throw convertException(ex);
        } finally {
            try {
                FileSystem actionFs = context.getAppFileSystem();
                cleanUpActionDir(actionFs, context);
                Closeables.closeQuietly(yarnClient);
            } catch (Exception ex) {
                LOG.error("Error when cleaning up action dir", ex);
                throw convertException(ex);
            }
        }
    }

    private boolean finalAppStatusUndefined(ApplicationReport appReport) {
        FinalApplicationStatus status = appReport.getFinalApplicationStatus();
        return !FinalApplicationStatus.SUCCEEDED.equals(status) &&
                !FinalApplicationStatus.FAILED.equals(status) &&
                !FinalApplicationStatus.KILLED.equals(status);
    }

    void killExternalApp(WorkflowAction action, YarnClient yarnClient, String appExternalId)
            throws YarnException, IOException {
        if (appExternalId != null) {
            ApplicationId appId = ConverterUtils.toApplicationId(appExternalId);
            if (finalAppStatusUndefined(yarnClient.getApplicationReport(appId))) {
                try {
                    LOG.info("Killing action {0}''s external application {1}", action.getId(), appExternalId);
                    yarnClient.killApplication(appId);
                } catch (Exception e) {
                    LOG.warn("Could not kill {0}", appExternalId, e);
                }
            }
        }
    }

    void killExternalChildApp(WorkflowAction action, YarnClient yarnClient, String appExternalId)
            throws YarnException, IOException {
        String externalChildIDs = action.getExternalChildIDs();
        if (externalChildIDs != null) {
            for (String childId : externalChildIDs.split(",")) {
                ApplicationId appChildId = ConverterUtils.toApplicationId(childId.trim());
                if (finalAppStatusUndefined(yarnClient.getApplicationReport(appChildId))) {
                    try {
                        LOG.info("Killing action {0}''s external child application {1}", action.getId(), childId);
                        yarnClient.killApplication(appChildId);
                    } catch (Exception e) {
                        LOG.warn("Could not kill external child of {0}, {1}",
                                appExternalId, childId, e);
                    }
                }
            }
        }
    }

    void killExternalChildAppByTags(WorkflowAction action, YarnClient yarnClient, Configuration jobConf, String appExternalId)
            throws YarnException, IOException {
        for (ApplicationId id : LauncherMain.getChildYarnJobs(jobConf, ApplicationsRequestScope.ALL,
                action.getStartTime().getTime())) {
            if (finalAppStatusUndefined(yarnClient.getApplicationReport(id))) {
                try {
                    LOG.info("Killing action {0}''s external child application {1} based on tags",
                            action.getId(), id.toString());
                    yarnClient.killApplication(id);
                } catch (Exception e) {
                    LOG.warn("Could not kill child of {0}, {1}", appExternalId, id, e);
                }
            }
        }
    }

    protected String getActionYarnTag(Context context, WorkflowAction action) {
        return LauncherHelper.getActionYarnTag(context.getProtoActionConf(), context.getWorkflow().getParentId(), action);
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
     * Return the sharelib names for the action.
     * See {@link SharelibResolver} for details.
     *
     * @param context executor context.
     * @param actionXml the action xml.
     * @param conf action configuration.
     * @return the action sharelib names.
     */
    protected String[] getShareLibNames(final Context context, final Element actionXml, Configuration conf) {
        final String sharelibFromConfigurationProperty = ACTION_SHARELIB_FOR + getType();
        try {
            final String defaultShareLibName = getDefaultShareLibName(actionXml);
            final Configuration oozieServerConfiguration = Services.get().get(ConfigurationService.class).getConf();
            final XConfiguration workflowConfiguration = getWorkflowConf(context);
            return new SharelibResolver(sharelibFromConfigurationProperty, conf, workflowConfiguration,
                    oozieServerConfiguration, defaultShareLibName).resolve();
        } catch (IOException ex) {
            throw new RuntimeException("Can't get workflow configuration: " + ex.toString(), ex);
        }
    }


    /**
     * Returns the default sharelib name for the action if any.
     *
     * @param actionXml the action XML fragment.
     * @return the sharelib name for the action, <code>NULL</code> if none.
     */
    protected String getDefaultShareLibName(Element actionXml) {
        return null;
    }

    public String[] getShareLibFilesForActionConf() {
        return null;
    }

    /**
     * Sets some data for the action on completion
     *
     * @param context executor context
     * @param actionFs the FileSystem object
     * @throws java.io.IOException
     * @throws org.apache.oozie.service.HadoopAccessorException
     * @throws java.net.URISyntaxException
     */
    protected void setActionCompletionData(Context context, FileSystem actionFs) throws IOException,
            HadoopAccessorException, URISyntaxException {
    }

    private void injectJobInfo(Configuration launcherJobConf, Configuration actionConf, Context context, WorkflowAction action) {
        if (OozieJobInfo.isJobInfoEnabled()) {
            try {
                OozieJobInfo jobInfo = new OozieJobInfo(actionConf, context, action);
                String jobInfoStr = jobInfo.getJobInfo();
                launcherJobConf.set(OozieJobInfo.JOB_INFO_KEY, jobInfoStr + "launcher=true");
                actionConf.set(OozieJobInfo.JOB_INFO_KEY, jobInfoStr + "launcher=false");
            }
            catch (Exception e) {
                // Just job info, should not impact the execution.
                LOG.error("Error while populating job info", e);
            }
        }
    }

    @Override
    public boolean requiresNameNodeJobTracker() {
        return true;
    }

    @Override
    public boolean supportsConfigurationJobXML() {
        return true;
    }

    private XConfiguration getWorkflowConf(Context context) throws IOException {
        if (workflowConf == null) {
            workflowConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
        }
        return workflowConf;

    }

    private String getActionTypeLauncherPrefix() {
        return "oozie.action." + getType() + ".launcher.";
    }
}
