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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.hadoop.io.Text;
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
import org.apache.oozie.command.wf.ActionStartXCommand;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.JobUtils;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.PropertiesUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.ELEvaluationException;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.oozie.hadoop.utils.HadoopShims;


public class JavaActionExecutor extends ActionExecutor {

    protected static final String HADOOP_USER = "user.name";
    public static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    public static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    public static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    public static final String HADOOP_NAME_NODE = "fs.default.name";
    private static final String HADOOP_JOB_NAME = "mapred.job.name";
    public static final String OOZIE_COMMON_LIBDIR = "oozie";
    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();
    public final static String MAX_EXTERNAL_STATS_SIZE = "oozie.external.stats.max.size";
    public static final String ACL_VIEW_JOB = "mapreduce.job.acl-view-job";
    public static final String ACL_MODIFY_JOB = "mapreduce.job.acl-modify-job";
    public static final String HADOOP_YARN_TIMELINE_SERVICE_ENABLED = "yarn.timeline-service.enabled";
    public static final String HADOOP_YARN_UBER_MODE = "mapreduce.job.ubertask.enable";
    public static final String HADOOP_MAP_MEMORY_MB = "mapreduce.map.memory.mb";
    public static final String HADOOP_CHILD_JAVA_OPTS = "mapred.child.java.opts";
    public static final String HADOOP_MAP_JAVA_OPTS = "mapreduce.map.java.opts";
    public static final String HADOOP_REDUCE_JAVA_OPTS = "mapreduce.reduce.java.opts";
    public static final String HADOOP_CHILD_JAVA_ENV = "mapred.child.env";
    public static final String HADOOP_MAP_JAVA_ENV = "mapreduce.map.env";
    public static final String YARN_AM_RESOURCE_MB = "yarn.app.mapreduce.am.resource.mb";
    public static final String YARN_AM_COMMAND_OPTS = "yarn.app.mapreduce.am.command-opts";
    public static final String YARN_AM_ENV = "yarn.app.mapreduce.am.env";
    private static final String JAVA_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.JavaMain";
    public static final int YARN_MEMORY_MB_MIN = 512;
    private static int maxActionOutputLen;
    private static int maxExternalStatsSize;
    private static int maxFSGlobMax;
    private static final String SUCCEEDED = "SUCCEEDED";
    private static final String KILLED = "KILLED";
    private static final String FAILED = "FAILED";
    private static final String FAILED_KILLED = "FAILED/KILLED";
    protected XLog LOG = XLog.getLog(getClass());
    private static final Pattern heapPattern = Pattern.compile("-Xmx(([0-9]+)[mMgG])");
    private static final String JAVA_TMP_DIR_SETTINGS = "-Djava.io.tmpdir=";
    public static final String CONF_HADOOP_YARN_UBER_MODE = "oozie.action.launcher." + HADOOP_YARN_UBER_MODE;
    public static final String HADOOP_JOB_CLASSLOADER = "mapreduce.job.classloader";
    public static final String HADOOP_USER_CLASSPATH_FIRST = "mapreduce.user.classpath.first";

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER);
        DISALLOWED_PROPERTIES.add(HADOOP_NAME_NODE);
        DISALLOWED_PROPERTIES.add(HADOOP_JOB_TRACKER_2);
        DISALLOWED_PROPERTIES.add(HADOOP_YARN_RM);
    }

    public JavaActionExecutor() {
        this("java");
    }

    protected JavaActionExecutor(String type) {
        super(type);
        requiresNNJT = true;
    }

    public static List<Class> getCommonLauncherClasses() {
        List<Class> classes = new ArrayList<Class>();
        classes.add(LauncherMapper.class);
        classes.add(OozieLauncherInputFormat.class);
        classes.add(LauncherMainHadoopUtils.class);
        classes.add(HadoopShims.class);
        classes.addAll(Services.get().get(URIHandlerService.class).getClassesForLauncher());
        return classes;
    }

    public List<Class> getLauncherClasses() {
       List<Class> classes = new ArrayList<Class>();
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
        maxActionOutputLen = ConfigurationService.getInt(LauncherMapper.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA);
        //Get the limit for the maximum allowed size of action stats
        maxExternalStatsSize = ConfigurationService.getInt(JavaActionExecutor.MAX_EXTERNAL_STATS_SIZE);
        maxExternalStatsSize = (maxExternalStatsSize == -1) ? Integer.MAX_VALUE : maxExternalStatsSize;
        //Get the limit for the maximum number of globbed files/dirs for FS operation
        maxFSGlobMax = ConfigurationService.getInt(LauncherMapper.CONF_OOZIE_ACTION_FS_GLOB_MAX);

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

    public JobConf createBaseHadoopConf(Context context, Element actionXml) {
        return createBaseHadoopConf(context, actionXml, true);
    }

    protected JobConf createBaseHadoopConf(Context context, Element actionXml, boolean loadResources) {
        Namespace ns = actionXml.getNamespace();
        String jobTracker = actionXml.getChild("job-tracker", ns).getTextTrim();
        String nameNode = actionXml.getChild("name-node", ns).getTextTrim();
        JobConf conf = null;
        if (loadResources) {
            conf = Services.get().get(HadoopAccessorService.class).createJobConf(jobTracker);
        }
        else {
            conf = new JobConf(false);
        }
        conf.set(HADOOP_USER, context.getProtoActionConf().get(WorkflowAppService.HADOOP_USER));
        conf.set(HADOOP_JOB_TRACKER, jobTracker);
        conf.set(HADOOP_JOB_TRACKER_2, jobTracker);
        conf.set(HADOOP_YARN_RM, jobTracker);
        conf.set(HADOOP_NAME_NODE, nameNode);
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true");
        return conf;
    }

    protected JobConf loadHadoopDefaultResources(Context context, Element actionXml) {
        return createBaseHadoopConf(context, actionXml);
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
                injectLauncherUseUberMode(launcherConf);
                checkForDisallowedProps(launcherConf, "launcher configuration");
                XConfiguration.copy(launcherConf, conf);
            } else {
                XConfiguration launcherConf = new XConfiguration();
                injectLauncherUseUberMode(launcherConf);
                XConfiguration.copy(launcherConf, conf);
            }
            e = actionXml.getChild("config-class", actionXml.getNamespace());
            if (e != null) {
                conf.set(LauncherMapper.OOZIE_ACTION_CONFIG_CLASS, e.getTextTrim());
            }
            return conf;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    void injectLauncherUseUberMode(Configuration launcherConf) {
        // Set Uber Mode for the launcher (YARN only, ignored by MR1)
        // Priority:
        // 1. action's <configuration>
        // 2. oozie.action.#action-type#.launcher.mapreduce.job.ubertask.enable
        // 3. oozie.action.launcher.mapreduce.job.ubertask.enable
        if (launcherConf.get(HADOOP_YARN_UBER_MODE) == null) {
            if (ConfigurationService.get("oozie.action." + getType() + ".launcher." + HADOOP_YARN_UBER_MODE).length() > 0) {
                if (ConfigurationService.getBoolean("oozie.action." + getType() + ".launcher." + HADOOP_YARN_UBER_MODE)) {
                    launcherConf.setBoolean(HADOOP_YARN_UBER_MODE, true);
                }
            } else {
                if (ConfigurationService.getBoolean("oozie.action.launcher." + HADOOP_YARN_UBER_MODE)) {
                    launcherConf.setBoolean(HADOOP_YARN_UBER_MODE, true);
                }
            }
        }
    }

    void injectLauncherTimelineServiceEnabled(Configuration launcherConf, Configuration actionConf) {
        // Getting delegation token for ATS. If tez-site.xml is present in distributed cache, turn on timeline service.
        if (actionConf.get("oozie.launcher." + HADOOP_YARN_TIMELINE_SERVICE_ENABLED) == null
                && ConfigurationService.getBoolean("oozie.action.launcher." + HADOOP_YARN_TIMELINE_SERVICE_ENABLED)) {
            String cacheFiles = launcherConf.get("mapred.cache.files");
            if (cacheFiles != null && cacheFiles.contains("tez-site.xml")) {
                launcherConf.setBoolean(HADOOP_YARN_TIMELINE_SERVICE_ENABLED, true);
            }
        }
    }

    void updateConfForUberMode(Configuration launcherConf) {

        // child.env
        boolean hasConflictEnv = false;
        String launcherMapEnv = launcherConf.get(HADOOP_MAP_JAVA_ENV);
        if (launcherMapEnv == null) {
            launcherMapEnv = launcherConf.get(HADOOP_CHILD_JAVA_ENV);
        }
        String amEnv = launcherConf.get(YARN_AM_ENV);
        StringBuffer envStr = new StringBuffer();
        HashMap<String, List<String>> amEnvMap = null;
        HashMap<String, List<String>> launcherMapEnvMap = null;
        if (amEnv != null) {
            envStr.append(amEnv);
            amEnvMap = populateEnvMap(amEnv);
        }
        if (launcherMapEnv != null) {
            launcherMapEnvMap = populateEnvMap(launcherMapEnv);
            if (amEnvMap != null) {
                Iterator<String> envKeyItr = launcherMapEnvMap.keySet().iterator();
                while (envKeyItr.hasNext()) {
                    String envKey = envKeyItr.next();
                    if (amEnvMap.containsKey(envKey)) {
                        List<String> amValList = amEnvMap.get(envKey);
                        List<String> launcherValList = launcherMapEnvMap.get(envKey);
                        Iterator<String> valItr = launcherValList.iterator();
                        while (valItr.hasNext()) {
                            String val = valItr.next();
                            if (!amValList.contains(val)) {
                                hasConflictEnv = true;
                                break;
                            }
                            else {
                                valItr.remove();
                            }
                        }
                        if (launcherValList.isEmpty()) {
                            envKeyItr.remove();
                        }
                    }
                }
            }
        }
        if (hasConflictEnv) {
            launcherConf.setBoolean(HADOOP_YARN_UBER_MODE, false);
        }
        else {
            if (launcherMapEnvMap != null) {
                for (String key : launcherMapEnvMap.keySet()) {
                    List<String> launcherValList = launcherMapEnvMap.get(key);
                    for (String val : launcherValList) {
                        if (envStr.length() > 0) {
                            envStr.append(",");
                        }
                        envStr.append(key).append("=").append(val);
                    }
                }
            }

            launcherConf.set(YARN_AM_ENV, envStr.toString());

            // memory.mb
            int launcherMapMemoryMB = launcherConf.getInt(HADOOP_MAP_MEMORY_MB, 1536);
            int amMemoryMB = launcherConf.getInt(YARN_AM_RESOURCE_MB, 1536);
            // YARN_MEMORY_MB_MIN to provide buffer.
            // suppose launcher map aggressively use high memory, need some
            // headroom for AM
            int memoryMB = Math.max(launcherMapMemoryMB, amMemoryMB) + YARN_MEMORY_MB_MIN;
            // limit to 4096 in case of 32 bit
            if (launcherMapMemoryMB < 4096 && amMemoryMB < 4096 && memoryMB > 4096) {
                memoryMB = 4096;
            }
            launcherConf.setInt(YARN_AM_RESOURCE_MB, memoryMB);

            // We already made mapred.child.java.opts and
            // mapreduce.map.java.opts equal, so just start with one of them
            String launcherMapOpts = launcherConf.get(HADOOP_MAP_JAVA_OPTS, "");
            String amChildOpts = launcherConf.get(YARN_AM_COMMAND_OPTS);
            StringBuilder optsStr = new StringBuilder();
            int heapSizeForMap = extractHeapSizeMB(launcherMapOpts);
            int heapSizeForAm = extractHeapSizeMB(amChildOpts);
            int heapSize = Math.max(heapSizeForMap, heapSizeForAm) + YARN_MEMORY_MB_MIN;
            // limit to 3584 in case of 32 bit
            if (heapSizeForMap < 4096 && heapSizeForAm < 4096 && heapSize > 3584) {
                heapSize = 3584;
            }
            if (amChildOpts != null) {
                optsStr.append(amChildOpts);
            }
            optsStr.append(" ").append(launcherMapOpts.trim());
            if (heapSize > 0) {
                // append calculated total heap size to the end
                optsStr.append(" ").append("-Xmx").append(heapSize).append("m");
            }
            launcherConf.set(YARN_AM_COMMAND_OPTS, optsStr.toString().trim());
        }
    }

    void updateConfForJavaTmpDir(Configuration conf) {
        String mapOpts = conf.get(HADOOP_MAP_JAVA_OPTS);
        String reduceOpts = conf.get(HADOOP_REDUCE_JAVA_OPTS);
        String childOpts = conf.get(HADOOP_CHILD_JAVA_OPTS);
        String amChildOpts = conf.get(YARN_AM_COMMAND_OPTS);
        String oozieJavaTmpDirSetting = "-Djava.io.tmpdir=./tmp";
        if (childOpts == null) {
            conf.set(HADOOP_CHILD_JAVA_OPTS, oozieJavaTmpDirSetting);
        } else {
            conf.set(HADOOP_CHILD_JAVA_OPTS, childOpts + " " + oozieJavaTmpDirSetting);
        }

        if (mapOpts != null && !mapOpts.contains(JAVA_TMP_DIR_SETTINGS)) {
            conf.set(HADOOP_MAP_JAVA_OPTS, mapOpts + " " + oozieJavaTmpDirSetting);
        }

        if (reduceOpts != null && !reduceOpts.contains(JAVA_TMP_DIR_SETTINGS)) {
            conf.set(HADOOP_REDUCE_JAVA_OPTS, reduceOpts + " " + oozieJavaTmpDirSetting);
        }

        if (amChildOpts != null && !amChildOpts.contains(JAVA_TMP_DIR_SETTINGS)) {
            conf.set(YARN_AM_COMMAND_OPTS, amChildOpts + " " + oozieJavaTmpDirSetting);
        }
    }

    private HashMap<String, List<String>> populateEnvMap(String input) {
        HashMap<String, List<String>> envMaps = new HashMap<String, List<String>>();
        String[] envEntries = input.split(",");
        for (String envEntry : envEntries) {
            String[] envKeyVal = envEntry.split("=");
            String envKey = envKeyVal[0].trim();
            List<String> valList = envMaps.get(envKey);
            if (valList == null) {
                valList = new ArrayList<String>();
            }
            valList.add(envKeyVal[1].trim());
            envMaps.put(envKey, valList);
        }
        return envMaps;
    }

    public int extractHeapSizeMB(String input) {
        int ret = 0;
        if(input == null || input.equals(""))
            return ret;
        Matcher m = heapPattern.matcher(input);
        String heapStr = null;
        String heapNum = null;
        // Grabs the last match which takes effect (in case that multiple Xmx options specified)
        while (m.find()) {
            heapStr = m.group(1);
            heapNum = m.group(2);
        }
        if (heapStr != null) {
            // when Xmx specified in Gigabyte
            if(heapStr.endsWith("g") || heapStr.endsWith("G")) {
                ret = Integer.parseInt(heapNum) * 1024;
            } else {
                ret = Integer.parseInt(heapNum);
            }
        }
        return ret;
    }

    public static void parseJobXmlAndConfiguration(Context context, Element element, Path appPath, Configuration conf)
            throws IOException, ActionExecutorException, HadoopAccessorException, URISyntaxException {
        Namespace ns = element.getNamespace();
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
                        has.createJobConf(path.toUri().getAuthority()));
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

            // Set the Java Main Class for the Java action to give to the Java launcher
            setJavaMain(actionConf, actionXml);

            parseJobXmlAndConfiguration(context, actionXml, appPath, actionConf);

            // set cancel.delegation.token in actionConf that child job doesn't cancel delegation token
            actionConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
            updateConfForJavaTmpDir(actionConf);
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

        URI uri = null;
        try {
            uri = new URI(filePath);
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
                        Path pathToAdd = new Path(uri.normalize());
                        Services.get().get(HadoopAccessorService.class).addFileToClassPath(user, pathToAdd, conf);
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
            LOG.debug(
                    "Errors when add to DistributedCache. Path=" + uri.toString() + ", archive=" + archive + ", conf="
                            + XmlUtils.prettyPrint(conf).toString());
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
            if (!context.getProtoActionConf().getBoolean("oozie.action.keep.action.dir", false)
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
                                Path pathWithFragment = fragmentName == null ? actionLibPath : new Path(new URI(
                                        actionLibPath.toString()).getPath());
                                String fileName = fragmentName == null ? actionLibPath.getName() : fragmentName;
                                if (confSet.contains(fileName)) {
                                    Configuration jobXmlConf = shareLibService.getShareLibConf(actionShareLibName,
                                            pathWithFragment);
                                    checkForDisallowedProps(jobXmlConf, actionLibPath.getName());
                                    XConfiguration.injectDefaults(jobXmlConf, conf);
                                    LOG.trace("Adding properties of " + actionLibPath + " to job conf");
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
                for (Path libPath : sharelibList) {
                    addToCache(conf, libPath, libPath.toUri().getPath(), false);
                }
            }
            catch (URISyntaxException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "Error configuring sharelib",
                        ex.getMessage());
            }
            catch (IOException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "It should never happen",
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
                if (listOfPaths == null || listOfPaths.isEmpty()) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "EJ001",
                            "Could not locate Oozie sharelib");
                }
                FileSystem fs = listOfPaths.get(0).getFileSystem(conf);
                for (Path actionLibPath : listOfPaths) {
                    JobUtils.addFileToClassPath(actionLibPath, conf, fs);
                    DistributedCache.createSymlink(conf);
                }
                listOfPaths = shareLibService.getSystemLibJars(getType());
                if (listOfPaths != null) {
                    for (Path actionLibPath : listOfPaths) {
                        JobUtils.addFileToClassPath(actionLibPath, conf, fs);
                        DistributedCache.createSymlink(conf);
                    }
                }
            }
            catch (IOException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "It should never happen",
                        ex.getMessage());
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
        // Add common sharelibs for Oozie and launcher jars
        addSystemShareLibForAction(conf);
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
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, JavaMain.class.getName());
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
            // cancel delegation token on a launcher job which stays alive till child job(s) finishes
            // otherwise (in mapred action), doesn't cancel not to disturb running child job
            launcherJobConf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", true);
            setupLauncherConf(launcherJobConf, actionXml, appPathRoot, context);

            String launcherTag = null;
            // Extracting tag and appending action name to maintain the uniqueness.
            if (context.getVar(ActionStartXCommand.OOZIE_ACTION_YARN_TAG) != null) {
                launcherTag = context.getVar(ActionStartXCommand.OOZIE_ACTION_YARN_TAG);
            } else { //Keeping it to maintain backward compatibly with test cases.
                launcherTag = action.getId();
            }

            // Properties for when a launcher job's AM gets restarted
            LauncherMapperHelper.setupYarnRestartHandling(launcherJobConf, actionConf, launcherTag);

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
            LauncherMapperHelper.setupLauncherInfo(launcherJobConf, jobId, actionId, actionDir, recoveryId, actionConf,
                    prepareXML);

            // Set the launcher Main Class
            LauncherMapperHelper.setupMainClass(launcherJobConf, getLauncherMain(launcherJobConf, actionXml));
            LauncherMapperHelper.setupLauncherURIHandlerConf(launcherJobConf);
            LauncherMapperHelper.setupMaxOutputData(launcherJobConf, maxActionOutputLen);
            LauncherMapperHelper.setupMaxExternalStatsSize(launcherJobConf, maxExternalStatsSize);
            LauncherMapperHelper.setupMaxFSGlob(launcherJobConf, maxFSGlobMax);

            List<Element> list = actionXml.getChildren("arg", ns);
            String[] args = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                args[i] = list.get(i).getTextTrim();
            }
            LauncherMapperHelper.setupMainArguments(launcherJobConf, args);

            // Make mapred.child.java.opts and mapreduce.map.java.opts equal, but give values from the latter priority; also append
            // <java-opt> and <java-opts> and give those highest priority
            StringBuilder opts = new StringBuilder(launcherJobConf.get(HADOOP_CHILD_JAVA_OPTS, ""));
            if (launcherJobConf.get(HADOOP_MAP_JAVA_OPTS) != null) {
                opts.append(" ").append(launcherJobConf.get(HADOOP_MAP_JAVA_OPTS));
            }
            List<Element> javaopts = actionXml.getChildren("java-opt", ns);
            for (Element opt: javaopts) {
                opts.append(" ").append(opt.getTextTrim());
            }
            Element opt = actionXml.getChild("java-opts", ns);
            if (opt != null) {
                opts.append(" ").append(opt.getTextTrim());
            }
            launcherJobConf.set(HADOOP_CHILD_JAVA_OPTS, opts.toString().trim());
            launcherJobConf.set(HADOOP_MAP_JAVA_OPTS, opts.toString().trim());

            // setting for uber mode
            if (launcherJobConf.getBoolean(HADOOP_YARN_UBER_MODE, false)) {
                if (checkPropertiesToDisableUber(launcherJobConf)) {
                    launcherJobConf.setBoolean(HADOOP_YARN_UBER_MODE, false);
                }
                else {
                    updateConfForUberMode(launcherJobConf);
                }
            }
            updateConfForJavaTmpDir(launcherJobConf);
            injectLauncherTimelineServiceEnabled(launcherJobConf, actionConf);

            // properties from action that are needed by the launcher (e.g. QUEUE NAME, ACLs)
            // maybe we should add queue to the WF schema, below job-tracker
            actionConfToLauncherConf(actionConf, launcherJobConf);

            return launcherJobConf;
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    private boolean checkPropertiesToDisableUber(Configuration launcherConf) {
        boolean disable = false;
        if (launcherConf.getBoolean(HADOOP_JOB_CLASSLOADER, false)) {
            disable = true;
        }
        else if (launcherConf.getBoolean(HADOOP_USER_CLASSPATH_FIRST, false)) {
            disable = true;
        }
        return disable;
    }

    private void injectCallback(Context context, Configuration conf) {
        String callback = context.getCallbackUrl("$jobStatus");
        if (conf.get("job.end.notification.url") != null) {
            LOG.warn("Overriding the action job end notification URI");
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
            Configuration actionConf = loadHadoopDefaultResources(context, actionXml);
            setupActionConf(actionConf, context, actionXml, appPathRoot);
            LOG.debug("Setting LibFilesArchives ");
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

            injectJobInfo(launcherJobConf, actionConf, context, action);
            injectLauncherCallback(context, launcherJobConf);
            LOG.debug("Creating Job Client for action " + action.getId());
            jobClient = createJobClient(context, launcherJobConf);
            String launcherId = LauncherMapperHelper.getRecoveryId(launcherJobConf, context.getActionDir(), context
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
                LOG.debug("Submitting the job through Job Client for action " + action.getId());

                // setting up propagation of the delegation token.
                HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
                Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(has
                        .getMRDelegationTokenRenewer(launcherJobConf));
                launcherJobConf.getCredentials().addToken(HadoopAccessorService.MR_TOKEN_ALIAS, mrdt);

                // insert credentials tokens to launcher job conf if needed
                if (needInjectCredentials()) {
                    for (Token<? extends TokenIdentifier> tk : credentialsConf.getCredentials().getAllTokens()) {
                        Text fauxAlias = new Text(tk.getKind() + "_" + tk.getService());
                        LOG.debug("ADDING TOKEN: " + fauxAlias);
                        launcherJobConf.getCredentials().addToken(fauxAlias, tk);
                    }
                }
                else {
                    LOG.info("No need to inject credentials.");
                }
                runningJob = jobClient.submitJob(launcherJobConf);
                if (runningJob == null) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA017",
                            "Error submitting launcher for action [{0}]", action.getId());
                }
                launcherId = runningJob.getID().toString();
                LOG.debug("After submission get the launcherId " + launcherId);
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
                        LOG.error("JobClient error: ", e);
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
                        LOG.debug("Credential Properties set for action : " + action.getId());
                        for (String property : prop.getProperties().keySet()) {
                            actionConf.set(property, prop.getProperties().get(property));
                            LOG.debug("property : '" + property + "', value : '" + prop.getProperties().get(property) + "'");
                        }
                    }
                }
            }
            else {
                LOG.warn("No credential properties found for action : " + action.getId() + ", cred : " + action.getCred());
            }
        }
        else {
            LOG.warn("context or action is null");
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
                        LOG.debug("Retrieved Credential '" + credName + "' for action " + action.getId());
                    }
                    else {
                        LOG.debug("Credentials object is null for name= " + credName + ", type=" + credProps.getType());
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "JA020",
                            "Could not load credentials of type [{0}] with name [{1}]]; perhaps it was not defined"
                                + " in oozie-site.xml?", credProps.getType(), credName);
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
            LOG.debug("Get credential '" + credsInAction + "' properties for action : " + action.getId());
            String[] credNames = credsInAction.split(",");
            for (String credName : credNames) {
                CredentialsProperties credProps = getCredProperties(context, credName);
                props.put(credName, credProps);
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
        XConfiguration wfjobConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
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
        } else {
            LOG.debug("credentials is null for the action");
        }
        return credProp;
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        LogUtils.setLogInfo(action);
        try {
            LOG.debug("Starting action " + action.getId() + " getting Action File System");
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
    protected JobClient createJobClient(Context context, JobConf jobConf) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        String group = context.getWorkflow().getGroup();
        return Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
    }

    protected RunningJob getRunningJob(Context context, WorkflowAction action, JobClient jobClient) throws Exception{
        RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalId()));
        return runningJob;
    }

    /**
     * Useful for overriding in actions that do subsequent job runs
     * such as the MapReduce Action, where the launcher job is not the
     * actual job that then gets monitored.
     */
    protected String getActualExternalId(WorkflowAction action) {
        return action.getExternalId();
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        JobClient jobClient = null;
        boolean exception = false;
        LogUtils.setLogInfo(action);
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            FileSystem actionFs = context.getAppFileSystem();
            JobConf jobConf = createBaseHadoopConf(context, actionXml);
            jobClient = createJobClient(context, jobConf);
            RunningJob runningJob = getRunningJob(context, action, jobClient);
            if (runningJob == null) {
                context.setExecutionData(FAILED, null);
                throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA017",
                        "Could not lookup launched hadoop Job ID [{0}] which was associated with " +
                        " action [{1}].  Failing this action!", getActualExternalId(action), action.getId());
            }
            if (runningJob.isComplete()) {
                Path actionDir = context.getActionDir();
                String newId = null;
                // load sequence file into object
                Map<String, String> actionData = LauncherMapperHelper.getActionData(actionFs, actionDir, jobConf);
                if (actionData.containsKey(LauncherMapper.ACTION_DATA_NEW_ID)) {
                    newId = actionData.get(LauncherMapper.ACTION_DATA_NEW_ID);
                    String launcherId = action.getExternalId();
                    runningJob = jobClient.getJob(JobID.forName(newId));
                    if (runningJob == null) {
                        context.setExternalStatus(FAILED);
                        throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA017",
                                "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", newId,
                                action.getId());
                    }
                    context.setExternalChildIDs(newId);
                    LOG.info(XLog.STD, "External ID swap, old ID [{0}] new ID [{1}]", launcherId,
                            newId);
                }
                else {
                    String externalIDs = actionData.get(LauncherMapper.ACTION_DATA_EXTERNAL_CHILD_IDS);
                    if (externalIDs != null) {
                        context.setExternalChildIDs(externalIDs);
                        LOG.info(XLog.STD, "Hadoop Jobs launched : [{0}]", externalIDs);
                    }
                }
                if (runningJob.isComplete()) {
                    // fetching action output and stats for the Map-Reduce action.
                    if (newId != null) {
                        actionData = LauncherMapperHelper.getActionData(actionFs, context.getActionDir(), jobConf);
                    }
                    LOG.info(XLog.STD, "action completed, external ID [{0}]",
                            action.getExternalId());
                    if (LauncherMapperHelper.isMainSuccessful(runningJob)) {
                        if (getCaptureOutput(action) && LauncherMapperHelper.hasOutputData(actionData)) {
                            context.setExecutionData(SUCCEEDED, PropertiesUtils.stringToProperties(actionData
                                    .get(LauncherMapper.ACTION_DATA_OUTPUT_PROPS)));
                            LOG.info(XLog.STD, "action produced output");
                        }
                        else {
                            context.setExecutionData(SUCCEEDED, null);
                        }
                        if (LauncherMapperHelper.hasStatsData(actionData)) {
                            context.setExecutionStats(actionData.get(LauncherMapper.ACTION_DATA_STATS));
                            LOG.info(XLog.STD, "action produced stats");
                        }
                        getActionData(actionFs, runningJob, action, context);
                    }
                    else {
                        String errorReason;
                        if (actionData.containsKey(LauncherMapper.ACTION_DATA_ERROR_PROPS)) {
                            Properties props = PropertiesUtils.stringToProperties(actionData
                                    .get(LauncherMapper.ACTION_DATA_ERROR_PROPS));
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
                            errorReason = XLog.format("LauncherMapper died, check Hadoop LOG for job [{0}:{1}]", action
                                    .getTrackerUri(), action.getExternalId());
                            LOG.warn(errorReason);
                        }
                        context.setExecutionData(FAILED_KILLED, null);
                    }
                }
                else {
                    context.setExternalStatus("RUNNING");
                    LOG.info(XLog.STD, "checking action, hadoop job ID [{0}] status [RUNNING]",
                            runningJob.getID());
                }
            }
            else {
                context.setExternalStatus("RUNNING");
                LOG.info(XLog.STD, "checking action, hadoop job ID [{0}] status [RUNNING]",
                        runningJob.getID());
            }
        }
        catch (Exception ex) {
            LOG.warn("Exception in check(). Message[{0}]", ex.getMessage(), ex);
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
                        LOG.error("JobClient error: ", e);
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
    }

    protected final void readExternalChildIDs(WorkflowAction action, Context context) throws IOException {
        if (action.getData() != null) {
            // Load stored Hadoop jobs ids and promote them as external child ids
            // See LauncherMain#writeExternalChildIDs for how they are written
            Properties props = new Properties();
            props.load(new StringReader(action.getData()));
            context.setExternalChildIDs((String) props.get(LauncherMain.HADOOP_JOBS));
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
            JobConf jobConf = createBaseHadoopConf(context, actionXml);
            jobClient = createJobClient(context, jobConf);
            RunningJob runningJob = getRunningJob(context, action, jobClient);
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
                    LOG.error("Error: ", ex);
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
     * Return the sharelib names for the action.
     * <p/>
     * If <code>NULL</code> or empty, it means that the action does not use the action
     * sharelib.
     * <p/>
     * If a non-empty string, i.e. <code>foo</code>, it means the action uses the
     * action sharelib sub-directory <code>foo</code> and all JARs in the sharelib
     * <code>foo</code> directory will be in the action classpath. Multiple sharelib
     * sub-directories can be specified as a comma separated list.
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
     * @param conf action configuration.
     * @return the action sharelib names.
     */
    protected String[] getShareLibNames(Context context, Element actionXml, Configuration conf) {
        String[] names = conf.getStrings(ACTION_SHARELIB_FOR + getType());
        if (names == null || names.length == 0) {
            try {
                XConfiguration jobConf = new XConfiguration(new StringReader(context.getWorkflow().getConf()));
                names = jobConf.getStrings(ACTION_SHARELIB_FOR + getType());
                if (names == null || names.length == 0) {
                    names = Services.get().getConf().getStrings(ACTION_SHARELIB_FOR + getType());
                    if (names == null || names.length == 0) {
                        String name = getDefaultShareLibName(actionXml);
                        if (name != null) {
                            names = new String[] { name };
                        }
                    }
                }
            }
            catch (IOException ex) {
                throw new RuntimeException("It cannot happen, " + ex.toString(), ex);
            }
        }
        return names;
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

    public String[] getShareLibFilesForActionConf() {
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

    private void injectJobInfo(JobConf launcherJobConf, Configuration actionConf, Context context, WorkflowAction action) {
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
}
