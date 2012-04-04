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
package org.apache.oozie.service;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.security.token.Token;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The HadoopAccessorService returns HadoopAccessor instances configured to work on behalf of a user-group. <p/> The
 * default accessor used is the base accessor which just injects the UGI into the configuration instance used to
 * create/obtain JobClient and ileSystem instances. <p/> The HadoopAccess class to use can be configured in the
 * <code>oozie-site.xml</code> using the <code>oozie.service.HadoopAccessorService.accessor.class</code> property.
 */
public class HadoopAccessorService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HadoopAccessorService.";
    public static final String JOB_TRACKER_WHITELIST = CONF_PREFIX + "jobTracker.whitelist";
    public static final String NAME_NODE_WHITELIST = CONF_PREFIX + "nameNode.whitelist";
    public static final String HADOOP_CONFS = CONF_PREFIX + "hadoop.configurations";
    public static final String ACTION_CONFS = CONF_PREFIX + "action.configurations";
    public static final String KERBEROS_AUTH_ENABLED = CONF_PREFIX + "kerberos.enabled";
    public static final String KERBEROS_KEYTAB = CONF_PREFIX + "keytab.file";
    public static final String KERBEROS_PRINCIPAL = CONF_PREFIX + "kerberos.principal";

    private static final String OOZIE_HADOOP_ACCESSOR_SERVICE_CREATED = "oozie.HadoopAccessorService.created";

    private Set<String> jobTrackerWhitelist = new HashSet<String>();
    private Set<String> nameNodeWhitelist = new HashSet<String>();
    private Map<String, Configuration> hadoopConfigs = new HashMap<String, Configuration>();
    private Map<String, File> actionConfigDirs = new HashMap<String, File>();
    private Map<String, Map<String, XConfiguration>> actionConfigs = new HashMap<String, Map<String, XConfiguration>>();

    private ConcurrentMap<String, UserGroupInformation> userUgiMap;

    public void init(Services services) throws ServiceException {
        init(services.getConf());
    }

    //for testing purposes, see XFsTestCase
    public void init(Configuration conf) throws ServiceException {
        for (String name : conf.getStringCollection(JOB_TRACKER_WHITELIST)) {
            String tmp = name.toLowerCase().trim();
            if (tmp.length() == 0) {
                continue;
            }
            jobTrackerWhitelist.add(tmp);
        }
        XLog.getLog(getClass()).info(
                "JOB_TRACKER_WHITELIST :" + conf.getStringCollection(JOB_TRACKER_WHITELIST)
                        + ", Total entries :" + jobTrackerWhitelist.size());
        for (String name : conf.getStringCollection(NAME_NODE_WHITELIST)) {
            String tmp = name.toLowerCase().trim();
            if (tmp.length() == 0) {
                continue;
            }
            nameNodeWhitelist.add(tmp);
        }
        XLog.getLog(getClass()).info(
                "NAME_NODE_WHITELIST :" + conf.getStringCollection(NAME_NODE_WHITELIST)
                        + ", Total entries :" + nameNodeWhitelist.size());

        boolean kerberosAuthOn = conf.getBoolean(KERBEROS_AUTH_ENABLED, true);
        XLog.getLog(getClass()).info("Oozie Kerberos Authentication [{0}]", (kerberosAuthOn) ? "enabled" : "disabled");
        if (kerberosAuthOn) {
            kerberosInit(conf);
        }
        else {
            Configuration ugiConf = new Configuration();
            ugiConf.set("hadoop.security.authentication", "simple");
            UserGroupInformation.setConfiguration(ugiConf);
        }

        userUgiMap = new ConcurrentHashMap<String, UserGroupInformation>();

        loadHadoopConfigs(conf);
        preLoadActionConfigs(conf);
    }

    private void kerberosInit(Configuration serviceConf) throws ServiceException {
            try {
                String keytabFile = serviceConf.get(KERBEROS_KEYTAB,
                                                    System.getProperty("user.home") + "/oozie.keytab").trim();
                if (keytabFile.length() == 0) {
                    throw new ServiceException(ErrorCode.E0026, KERBEROS_KEYTAB);
                }
                String principal = serviceConf.get(KERBEROS_PRINCIPAL, "oozie/localhost@LOCALHOST");
                if (principal.length() == 0) {
                    throw new ServiceException(ErrorCode.E0026, KERBEROS_PRINCIPAL);
                }
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
                XLog.getLog(getClass()).info("Got Kerberos ticket, keytab [{0}], Oozie principal principal [{1}]",
                                             keytabFile, principal);
            }
            catch (ServiceException ex) {
                throw ex;
            }
            catch (Exception ex) {
                throw new ServiceException(ErrorCode.E0100, getClass().getName(), ex.getMessage(), ex);
            }
    }

    private static final String[] HADOOP_CONF_FILES =
        {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml"};


    private Configuration loadHadoopConf(File dir) throws IOException {
        Configuration hadoopConf = new XConfiguration();
        for (String file : HADOOP_CONF_FILES) {
            File f = new File(dir, file);
            if (f.exists()) {
                InputStream is = new FileInputStream(f);
                Configuration conf = new XConfiguration(is);
                is.close();
                XConfiguration.copy(conf, hadoopConf);
            }
        }
        return hadoopConf;
    }

    private Map<String, File> parseConfigDirs(String[] confDefs, String type) throws ServiceException, IOException {
        Map<String, File> map = new HashMap<String, File>();
        File configDir = new File(ConfigurationService.getConfigurationDirectory());
        for (String confDef : confDefs) {
            if (confDef.trim().length() > 0) {
                String[] parts = confDef.split("=");
                if (parts.length == 2) {
                    String hostPort = parts[0];
                    String confDir = parts[1];
                    File dir = new File(confDir);
                    if (!dir.isAbsolute()) {
                        dir = new File(configDir, confDir);
                    }
                    if (dir.exists()) {
                        map.put(hostPort.toLowerCase(), dir);
                    }
                    else {
                        throw new ServiceException(ErrorCode.E0100, getClass().getName(),
                                                   "could not find " + type + " configuration directory: " +
                                                   dir.getAbsolutePath());
                    }
                }
                else {
                    throw new ServiceException(ErrorCode.E0100, getClass().getName(),
                                               "Incorrect " + type + " configuration definition: " + confDef);
                }
            }
        }
        return map;
    }

    private void loadHadoopConfigs(Configuration serviceConf) throws ServiceException {
        try {
            Map<String, File> map = parseConfigDirs(serviceConf.getStrings(HADOOP_CONFS, "*=hadoop-conf"), "hadoop");
            for (Map.Entry<String, File> entry : map.entrySet()) {
                hadoopConfigs.put(entry.getKey(), loadHadoopConf(entry.getValue()));
            }
        }
        catch (ServiceException ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), ex.getMessage(), ex);
        }
    }

    private void preLoadActionConfigs(Configuration serviceConf) throws ServiceException {
        try {
            actionConfigDirs = parseConfigDirs(serviceConf.getStrings(ACTION_CONFS, "*=hadoop-conf"), "action");
            for (String hostport : actionConfigDirs.keySet()) {
                actionConfigs.put(hostport, new ConcurrentHashMap<String, XConfiguration>());
            }
        }
        catch (ServiceException ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), ex.getMessage(), ex);
        }
    }

    public void destroy() {
    }

    public Class<? extends Service> getInterface() {
        return HadoopAccessorService.class;
    }

    private UserGroupInformation getUGI(String user) throws IOException {
        UserGroupInformation ugi = userUgiMap.get(user);
        if (ugi == null) {
            // taking care of a race condition, the latest UGI will be discarded
            ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
            userUgiMap.putIfAbsent(user, ugi);
        }
        return ugi;
    }

    /**
     * Creates a JobConf using the site configuration for the specified hostname:port.
     * <p/>
     * If the specified hostname:port is not defined it falls back to the '*' site
     * configuration if available. If the '*' site configuration is not available,
     * the JobConf has all Hadoop defaults.
     *
     * @param hostPort hostname:port to lookup Hadoop site configuration.
     * @return a JobConf with the corresponding site configuration for hostPort.
     */
    public JobConf createJobConf(String hostPort) {
        JobConf jobConf = new JobConf();
        XConfiguration.copy(getConfiguration(hostPort), jobConf);
        jobConf.setBoolean(OOZIE_HADOOP_ACCESSOR_SERVICE_CREATED, true);
        return jobConf;
    }

    private XConfiguration loadActionConf(String hostPort, String action) {
        File dir = actionConfigDirs.get(hostPort);
        XConfiguration actionConf = new XConfiguration();
        if (dir != null) {
            File actionConfFile = new File(dir, action + ".xml");
            if (actionConfFile.exists()) {
                try {
                    actionConf = new XConfiguration(new FileInputStream(actionConfFile));
                }
                catch (IOException ex) {
                    XLog.getLog(getClass()).warn("Could not read file [{0}] for action [{1}] configuration for hostPort [{2}]",
                                                 actionConfFile.getAbsolutePath(), action, hostPort);
                }
            }
        }
        return actionConf;
    }

    /**
     * Returns a Configuration containing any defaults for an action for a particular cluster.
     * <p/>
     * This configuration is used as default for the action configuration and enables cluster
     * level default values per action.
     *
     * @param hostPort hostname"port to lookup the action default confiugration.
     * @param action action name.
     * @return the default configuration for the action for the specified cluster.
     */
    public XConfiguration createActionDefaultConf(String hostPort, String action) {
        hostPort = (hostPort != null) ? hostPort.toLowerCase() : null;
        Map<String, XConfiguration> hostPortActionConfigs = actionConfigs.get(hostPort);
        if (hostPortActionConfigs == null) {
            hostPortActionConfigs = actionConfigs.get("*");
            hostPort = "*";
        }
        XConfiguration actionConf = hostPortActionConfigs.get(action);
        if (actionConf == null) {
            // doing lazy loading as we don't know upfront all actions, no need to synchronize
            // as it is a read operation an in case of a race condition loading and inserting
            // into the Map is idempotent and the action-config Map is a ConcurrentHashMap
            actionConf = loadActionConf(hostPort, action);
            hostPortActionConfigs.put(action, actionConf);
        }
        return new XConfiguration(actionConf.toProperties());
    }

    private Configuration getConfiguration(String hostPort) {
        hostPort = (hostPort != null) ? hostPort.toLowerCase() : null;
        Configuration conf = hadoopConfigs.get(hostPort);
        if (conf == null) {
            conf = hadoopConfigs.get("*");
            if (conf == null) {
                conf = new XConfiguration();
            }
        }
        return conf;
    }

    /**
     * Return a JobClient created with the provided user/group.
     *
     *
     * @param conf JobConf with all necessary information to create the
     *        JobClient.
     * @return JobClient created with the provided user/group.
     * @throws HadoopAccessorException if the client could not be created.
     */
    public JobClient createJobClient(String user, final JobConf conf) throws HadoopAccessorException {
        ParamChecker.notEmpty(user, "user");
        if (!conf.getBoolean(OOZIE_HADOOP_ACCESSOR_SERVICE_CREATED, false)) {
            throw new HadoopAccessorException(ErrorCode.E0903);
        }
        String jobTracker = conf.get("mapred.job.tracker");
        validateJobTracker(jobTracker);
        try {
            UserGroupInformation ugi = getUGI(user);
            JobClient jobClient = ugi.doAs(new PrivilegedExceptionAction<JobClient>() {
                public JobClient run() throws Exception {
                    return new JobClient(conf);
                }
            });
            Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(new Text("mr token"));
            conf.getCredentials().addToken(new Text("mr token"), mrdt);
            return jobClient;
        }
        catch (InterruptedException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex);
        }
        catch (IOException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex);
        }
    }

    /**
     * Return a FileSystem created with the provided user for the specified URI.
     *
     *
     * @param uri file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws HadoopAccessorException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, final URI uri, final Configuration conf)
            throws HadoopAccessorException {
        ParamChecker.notEmpty(user, "user");
        if (!conf.getBoolean(OOZIE_HADOOP_ACCESSOR_SERVICE_CREATED, false)) {
            throw new HadoopAccessorException(ErrorCode.E0903);
        }
        String nameNode = uri.getAuthority();
        if (nameNode == null) {
            nameNode = conf.get("fs.default.name");
            if (nameNode != null) {
                try {
                    nameNode = new URI(nameNode).getAuthority();
                }
                catch (URISyntaxException ex) {
                    throw new HadoopAccessorException(ErrorCode.E0902, ex);
                }
            }
        }
        validateNameNode(nameNode);

        try {
            UserGroupInformation ugi = getUGI(user);
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        }
        catch (InterruptedException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex);
        }
        catch (IOException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex);
        }
    }

    /**
     * Validate Job tracker
     * @param jobTrackerUri
     * @throws HadoopAccessorException
     */
    protected void validateJobTracker(String jobTrackerUri) throws HadoopAccessorException {
        validate(jobTrackerUri, jobTrackerWhitelist, ErrorCode.E0900);
    }

    /**
     * Validate Namenode list
     * @param nameNodeUri
     * @throws HadoopAccessorException
     */
    protected void validateNameNode(String nameNodeUri) throws HadoopAccessorException {
        validate(nameNodeUri, nameNodeWhitelist, ErrorCode.E0901);
    }

    private void validate(String uri, Set<String> whitelist, ErrorCode error) throws HadoopAccessorException {
        if (uri != null) {
            uri = uri.toLowerCase().trim();
            if (whitelist.size() > 0 && !whitelist.contains(uri)) {
                throw new HadoopAccessorException(error, uri);
            }
        }
    }

    public void addFileToClassPath(String user, final Path file, final Configuration conf)
            throws IOException {
        ParamChecker.notEmpty(user, "user");
        try {
            UserGroupInformation ugi = getUGI(user);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    Configuration defaultConf = new Configuration();
                    XConfiguration.copy(conf, defaultConf);
                    //Doing this NOP add first to have the FS created and cached
                    DistributedCache.addFileToClassPath(file, defaultConf);

                    DistributedCache.addFileToClassPath(file, conf);
                    return null;
                }
            });

        }
        catch (InterruptedException ex) {
            throw new IOException(ex);
        }

    }

}
