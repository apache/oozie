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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.security.token.Token;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.JobUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The HadoopAccessorService returns HadoopAccessor instances configured to work on behalf of a user-group. <p/> The
 * default accessor used is the base accessor which just injects the UGI into the configuration instance used to
 * create/obtain JobClient and FileSystem instances.
 */
public class HadoopAccessorService implements Service {

    private static XLog LOG = XLog.getLog(HadoopAccessorService.class);

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HadoopAccessorService.";
    public static final String JOB_TRACKER_WHITELIST = CONF_PREFIX + "jobTracker.whitelist";
    public static final String NAME_NODE_WHITELIST = CONF_PREFIX + "nameNode.whitelist";
    public static final String HADOOP_CONFS = CONF_PREFIX + "hadoop.configurations";
    public static final String ACTION_CONFS = CONF_PREFIX + "action.configurations";
    public static final String ACTION_CONFS_LOAD_DEFAULT_RESOURCES = ACTION_CONFS + ".load.default.resources";
    public static final String KERBEROS_AUTH_ENABLED = CONF_PREFIX + "kerberos.enabled";
    public static final String KERBEROS_KEYTAB = CONF_PREFIX + "keytab.file";
    public static final String KERBEROS_PRINCIPAL = CONF_PREFIX + "kerberos.principal";
    public static final Text MR_TOKEN_ALIAS = new Text("oozie mr token");

    protected static final String OOZIE_HADOOP_ACCESSOR_SERVICE_CREATED = "oozie.HadoopAccessorService.created";
    /** The Kerberos principal for the job tracker.*/
    protected static final String JT_PRINCIPAL = "mapreduce.jobtracker.kerberos.principal";
    /** The Kerberos principal for the resource manager.*/
    protected static final String RM_PRINCIPAL = "yarn.resourcemanager.principal";
    protected static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
    protected static final String HADOOP_JOB_TRACKER_2 = "mapreduce.jobtracker.address";
    protected static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
    private static final Map<String, Text> mrTokenRenewers = new HashMap<String, Text>();

    private static final String DEFAULT_ACTIONNAME = "default";

    private Set<String> jobTrackerWhitelist = new HashSet<String>();
    private Set<String> nameNodeWhitelist = new HashSet<String>();
    private Map<String, Configuration> hadoopConfigs = new HashMap<String, Configuration>();
    private Map<String, File> actionConfigDirs = new HashMap<String, File>();
    private Map<String, Map<String, XConfiguration>> actionConfigs = new HashMap<String, Map<String, XConfiguration>>();

    private UserGroupInformationService ugiService;

    /**
     * Supported filesystem schemes for namespace federation
     */
    public static final String SUPPORTED_FILESYSTEMS = CONF_PREFIX + "supported.filesystems";
    private Set<String> supportedSchemes;
    private boolean allSchemesSupported;

    public void init(Services services) throws ServiceException {
        this.ugiService = services.get(UserGroupInformationService.class);
        init(services.getConf());
    }

    //for testing purposes, see XFsTestCase
    public void init(Configuration conf) throws ServiceException {
        for (String name : ConfigurationService.getStrings(conf, JOB_TRACKER_WHITELIST)) {
            String tmp = name.toLowerCase().trim();
            if (tmp.length() == 0) {
                continue;
            }
            jobTrackerWhitelist.add(tmp);
        }
        LOG.info(
                "JOB_TRACKER_WHITELIST :" + jobTrackerWhitelist.toString()
                        + ", Total entries :" + jobTrackerWhitelist.size());
        for (String name : ConfigurationService.getStrings(conf, NAME_NODE_WHITELIST)) {
            String tmp = name.toLowerCase().trim();
            if (tmp.length() == 0) {
                continue;
            }
            nameNodeWhitelist.add(tmp);
        }
        LOG.info(
                "NAME_NODE_WHITELIST :" + nameNodeWhitelist.toString()
                        + ", Total entries :" + nameNodeWhitelist.size());

        boolean kerberosAuthOn = ConfigurationService.getBoolean(conf, KERBEROS_AUTH_ENABLED);
        LOG.info("Oozie Kerberos Authentication [{0}]", (kerberosAuthOn) ? "enabled" : "disabled");
        if (kerberosAuthOn) {
            kerberosInit(conf);
        }
        else {
            Configuration ugiConf = new Configuration();
            ugiConf.set("hadoop.security.authentication", "simple");
            UserGroupInformation.setConfiguration(ugiConf);
        }

        if (ugiService == null) { //for testing purposes, see XFsTestCase
            this.ugiService = new UserGroupInformationService();
        }

        loadHadoopConfigs(conf);
        preLoadActionConfigs(conf);

        supportedSchemes = new HashSet<String>();
        String[] schemesFromConf = ConfigurationService.getStrings(conf, SUPPORTED_FILESYSTEMS);
        if(schemesFromConf != null) {
            for (String scheme: schemesFromConf) {
                scheme = scheme.trim();
                // If user gives "*", supportedSchemes will be empty, so that checking is not done i.e. all schemes allowed
                if(scheme.equals("*")) {
                    if(schemesFromConf.length > 1) {
                        throw new ServiceException(ErrorCode.E0100, getClass().getName(),
                            SUPPORTED_FILESYSTEMS + " should contain either only wildcard or explicit list, not both");
                    }
                    allSchemesSupported = true;
                }
                supportedSchemes.add(scheme);
            }
        }
    }

    private void kerberosInit(Configuration serviceConf) throws ServiceException {
            try {
                String keytabFile = ConfigurationService.get(serviceConf, KERBEROS_KEYTAB).trim();
                if (keytabFile.length() == 0) {
                    throw new ServiceException(ErrorCode.E0026, KERBEROS_KEYTAB);
                }
                String principal = SecurityUtil.getServerPrincipal(
                        serviceConf.get(KERBEROS_PRINCIPAL, "oozie/localhost@LOCALHOST"),
                        InetAddress.getLocalHost().getCanonicalHostName());
                if (principal.length() == 0) {
                    throw new ServiceException(ErrorCode.E0026, KERBEROS_PRINCIPAL);
                }
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
                LOG.info("Got Kerberos ticket, keytab [{0}], Oozie principal principal [{1}]",
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
        {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml", "ssl-client.xml"};


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
            Map<String, File> map = parseConfigDirs(ConfigurationService.getStrings(serviceConf, HADOOP_CONFS),
                    "hadoop");
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
            actionConfigDirs = parseConfigDirs(ConfigurationService.getStrings(serviceConf, ACTION_CONFS), "action");
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
        return ugiService.getProxyUser(user);
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
            // See if a dir with the action name exists.   If so, load all the xml files in the dir
            File actionConfDir = new File(dir, action);

            if (actionConfDir.exists() && actionConfDir.isDirectory()) {
                LOG.info("Processing configuration files under [{0}]"
                                + " for action [{1}] and hostPort [{2}]",
                        actionConfDir.getAbsolutePath(), action, hostPort);
                File[] xmlFiles = actionConfDir.listFiles(
                        new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return name.endsWith(".xml");
                            }});
                Arrays.sort(xmlFiles, new Comparator<File>() {
                    @Override
                    public int compare(File o1, File o2) {
                        return o1.getName().compareTo(o2.getName());
                    }
                });
                for (File f : xmlFiles) {
                    if (f.isFile() && f.canRead()) {
                        LOG.info("Processing configuration file [{0}]", f.getName());
                        FileInputStream fis = null;
                        try {
                            fis = new FileInputStream(f);
                            XConfiguration conf = new XConfiguration(fis);
                            XConfiguration.copy(conf, actionConf);
                        }
                        catch (IOException ex) {
                            LOG
                                .warn("Could not read file [{0}] for action [{1}] configuration and hostPort [{2}]",
                                        f.getAbsolutePath(), action, hostPort);
                        }
                        finally {
                            if (fis != null) {
                                try { fis.close(); } catch(IOException ioe) { }
                            }
                        }
                    }
                }
            }
        }

        // Now check for <action.xml>   This way <action.xml> has priority over <action-dir>/*.xml

        File actionConfFile = new File(dir, action + ".xml");
        if (actionConfFile.exists()) {
            try {
                XConfiguration conf = new XConfiguration(new FileInputStream(actionConfFile));
                XConfiguration.copy(conf, actionConf);
            }
            catch (IOException ex) {
                LOG.warn("Could not read file [{0}] for action [{1}] configuration for hostPort [{2}]",
                        actionConfFile.getAbsolutePath(), action, hostPort);
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

            // We first load a action of type default
            // This allows for global configuration for all actions - for example
            // all launchers in one queue and actions in another queue
            // Are some configuration that applies to multiple actions - like
            // config libraries path etc
            actionConf = loadActionConf(hostPort, DEFAULT_ACTIONNAME);

            // Action specific default configuration will override the default action config

            XConfiguration.copy(loadActionConf(hostPort, action), actionConf);
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
        String jobTracker = conf.get(JavaActionExecutor.HADOOP_JOB_TRACKER);
        validateJobTracker(jobTracker);
        try {
            UserGroupInformation ugi = getUGI(user);
            JobClient jobClient = ugi.doAs(new PrivilegedExceptionAction<JobClient>() {
                public JobClient run() throws Exception {
                    return new JobClient(conf);
                }
            });
            Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(getMRDelegationTokenRenewer(conf));
            conf.getCredentials().addToken(MR_TOKEN_ALIAS, mrdt);
            return jobClient;
        }
        catch (InterruptedException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex.getMessage(), ex);
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

        checkSupportedFilesystem(uri);

        String nameNode = uri.getAuthority();
        if (nameNode == null) {
            nameNode = conf.get("fs.default.name");
            if (nameNode != null) {
                try {
                    nameNode = new URI(nameNode).getAuthority();
                }
                catch (URISyntaxException ex) {
                    throw new HadoopAccessorException(ErrorCode.E0902, ex.getMessage(), ex);
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
            throw new HadoopAccessorException(ErrorCode.E0902, ex.getMessage(), ex);
        }
        catch (IOException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex.getMessage(), ex);
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
                throw new HadoopAccessorException(error, uri, whitelist);
            }
        }
    }

    public Text getMRDelegationTokenRenewer(JobConf jobConf) throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) { // secure cluster
            return getMRTokenRenewerInternal(jobConf);
        }
        else {
            return MR_TOKEN_ALIAS; //Doesn't matter what we pass as renewer
        }
    }

    // Package private for unit test purposes
    Text getMRTokenRenewerInternal(JobConf jobConf) throws IOException {
        // Getting renewer correctly for JT principal also though JT in hadoop 1.x does not have
        // support for renewing/cancelling tokens
        String servicePrincipal = jobConf.get(RM_PRINCIPAL, jobConf.get(JT_PRINCIPAL));
        Text renewer;
        if (servicePrincipal != null) { // secure cluster
            renewer = mrTokenRenewers.get(servicePrincipal);
            if (renewer == null) {
                // Mimic org.apache.hadoop.mapred.Master.getMasterPrincipal()
                String target = jobConf.get(HADOOP_YARN_RM, jobConf.get(HADOOP_JOB_TRACKER_2));
                if (target == null) {
                    target = jobConf.get(HADOOP_JOB_TRACKER);
                }
                try {
                    String addr = NetUtils.createSocketAddr(target).getHostName();
                    renewer = new Text(SecurityUtil.getServerPrincipal(servicePrincipal, addr));
                    LOG.info("Delegation Token Renewer details: Principal=" + servicePrincipal + ",Target=" + target
                            + ",Renewer=" + renewer);
                }
                catch (IllegalArgumentException iae) {
                    renewer = new Text(servicePrincipal.split("[/@]")[0]);
                    LOG.info("Delegation Token Renewer for " + servicePrincipal + " is " + renewer);
                }
                mrTokenRenewers.put(servicePrincipal, renewer);
            }
        }
        else {
            renewer = MR_TOKEN_ALIAS; //Doesn't matter what we pass as renewer
        }
        return renewer;
    }

    public void addFileToClassPath(String user, final Path file, final Configuration conf)
            throws IOException {
        ParamChecker.notEmpty(user, "user");
        try {
            UserGroupInformation ugi = getUGI(user);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    JobUtils.addFileToClassPath(file, conf, null);
                    return null;
                }
            });

        }
        catch (InterruptedException ex) {
            throw new IOException(ex);
        }

    }

    /**
     * checks configuration parameter if filesystem scheme is among the list of supported ones
     * this makes system robust to filesystems other than HDFS also
     */

    public void checkSupportedFilesystem(URI uri) throws HadoopAccessorException {
        if (allSchemesSupported)
            return;
        String uriScheme = uri.getScheme();
        if (uriScheme != null) {    // skip the check if no scheme is given
            if(!supportedSchemes.isEmpty()) {
                LOG.debug("Checking if filesystem " + uriScheme + " is supported");
                if (!supportedSchemes.contains(uriScheme)) {
                    throw new HadoopAccessorException(ErrorCode.E0904, uriScheme, uri.toString());
                }
             }
         }
    }

    public Set<String> getSupportedSchemes() {
        return supportedSchemes;
    }

}
