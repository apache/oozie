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
import org.apache.oozie.workflow.WorkflowApp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
    public static final String KERBEROS_AUTH_ENABLED = CONF_PREFIX + "kerberos.enabled";
    public static final String KERBEROS_KEYTAB = CONF_PREFIX + "keytab.file";
    public static final String KERBEROS_PRINCIPAL = CONF_PREFIX + "kerberos.principal";

    private Set<String> jobTrackerWhitelist = new HashSet<String>();
    private Set<String> nameNodeWhitelist = new HashSet<String>();
    private Map<String, Configuration> hadoopConfigs = new HashMap<String, Configuration>();

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

    private void loadHadoopConfigs(Configuration serviceConf) throws ServiceException {
        try {
            File configDir = new File(ConfigurationService.getConfigurationDirectory());
            String[] confDefs = serviceConf.getStrings(HADOOP_CONFS, "*=hadoop-config.xml");
            for (String confDef : confDefs) {
                if (confDef.trim().length() > 0) {
                    String[] parts = confDef.split("=");
                    String hostPort = parts[0];
                    String confFile = parts[1];
                    File configFile = new File(configDir, confFile);
                    if (configFile.exists()) {
                        Configuration conf = new XConfiguration(new FileInputStream(configFile));
                        hadoopConfigs.put(hostPort.toLowerCase(), conf);
                    }
                    else {
                        throw new ServiceException(ErrorCode.E0100, getClass().getName(),
                                                   "could not find hadoop configuration file: " + confFile);
                    }
                }
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

    public Configuration getConfiguration(String hostPort) {
        Configuration conf = hadoopConfigs.get(hostPort.toLowerCase());
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
     * @param conf JobConf with all necessary information to create the
     *        JobClient.
     * @return JobClient created with the provided user/group.
     * @throws HadoopAccessorException if the client could not be created.
     */
    public JobClient createJobClient(String user, String group, final JobConf conf) throws HadoopAccessorException {
        ParamChecker.notEmpty(user, "user");
        String jobTracker = conf.get("mapred.job.tracker");
        validateJobTracker(jobTracker);
        XConfiguration.injectDefaults(getConfiguration(jobTracker), conf);
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
     * Return a FileSystem created with the provided user/group.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws HadoopAccessorException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, final Configuration conf)
            throws HadoopAccessorException {
        try {
            return createFileSystem(user, group,new URI(conf.get("fs.default.name")), conf);
        }
        catch (URISyntaxException ex) {
            throw new HadoopAccessorException(ErrorCode.E0902, ex);
        }
    }

    /**
     * Return a FileSystem created with the provided user/group for the specified URI.
     *
     * @param uri file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws HadoopAccessorException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, final URI uri, final Configuration conf)
            throws HadoopAccessorException {
        ParamChecker.notEmpty(user, "user");
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

        //it is null in the case of localFileSystem (in many testcases)
        if (nameNode != null) {
            XConfiguration.injectDefaults(getConfiguration(nameNode), conf);
        }
        try {
            UserGroupInformation ugi = getUGI(user);
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    Configuration defaultConf = new Configuration();
                    XConfiguration.copy(conf, defaultConf);
                    return FileSystem.get(uri, defaultConf);
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

    public void addFileToClassPath(String user, String group, final Path file, final Configuration conf)
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
