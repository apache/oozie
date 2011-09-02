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
package org.apache.oozie.service.kerberos;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The HadoopAccessorService returns HadoopAccessor instances configured to work on behalf of a user-group.
 * <p/>
 * The default accessor used is the base accessor which just injects the UGI into the configuration instance
 * used to create/obtain JobClient and ileSystem instances.
 * <p/>
 * The HadoopAccess class to use can be configured in the <code>oozie-site.xml</code> using the
 * <code>oozie.service.HadoopAccessorService.accessor.class</code> property.
 */
public class KerberosHadoopAccessorService extends HadoopAccessorService {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HadoopAccessorService.";

    public static final String KERBEROS_AUTH_ENABLED = CONF_PREFIX + "kerberos.enabled";
    public static final String KERBEROS_KEYTAB = CONF_PREFIX + "keytab.file";
    public static final String KERBEROS_PRINCIPAL = CONF_PREFIX + "kerberos.principal";

    private ConcurrentMap<String, UserGroupInformation> userUgiMap;

    public void init(Configuration serviceConf) throws ServiceException {
        boolean kerberosAuthOn = serviceConf.getBoolean(KERBEROS_AUTH_ENABLED, true);
        XLog.getLog(getClass()).info("Oozie Kerberos Authentication [{0}]", (kerberosAuthOn) ? "enabled" : "disabled");
        if (kerberosAuthOn) {
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
        else {
            Configuration conf = new Configuration();
            conf.set("hadoop.security.authentication", "simple");
            UserGroupInformation.setConfiguration(conf);
        }
        userUgiMap = new ConcurrentHashMap<String, UserGroupInformation>();
    }

    public void destroy() {
        userUgiMap = null;
        super.destroy();
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
     * Return a JobClient created with the provided user/group.
     *
     * @param conf JobConf with all necessary information to create the JobClient.
     * @return JobClient created with the provided user/group.
     * @throws IOException if the client could not be created.
     */
    public JobClient createJobClient(String user, String group, final JobConf conf) throws IOException {
        ParamChecker.notEmpty(user, "user");
        ParamChecker.notEmpty(group, "group");
        try {
            UserGroupInformation ugi = getUGI(user);
            JobClient jobClient = ugi.doAs(new PrivilegedExceptionAction<JobClient>() {
                public JobClient run() throws Exception {
                    return new JobClient(conf);
                }
            });
            Token<DelegationTokenIdentifier> mrdt = jobClient.getDelegationToken(new Text("mr token"));
            conf.getCredentials().addToken( new Text("mr token"), mrdt);
            return jobClient;
        }
        catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Return a FileSystem created with the provided user/group.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws IOException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, final Configuration conf) throws IOException {
        ParamChecker.notEmpty(user, "user");
        ParamChecker.notEmpty(group, "group");
        try {
            UserGroupInformation ugi = getUGI(user);
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    Configuration defaultConf = new Configuration();
                    XConfiguration.copy(conf, defaultConf);
                    return FileSystem.get(defaultConf);
                }
            });
        }
        catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Return a FileSystem created with the provided user/group for the specified URI.
     *
     * @param uri file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws IOException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, final URI uri, final Configuration conf)
            throws IOException {
        ParamChecker.notEmpty(user, "user");
        ParamChecker.notEmpty(group, "group");
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
            throw new IOException(ex);
        }
    }



    public void addFileToClassPath(String user, String group, final Path file, final Configuration conf)
            throws IOException {
        ParamChecker.notEmpty(user, "user");
        ParamChecker.notEmpty(group, "group");
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
