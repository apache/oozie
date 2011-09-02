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
package org.apache.oozie.service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;

/**
 * The authorization service provides all authorization checks.
 */
public class AuthorizationService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "AuthorizationService.";

    /**
     * Configuration parameter to enable or disable Oozie admin role.
     */
    public static final String CONF_SECURITY_ENABLED = CONF_PREFIX + "security.enabled";

    /**
     * File that contains list of admin users for Oozie.
     */
    public static final String ADMIN_USERS_FILE = "adminusers.txt";

    /**
     * Default group returned by getDefaultGroup().
     */
    public static final String DEFAULT_GROUP = "users";

    protected static final String INSTRUMENTATION_GROUP = "authorization";
    protected static final String INSTR_FAILED_AUTH_COUNTER = "authorization.failed";

    private Set<String> adminUsers;
    private boolean securityEnabled;

    private final XLog log = XLog.getLog(getClass());
    private Instrumentation instrumentation;

    /**
     * Initialize the service. <p/> Reads the security related configuration. parameters - security enabled and list of
     * super users.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        adminUsers = new HashSet<String>();
        securityEnabled = services.getConf().getBoolean(CONF_SECURITY_ENABLED, false);
        instrumentation = Services.get().get(InstrumentationService.class).get();
        if (securityEnabled) {
            log.info("Oozie running with security enabled");
            loadAdminUsers();
        }
        else {
            log.warn("Oozie running with security disabled");
        }
    }

    /**
     * Return if security is enabled or not.
     *
     * @return if security is enabled or not.
     */
    public boolean isSecurityEnabled() {
        return securityEnabled;
    }

    /**
     * Load the list of admin users from {@link AuthorizationService#ADMIN_USERS_FILE} </p>
     *
     * @throws ServiceException if the admin user list could not be loaded.
     */
    private void loadAdminUsers() throws ServiceException {
        String configDir = System.getProperty(ConfigurationService.CONFIG_PATH);
        if (configDir != null) {
            File file = new File(configDir, ADMIN_USERS_FILE);
            if (file.exists()) {
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    try {
                        String line = br.readLine();
                        while (line != null) {
                            line = line.trim();
                            if (line.length() > 0 && !line.startsWith("#")) {
                                adminUsers.add(line);
                            }
                            line = br.readLine();
                        }
                    }
                    catch (IOException ex) {
                        throw new ServiceException(ErrorCode.E0160, file.getAbsolutePath(), ex);
                    }
                }
                catch (FileNotFoundException ex) {
                    throw new ServiceException(ErrorCode.E0160, ex);
                }
            }
            else {
                log.warn("Admin users file not available in config dir [{0}], running without admin users", configDir);
            }
        }
        else {
            log.warn("Reading configuration from classpath, running without admin users");
        }
    }

    /**
     * Destroy the service. <p/> This implementation does a NOP.
     */
    public void destroy() {
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link AuthorizationService}.
     */
    public Class<? extends Service> getInterface() {
        return AuthorizationService.class;
    }

    /**
     * Check if the user belongs to the group or not. <p/> This implementation returns always <code>true</code>.
     *
     * @param user user name.
     * @param group group name.
     * @return if the user belongs to the group or not.
     * @throws AuthorizationException thrown if the authorization query can not be performed.
     */
    protected boolean isUserInGroup(String user, String group) throws AuthorizationException {
        return true;
    }

    /**
     * Check if the user belongs to the group or not. <p/> <p/> Subclasses should override the {@link #isUserInGroup}
     * method.
     *
     * @param user user name.
     * @param group group name.
     * @throws AuthorizationException thrown if the user is not authorized for the group or if the authorization query
     * can not be performed.
     */
    public void authorizeForGroup(String user, String group) throws AuthorizationException {
        if (securityEnabled && !isUserInGroup(user, group)) {
            throw new AuthorizationException(ErrorCode.E0502, user, group);
        }
    }

    /**
     * Return the default group to which the user belongs. <p/> This implementation always returns 'users'.
     *
     * @param user user name.
     * @return default group of user.
     * @throws AuthorizationException thrown if the default group con not be retrieved.
     */
    public String getDefaultGroup(String user) throws AuthorizationException {
        return DEFAULT_GROUP;
    }

    /**
     * Check if the user has admin privileges. <p/> If admin is disabled it returns always <code>true</code>. <p/> If
     * admin is enabled it returns <code>true</code> if the user is in the <code>adminusers.txt</code> file.
     *
     * @param user user name.
     * @return if the user has admin privileges or not.
     */
    protected boolean isAdmin(String user) {
        return adminUsers.contains(user);
    }

    /**
     * Check if the user has admin privileges. <p/> Subclasses should override the {@link #isUserInGroup} method.
     *
     * @param user user name.
     * @param write indicates if the check is for read or write admin tasks (in this implementation this is ignored)
     * @throws AuthorizationException thrown if user does not have admin priviledges.
     */
    public void authorizeForAdmin(String user, boolean write) throws AuthorizationException {
        if (securityEnabled && write && !isAdmin(user)) {
            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
            throw new AuthorizationException(ErrorCode.E0503, user);
        }
    }

    /**
     * Check if the user+group is authorized to use the specified application. <p/> The check is done by checking the
     * file system permissions on the workflow application.
     *
     * @param user user name.
     * @param group group name.
     * @param appPath application path.
     * @throws AuthorizationException thrown if the user is not authorized for the app.
     */
    public void authorizeForApp(String user, String group, String appPath, Configuration jobConf)
            throws AuthorizationException {
        try {
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group,
                                                                                             new Path(appPath).toUri(), jobConf);

            Path path = new Path(appPath);
            try {
                if (!fs.exists(path)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0504, appPath);
                }
                Path wfXml = new Path(path, "workflow.xml");
                if (!fs.exists(wfXml)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0505, appPath);
                }
                if (!fs.isFile(wfXml)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0506, appPath);
                }
                fs.open(wfXml).close();
            }
            // TODO change this when stopping support of 0.18 to the new
            // Exception
            catch (org.apache.hadoop.fs.permission.AccessControlException ex) {
                incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                throw new AuthorizationException(ErrorCode.E0507, appPath, ex.getMessage(), ex);
            }
        }
        catch (IOException ex) {
            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
            throw new AuthorizationException(ErrorCode.E0501, ex.getMessage(), ex);
        }
    }

    /**
     * Check if the user+group is authorized to use the specified application. <p/> The check is done by checking the
     * file system permissions on the workflow application.
     *
     * @param user user name.
     * @param group group name.
     * @param appPath application path.
     * @param fileName workflow or coordinator.xml
     * @param conf
     * @throws AuthorizationException thrown if the user is not authorized for the app.
     */
    public void authorizeForApp(String user, String group, String appPath, String fileName, Configuration conf)
            throws AuthorizationException {
        try {
            //Configuration conf = new Configuration();
            //conf.set("user.name", user);
            // TODO Temporary fix till
            // https://issues.apache.org/jira/browse/HADOOP-4875 is resolved.
            //conf.set("hadoop.job.ugi", user + "," + group);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group,
                                                                                             new Path(appPath).toUri(), conf);
            Path path = new Path(appPath);
            try {
                if (!fs.exists(path)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0504, appPath);
                }
                Path wfXml = new Path(path, fileName);
                if (!fs.exists(wfXml)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0505, appPath);
                }
                if (!fs.isFile(wfXml)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0506, appPath);
                }
                fs.open(wfXml).close();
            }
            // TODO change this when stopping support of 0.18 to the new
            // Exception
            catch (org.apache.hadoop.fs.permission.AccessControlException ex) {
                incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                throw new AuthorizationException(ErrorCode.E0507, appPath, ex.getMessage(), ex);
            }
        }
        catch (IOException ex) {
            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
            throw new AuthorizationException(ErrorCode.E0501, ex.getMessage(), ex);
        }
    }

    /**
     * Check if the user+group is authorized to operate on the specified job. <p/> Checks if the user is a super-user or
     * the one who started the job. <p/> Read operations are allowed to all users.
     *
     * @param user user name.
     * @param jobId job id.
     * @param write indicates if the check is for read or write job tasks.
     * @throws AuthorizationException thrown if the user is not authorized for the job.
     */
    public void authorizeForJob(String user, String jobId, boolean write) throws AuthorizationException {
        if (securityEnabled && write && !isAdmin(user)) {
            // handle workflow jobs
            if (jobId.endsWith("-W")) {
                WorkflowJobBean jobBean;
                WorkflowStore store = null;
                try {
                    store = Services.get().get(WorkflowStoreService.class).create();
                    store.beginTrx();
                    jobBean = store.getWorkflow(jobId, false);
                    store.commitTrx();
                }
                catch (StoreException ex) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    if (store != null) {
                        store.rollbackTrx();
                    }
                    throw new AuthorizationException(ex);
                }
                finally {
                    if (store != null) {
                        try {
                            store.closeTrx();
                        }
                        catch (RuntimeException rex) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            log.error("Exception while attempting to close store", rex);
                        }
                    }
                }
                if (!jobBean.getUser().equals(user)) {
                    if (!isUserInGroup(user, jobBean.getGroup())) {
                        incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                        throw new AuthorizationException(ErrorCode.E0508, user, jobId);
                    }
                }
            }
            // handle coordinator jobs
            else {
                CoordinatorJobBean jobBean;
                CoordinatorStore store = null;
                try {
                    store = Services.get().get(CoordinatorStoreService.class).create();
                    store.beginTrx();
                    jobBean = store.getCoordinatorJob(jobId, false);
                    store.commitTrx();
                }
                catch (StoreException ex) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    if (store != null) {
                        store.rollbackTrx();
                    }
                    throw new AuthorizationException(ex);
                }
                finally {
                    if (store != null) {
                        try {
                            store.closeTrx();
                        }
                        catch (RuntimeException rex) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            log.error("Exception while attempting to close store", rex);
                        }
                    }
                }
                if (!jobBean.getUser().equals(user)) {
                    if (!isUserInGroup(user, jobBean.getGroup())) {
                        incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                        throw new AuthorizationException(ErrorCode.E0509, user, jobId);
                    }
                }
            }
        }
    }

    /**
     * Convenience method for instrumentation counters.
     *
     * @param name counter name.
     * @param count count to increment the counter.
     */
    private void incrCounter(String name, int count) {
        if (instrumentation != null) {
            instrumentation.incr(INSTRUMENTATION_GROUP, name, count);
        }
    }
}
