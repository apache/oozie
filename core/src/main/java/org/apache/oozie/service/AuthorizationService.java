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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

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
     * Configuration parameter to enable or disable Oozie admin role.
     */
    public static final String CONF_AUTHORIZATION_ENABLED = CONF_PREFIX + "authorization.enabled";

    /**
     * Configuration parameter to enable old behavior default group as ACL.
     */
    public static final String CONF_DEFAULT_GROUP_AS_ACL = CONF_PREFIX + "default.group.as.acl";

    /**
     * Configuration parameter to define admin groups, if NULL/empty the adminusers.txt file is used.
     */
    public static final String CONF_ADMIN_GROUPS = CONF_PREFIX + "admin.groups";

    /**
     * File that contains list of admin users for Oozie.
     */
    public static final String ADMIN_USERS_FILE = "adminusers.txt";

    protected static final String INSTRUMENTATION_GROUP = "authorization";
    protected static final String INSTR_FAILED_AUTH_COUNTER = "authorization.failed";

    private Set<String> adminGroups;
    private Set<String> adminUsers;
    private boolean authorizationEnabled;
    private boolean useDefaultGroupAsAcl;

    private final XLog log = XLog.getLog(getClass());
    private Instrumentation instrumentation;

    private String[] getTrimmedStrings(String str) {
        if (null == str || "".equals(str.trim())) {
            return new String[0];
        }
        return str.trim().split("\\s*,\\s*");
    }

    /**
     * Initialize the service. <p/> Reads the security related configuration. parameters - security enabled and list of
     * super users.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the service could not be initialized.
     */
    public void init(Services services) throws ServiceException {
        authorizationEnabled =
            ConfigUtils.getWithDeprecatedCheck(services.getConf(), CONF_AUTHORIZATION_ENABLED,
                                               CONF_SECURITY_ENABLED, false);
        if (authorizationEnabled) {
            log.info("Oozie running with authorization enabled");
            useDefaultGroupAsAcl = Services.get().getConf().getBoolean(CONF_DEFAULT_GROUP_AS_ACL, false);
            String[] str = getTrimmedStrings(Services.get().getConf().get(CONF_ADMIN_GROUPS));
            if (str.length > 0) {
                log.info("Admin users will be checked against the defined admin groups");
                adminGroups = new HashSet<String>();
                for (String s : str) {
                    adminGroups.add(s.trim());
                }
            }
            else {
                log.info("Admin users will be checked against the 'adminusers.txt' file contents");
                adminUsers = new HashSet<String>();
                loadAdminUsers();
            }
        }
        else {
            log.warn("Oozie running with authorization disabled");
        }
        instrumentation = Services.get().get(InstrumentationService.class).get();
    }

    /**
     * Return if security is enabled or not.
     *
     * @return if security is enabled or not.
     */
    @Deprecated
    public boolean isSecurityEnabled() {
        return authorizationEnabled;
    }

    public boolean useDefaultGroupAsAcl() {
        return useDefaultGroupAsAcl;
    }

    /**
     * Return if security is enabled or not.
     *
     * @return if security is enabled or not.
     */
    public boolean isAuthorizationEnabled() {
        return isSecurityEnabled();
    }

    /**
     * Load the list of admin users from {@link AuthorizationService#ADMIN_USERS_FILE} </p>
     *
     * @throws ServiceException if the admin user list could not be loaded.
     */
    private void loadAdminUsers() throws ServiceException {
        String configDir = Services.get().get(ConfigurationService.class).getConfigDir();
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
                    throw new ServiceException(ErrorCode.E0160, file.getAbsolutePath(), ex);
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
     * Check if the user belongs to the group or not.
     *
     * @param user user name.
     * @param group group name.
     * @return if the user belongs to the group or not.
     * @throws AuthorizationException thrown if the authorization query can not be performed.
     */
    protected boolean isUserInGroup(String user, String group) throws AuthorizationException {
        GroupsService groupsService = Services.get().get(GroupsService.class);
        try {
            return groupsService.getGroups(user).contains(group);
        }
        catch (IOException ex) {
            throw new AuthorizationException(ErrorCode.E0501, ex.getMessage(), ex);
        }
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
        if (authorizationEnabled && !isUserInGroup(user, group)) {
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
        try {
            return Services.get().get(GroupsService.class).getGroups(user).get(0);
        }
        catch (IOException ex) {
            throw new AuthorizationException(ErrorCode.E0501, ex.getMessage(), ex);
        }
    }

    /**
     * Check if the user has admin privileges. <p/> If admin is disabled it returns always <code>true</code>. <p/> If
     * admin is enabled it returns <code>true</code> if the user is in the <code>adminusers.txt</code> file.
     *
     * @param user user name.
     * @return if the user has admin privileges or not.
     */
    protected boolean isAdmin(String user) {
        boolean admin = false;
        if (adminUsers != null) {
            admin = adminUsers.contains(user);
        }
        else {
            for (String adminGroup : adminGroups) {
                try {
                    admin = isUserInGroup(user, adminGroup);
                    if (admin) {
                        break;
                    }
                }
                catch (AuthorizationException ex) {
                    log.warn("Admin check failed, " + ex.toString(), ex);
                    break;
                }
            }
        }
        return admin;
    }

    /**
     * Check if the user has admin privileges. <p/> Subclasses should override the {@link #isUserInGroup} method.
     *
     * @param user user name.
     * @param write indicates if the check is for read or write admin tasks (in this implementation this is ignored)
     * @throws AuthorizationException thrown if user does not have admin priviledges.
     */
    public void authorizeForAdmin(String user, boolean write) throws AuthorizationException {
        if (authorizationEnabled && write && !isAdmin(user)) {
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
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            URI uri = new Path(appPath).toUri();
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, fsConf);

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
        catch (HadoopAccessorException e) {
            throw new AuthorizationException(e);
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
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            URI uri = new Path(appPath).toUri();
            Configuration fsConf = has.createJobConf(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, fsConf);

            Path path = new Path(appPath);
            try {
                if (!fs.exists(path)) {
                    incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                    throw new AuthorizationException(ErrorCode.E0504, appPath);
                }
                if (conf.get(XOozieClient.IS_PROXY_SUBMISSION) == null) { // Only further check existence of job definition files for non proxy submission jobs;
                    if (!fs.isFile(path)) {
                        Path appXml = new Path(path, fileName);
                        if (!fs.exists(appXml)) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            throw new AuthorizationException(ErrorCode.E0505, appPath);
                        }
                        if (!fs.isFile(appXml)) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            throw new AuthorizationException(ErrorCode.E0506, appPath);
                        }
                        fs.open(appXml).close();
                    }
                }
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
        catch (HadoopAccessorException e) {
            throw new AuthorizationException(e);
        }
    }

    private boolean isUserInAcl(String user, String aclStr) throws IOException {
        boolean userInAcl = false;
        if (aclStr != null && aclStr.trim().length() > 0) {
            GroupsService groupsService = Services.get().get(GroupsService.class);
            String[] acl = aclStr.split(",");
            for (int i = 0; !userInAcl && i < acl.length; i++) {
                String aclItem = acl[i].trim();
                userInAcl = aclItem.equals(user) || groupsService.getGroups(user).equals(aclItem);
            }
        }
        return userInAcl;
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
        if (authorizationEnabled && write && !isAdmin(user)) {
            try {
                // handle workflow jobs
                if (jobId.endsWith("-W")) {
                    WorkflowJobBean jobBean = null;
                    JPAService jpaService = Services.get().get(JPAService.class);
                    if (jpaService != null) {
                        try {
                            jobBean = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
                        }
                        catch (JPAExecutorException je) {
                            throw new AuthorizationException(je);
                        }
                    }
                    else {
                        throw new AuthorizationException(ErrorCode.E0610);
                    }
                    if (jobBean != null && !jobBean.getUser().equals(user)) {
                        if (!isUserInAcl(user, jobBean.getGroup())) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            throw new AuthorizationException(ErrorCode.E0508, user, jobId);
                        }
                    }
                }
                // handle bundle jobs
                else if (jobId.endsWith("-B")){
                    BundleJobBean jobBean = null;
                    JPAService jpaService = Services.get().get(JPAService.class);
                    if (jpaService != null) {
                        try {
                            jobBean = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
                        }
                        catch (JPAExecutorException je) {
                            throw new AuthorizationException(je);
                        }
                    }
                    else {
                        throw new AuthorizationException(ErrorCode.E0610);
                    }
                    if (jobBean != null && !jobBean.getUser().equals(user)) {
                        if (!isUserInAcl(user, jobBean.getGroup())) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            throw new AuthorizationException(ErrorCode.E0509, user, jobId);
                        }
                    }
                }
                // handle coordinator jobs
                else {
                    CoordinatorJobBean jobBean = null;
                    JPAService jpaService = Services.get().get(JPAService.class);
                    if (jpaService != null) {
                        try {
                            jobBean = jpaService.execute(new CoordJobGetJPAExecutor(jobId));
                        }
                        catch (JPAExecutorException je) {
                            throw new AuthorizationException(je);
                        }
                    }
                    else {
                        throw new AuthorizationException(ErrorCode.E0610);
                    }
                    if (jobBean != null && !jobBean.getUser().equals(user)) {
                        if (!isUserInAcl(user, jobBean.getGroup())) {
                            incrCounter(INSTR_FAILED_AUTH_COUNTER, 1);
                            throw new AuthorizationException(ErrorCode.E0509, user, jobId);
                        }
                    }
                }
            }
            catch (IOException ex) {
                throw new AuthorizationException(ErrorCode.E0501, ex.getMessage(), ex);
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
