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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessControlException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The ProxyUserService checks if a user of a request has proxyuser privileges.
 * <p/>
 * This check is based on the following criteria:
 * <p/>
 * <ul>
 *     <li>The user of the request must be configured as proxy user in Oozie configuration.</li>
 *     <li>The user of the request must be making the request from a whitelisted host.</li>
 *     <li>The user of the request must be making the request on behalf of a user of a whitelisted group.</li>
 * </ul>
 * <p/>
 */
public class ProxyUserService implements Service {
    private static XLog LOG = XLog.getLog(ProxyUserService.class);

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "ProxyUserService.proxyuser.";
    public static final String GROUPS = ".groups";
    public static final String HOSTS = ".hosts";

    private Services services;
    private Map<String, Set<String>> proxyUserHosts = new HashMap<String, Set<String>>();
    private Map<String, Set<String>> proxyUserGroups = new HashMap<String, Set<String>>();

    /**
     * Returns the service interface.
     * @return <code>ProxyUserService</code>
     */
    @Override
    public Class<? extends Service> getInterface() {
        return ProxyUserService.class;
    }

    /**
     * Initializes the service.
     *
     * @param services services singleton initializing the service.
     * @throws ServiceException thrown if the service could not be configured correctly.
     */
    @Override
    public void init(Services services) throws ServiceException {
        this.services = services;
        for (Map.Entry<String, String> entry : services.getConf()) {
            String key = entry.getKey();
            if (key.startsWith(CONF_PREFIX) && key.endsWith(GROUPS)) {
                String proxyUser = key.substring(0, key.lastIndexOf(GROUPS));
                if (services.getConf().get(proxyUser + HOSTS) == null) {
                    throw new ServiceException(ErrorCode.E0551, CONF_PREFIX + proxyUser + HOSTS);
                }
                proxyUser = proxyUser.substring(CONF_PREFIX.length());
                String value = entry.getValue().trim();
                LOG.info("Loading proxyuser settings [{0}]=[{1}]", key, value);
                Set<String> values = null;
                if (!value.equals("*")) {
                    values = new HashSet<String>(Arrays.asList(value.split(",")));
                }
                proxyUserGroups.put(proxyUser, values);
            }
            if (key.startsWith(CONF_PREFIX) && key.endsWith(HOSTS)) {
                String proxyUser = key.substring(0, key.lastIndexOf(HOSTS));
                if (services.getConf().get(proxyUser + GROUPS) == null) {
                    throw new ServiceException(ErrorCode.E0551, CONF_PREFIX + proxyUser + GROUPS);
                }
                proxyUser = proxyUser.substring(CONF_PREFIX.length());
                String value = entry.getValue().trim();
                LOG.info("Loading proxyuser settings [{0}]=[{1}]", key, value);
                Set<String> values = null;
                if (!value.equals("*")) {
                    String[] hosts = value.split(",");
                    for (int i = 0; i < hosts.length; i++) {
                        String originalName = hosts[i];
                        try {
                            hosts[i] = normalizeHostname(originalName);
                        }
                        catch (Exception ex) {
                            throw new ServiceException(ErrorCode.E0550, originalName, ex.getMessage(), ex);
                        }
                        LOG.info("  Hostname, original [{0}], normalized [{1}]", originalName, hosts[i]);
                    }
                    values = new HashSet<String>(Arrays.asList(hosts));
                }
                proxyUserHosts.put(proxyUser, values);
            }
        }
    }

    /**
     * Verifies a proxyuser.
     *
     * @param proxyUser user name of the proxy user.
     * @param proxyHost host the proxy user is making the request from.
     * @param doAsUser user the proxy user is impersonating.
     * @throws IOException thrown if an error during the validation has occurred.
     * @throws AccessControlException thrown if the user is not allowed to perform the proxyuser request.
     */
    public void validate(String proxyUser, String proxyHost, String doAsUser) throws IOException,
        AccessControlException {
        ParamChecker.notEmpty(proxyUser, "proxyUser",
                "If you're attempting to use user-impersonation via a proxy user, please make sure that "
                + "oozie.service.ProxyUserService.proxyuser.#USER#.hosts and "
                + "oozie.service.ProxyUserService.proxyuser.#USER#.groups are configured correctly");
        ParamChecker.notEmpty(proxyHost, "proxyHost",
                "If you're attempting to use user-impersonation via a proxy user, please make sure that "
                + "oozie.service.ProxyUserService.proxyuser." + proxyUser + ".hosts and "
                + "oozie.service.ProxyUserService.proxyuser." + proxyUser + ".groups are configured correctly");
        ParamChecker.notEmpty(doAsUser, "doAsUser");
        LOG.debug("Authorization check proxyuser [{0}] host [{1}] doAs [{2}]",
                  new Object[]{proxyUser, proxyHost, doAsUser});
        if (proxyUserHosts.containsKey(proxyUser)) {
            proxyHost = normalizeHostname(proxyHost);
            validateRequestorHost(proxyUser, proxyHost, proxyUserHosts.get(proxyUser));
            validateGroup(proxyUser, doAsUser, proxyUserGroups.get(proxyUser));
        }
        else {
            throw new AccessControlException(MessageFormat.format("User [{0}] not defined as proxyuser", proxyUser));
        }
    }

    private void validateRequestorHost(String proxyUser, String hostname, Set<String> validHosts)
        throws IOException, AccessControlException {
        if (validHosts != null) {
            if (!validHosts.contains(hostname) && !validHosts.contains(normalizeHostname(hostname))) {
                throw new AccessControlException(MessageFormat.format("Unauthorized host [{0}] for proxyuser [{1}]",
                                                                      hostname, proxyUser));
            }
        }
    }

    private void validateGroup(String proxyUser, String user, Set<String> validGroups) throws IOException,
        AccessControlException {
        if (validGroups != null) {
            List<String> userGroups = services.get(GroupsService.class).getGroups(user);
            for (String g : validGroups) {
                if (userGroups.contains(g)) {
                    return;
                }
            }
            throw new AccessControlException(
                MessageFormat.format("Unauthorized proxyuser [{0}] for user [{1}], not in proxyuser groups",
                                     proxyUser, user));
        }
    }

    private String normalizeHostname(String name) {
        try {
            InetAddress address = InetAddress.getByName(name);
            return address.getCanonicalHostName();
        }
        catch (IOException ex) {
            throw new AccessControlException(MessageFormat.format("Could not resolve host [{0}], {1}", name,
                                                                  ex.getMessage()));
        }
    }

    /**
     * Destroys the service.
     */
    @Override
    public void destroy() {
    }

}
