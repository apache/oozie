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
package org.apache.oozie.servlet;

import org.apache.oozie.service.AuthorizationService;

import org.apache.oozie.service.ProxyUserService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ForTestAuthorizationService;
import org.apache.oozie.service.ForTestWorkflowStoreService;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.test.XFsTestCase;

import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class DagServletTestCase extends XFsTestCase {
    private EmbeddedServletContainer container;
    private String servletPath;

    protected String getContextURL() {
        return container.getContextURL();
    }

    protected URL createURL(String servletPath, String resource, Map<String, String> parameters) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(container.getServletURL(servletPath));
        if (resource != null && resource.length() > 0) {
            sb.append("/").append(resource);
        }
        if (parameters.size() > 0) {
            String separator = "?";
            for (Map.Entry<String, String> param : parameters.entrySet()) {
                sb.append(separator).append(URLEncoder.encode(param.getKey(), "UTF-8")).append("=")
                        .append(URLEncoder.encode(param.getValue(), "UTF-8"));
                separator = "&";
            }
        }
        return new URL(sb.toString());
    }

    protected URL createURL(String resource, Map<String, String> parameters) throws Exception {
        return createURL(servletPath, resource, parameters);
    }

    @SuppressWarnings("unchecked")
    protected void runTest(String servletPath, Class servletClass, boolean securityEnabled, Callable<Void> assertions)
            throws Exception {
        runTest(new String[]{servletPath}, new Class[]{servletClass}, securityEnabled, assertions);
    }

    protected void runTest(String[] servletPath, Class[] servletClass, boolean securityEnabled,
                           Callable<Void> assertions) throws Exception {
        Services services = new Services();
        this.servletPath = servletPath[0];
        try {
            String proxyUser = getTestUser();
            services.getConf().set(ProxyUserService.CONF_PREFIX + proxyUser +
                                   ProxyUserService.HOSTS, "*");
            services.getConf().set(ProxyUserService.CONF_PREFIX + proxyUser +
                                   ProxyUserService.GROUPS, "*");
            services.init();
            services.getConf().setBoolean(AuthorizationService.CONF_SECURITY_ENABLED, securityEnabled);
            Services.get().setService(ForTestAuthorizationService.class);
            Services.get().setService(ForTestWorkflowStoreService.class);
            Services.get().setService(MockDagEngineService.class);
            Services.get().setService(MockCoordinatorEngineService.class);
            container = new EmbeddedServletContainer("oozie");
            for (int i = 0; i < servletPath.length; i++) {
                container.addServletEndpoint(servletPath[i], servletClass[i]);
            }
            container.addFilter("*", HostnameFilter.class);
            container.addFilter("*", AuthFilter.class);
            setSystemProperty("user.name", getTestUser());
            container.start();
            assertions.call();
        }
        finally {
            this.servletPath = null;
            if (container != null) {
                container.stop();
            }
            services.destroy();
            container = null;
        }
    }

}
