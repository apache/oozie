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

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.Services;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Authentication filter that extends Hadoop-auth AuthenticationFilter to override
 * the configuration loading.
 */
public class AuthFilter extends AuthenticationFilter {
    private static final String OOZIE_PREFIX = "oozie.authentication.";

    private HttpServlet optionsServlet;

    /**
     * Initialize the filter.
     *
     * @param filterConfig filter configuration.
     * @throws ServletException thrown if the filter could not be initialized.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        optionsServlet = new HttpServlet() {};
        optionsServlet.init();
    }

    /**
     * Destroy the filter.
     */
    @Override
    public void destroy() {
        optionsServlet.destroy();
        super.destroy();
    }

    /**
     * Returns the configuration from Oozie configuration to be used by the authentication filter.
     * <p/>
     * All properties from Oozie configuration which name starts with {@link #OOZIE_PREFIX} will
     * be returned. The keys of the returned properties are trimmed from the {@link #OOZIE_PREFIX}
     * prefix, for example the Oozie configuration property name 'oozie.authentication.type' will
     * be just 'type'.
     *
     * @param configPrefix configuration prefix, this parameter is ignored by this implementation.
     * @param filterConfig filter configuration, this parameter is ignored by this implementation.
     * @return all Oozie configuration properties prefixed with {@link #OOZIE_PREFIX}, without the
     * prefix.
     */
    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) {
        Properties props = new Properties();
        Configuration conf = Services.get().getConf();

        //setting the cookie path to root '/' so it is used for all resources.
        props.setProperty(AuthenticationFilter.COOKIE_PATH, "/");

        for (Map.Entry<String, String> entry : conf) {
            String name = entry.getKey();
            if (name.startsWith(OOZIE_PREFIX)) {
                String value = conf.get(name);
                name = name.substring(OOZIE_PREFIX.length());
                props.setProperty(name, value);
            }
        }

        return props;
    }

    /**
     * Enforces authentication using Hadoop-auth AuthenticationFilter.
     * <p/>
     * This method is overriden to respond to HTTP OPTIONS requests for authenticated calls, regardless
     * of the target servlet supporting OPTIONS or not and to inject the authenticated user name as
     * request attribute for Oozie to retrieve the user id.
     *
     * @param request http request.
     * @param response http response.
     * @param filterChain filter chain.
     * @throws IOException thrown if an IO error occurs.
     * @throws ServletException thrown if a servlet error occurs.
     */
    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain filterChain)
            throws IOException, ServletException {

        FilterChain filterChainWrapper = new FilterChain() {
            @Override
            public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
                    throws IOException, ServletException {
                HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
                if (httpRequest.getMethod().equals("OPTIONS")) {
                    optionsServlet.service(request, response);
                }
                else {
                  httpRequest.setAttribute(JsonRestServlet.USER_NAME, httpRequest.getRemoteUser());
                  filterChain.doFilter(servletRequest, servletResponse);
                }
            }
        };

        super.doFilter(request, response, filterChainWrapper);
    }

}
