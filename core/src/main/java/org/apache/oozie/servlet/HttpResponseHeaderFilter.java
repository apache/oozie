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

import com.google.common.annotations.VisibleForTesting;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Filter that adds headers to the HTTP response
 */
public class HttpResponseHeaderFilter implements Filter {
    @VisibleForTesting
    static final String X_FRAME_OPTIONS = "X-Frame-Options";
    @VisibleForTesting
    static final String X_FRAME_OPTION_DENY = "DENY";

    /**
     * Initializes the filter.
     * <p>
     * This implementation is a NOP.
     *
     * @param config filter configuration.
     *
     * @throws ServletException thrown if the filter could not be initialized.
     */
    @Override
    public void init(FilterConfig config) throws ServletException {
    }

    /**
     * Sets the X-Frame-Options response header
     *
     * @param request servlet request.
     * @param response servlet response.
     * @param chain filter chain.
     *
     * @throws IOException thrown if an IO error occurrs.
     * @throws ServletException thrown if a servet error occurrs.
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        chain.doFilter(request, response);
        if (response instanceof HttpServletResponse) {
            final HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.addHeader(X_FRAME_OPTIONS, X_FRAME_OPTION_DENY);
        }
    }

    /**
     * Destroys the filter.
     * <p>
     * This implementation is a NOP.
     */
    @Override
    public void destroy() {
    }
}
