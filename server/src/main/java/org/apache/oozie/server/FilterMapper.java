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

package org.apache.oozie.server;

import com.google.inject.Inject;
import org.apache.oozie.servlet.AuthFilter;
import org.apache.oozie.servlet.HttpResponseHeaderFilter;
import org.apache.oozie.servlet.HostnameFilter;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.Objects;

public class FilterMapper {
    private final WebAppContext servletContextHandler;

    @Inject
    public FilterMapper(final WebAppContext servletContextHandler) {
        this.servletContextHandler = Objects.requireNonNull(servletContextHandler, "ServletContextHandler is null");
    }

    /**
     *  Map filters to endpoints. Make sure it in sync with ServletMapper when making changes
     * */
    void addFilters() {
        mapFilter(new FilterHolder(new HostnameFilter()), "/*");
        mapFilter(new FilterHolder(new HttpResponseHeaderFilter()), "/*");

        FilterHolder authFilter = new FilterHolder(new AuthFilter());
        mapFilter(authFilter, "/versions/*");
        mapFilter(authFilter, "/v0/*");
        mapFilter(authFilter, "/v1/*");
        mapFilter(authFilter, "/v2/*");
        mapFilter(authFilter, "/admin/*");
        mapFilter(authFilter, "/docs/*");
        mapFilter(authFilter, "/error/*");
    }

    private void mapFilter(FilterHolder filterHolder, String pathSpec) {
        servletContextHandler.addFilter(filterHolder, pathSpec,
                EnumSet.of(DispatcherType.REQUEST, DispatcherType.FORWARD, DispatcherType.INCLUDE));
    }
}
