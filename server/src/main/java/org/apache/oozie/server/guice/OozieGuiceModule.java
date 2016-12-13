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

package org.apache.oozie.server.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.apache.oozie.server.JspHandler;
import org.apache.oozie.service.Services;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class OozieGuiceModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Services.class).toProvider(ServicesProvider.class).in(Singleton.class);

        bind(Server.class).toProvider(JettyServerProvider.class).in(Singleton.class);

        bind(WebAppContext.class).in(Singleton.class);

        bind(ConstraintSecurityHandler.class).toProvider(ConstraintSecurityHandlerProvider.class).in(Singleton.class);

        bind(JspHandler.class).toProvider(JspHandlerProvider.class).in(Singleton.class);

        bind(RewriteHandler.class).toProvider(RewriteHandlerProvider.class).in(Singleton.class);
    }
}
