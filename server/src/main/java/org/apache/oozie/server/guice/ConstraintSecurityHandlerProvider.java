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

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.server.EmbeddedOozieServer;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.util.security.Constraint;

import java.util.Arrays;

class ConstraintSecurityHandlerProvider implements Provider<ConstraintSecurityHandler> {
    private final Configuration oozieConfiguration;

    @Inject
    public ConstraintSecurityHandlerProvider(final Services oozieServices) {
        oozieConfiguration = oozieServices.get(ConfigurationService.class).getConf();
    }

    @Override
    public ConstraintSecurityHandler get() {
        String contextPath = EmbeddedOozieServer.getContextPath(oozieConfiguration);
        ConstraintMapping callbackConstraintMapping = new ConstraintMapping();
        callbackConstraintMapping.setPathSpec(String.format("%s/callback/*", contextPath));
        Constraint unsecureConstraint = new Constraint();
        unsecureConstraint.setDataConstraint(Constraint.DC_NONE);
        callbackConstraintMapping.setConstraint(unsecureConstraint);

        ConstraintMapping mapping = new ConstraintMapping();
        mapping.setPathSpec("/*");
        Constraint constraint = new Constraint();
        constraint.setDataConstraint(Constraint.DC_CONFIDENTIAL);
        mapping.setConstraint(constraint);

        ConstraintSecurityHandler security = new ConstraintSecurityHandler();
        security.setConstraintMappings(Arrays.asList(callbackConstraintMapping, mapping));
        return security;
    }
}
