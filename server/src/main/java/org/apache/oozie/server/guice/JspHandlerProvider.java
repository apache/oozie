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
import org.apache.oozie.server.JspHandler;
import org.apache.oozie.server.WebRootResourceLocator;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;

import java.io.File;

public class JspHandlerProvider implements Provider<JspHandler> {
    public static final String OOZIE_JSP_TMP_DIR = "oozie.jsp.tmp.dir";
    public static final String EMBEDDED_JETTY_JSP_DIR = "embedded-jetty-jsp";
    private final Configuration oozieConfiguration;

    @Inject
    public JspHandlerProvider(final Services oozieServices) {
        oozieConfiguration = oozieServices.get(ConfigurationService.class).getConf();
    }

    @Override
    public JspHandler get() {
        final File tempDir = new File(oozieConfiguration.get(OOZIE_JSP_TMP_DIR), EMBEDDED_JETTY_JSP_DIR);

        return new JspHandler(tempDir, new WebRootResourceLocator());
    }
}
