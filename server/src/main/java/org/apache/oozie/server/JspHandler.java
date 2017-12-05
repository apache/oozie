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

import com.google.common.base.Preconditions;
import org.eclipse.jetty.annotations.ServletContainerInitializersStarter;
import org.eclipse.jetty.apache.jsp.JettyJasperInitializer;
import org.eclipse.jetty.jsp.JettyJspServlet;
import org.eclipse.jetty.plus.annotation.ContainerInitializer;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class that is used to handle JSP requests in Oozie server.
 */
public class JspHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JspHandler.class);
    private final File scratchDir;
    private final WebRootResourceLocator webRootResourceLocator;

    public JspHandler(final File scratchDir, final WebRootResourceLocator webRootResourceLocator) {
        this.scratchDir = scratchDir;
        this.webRootResourceLocator = webRootResourceLocator;
    }

    /**
     * Establish Scratch directory for the servlet context (used by JSP compilation)
     */
    private File getScratchDir() throws IOException
    {
        if (scratchDir.exists()) {
            LOG.info(String.format("Scratch directory exists and will be reused: %s", scratchDir.getAbsolutePath()));
            return scratchDir;
        }

        if (!scratchDir.mkdirs()) {
            throw new IOException("Unable to create scratch directory: " + scratchDir);
        }

        LOG.info(String.format("Scratch directory created: %s", scratchDir.getAbsolutePath()));
        return scratchDir;
    }

    /**
     * Setup the basic application "context" for this application at "/"
     * This is also known as the handler tree (in jetty speak)
     * @param servletContextHandler the context handler
     * @throws IOException in case of IO errors
     * @throws URISyntaxException if the server URI is not well formatted
     */
    public void setupWebAppContext(WebAppContext servletContextHandler)
            throws IOException, URISyntaxException
    {
        Preconditions.checkNotNull(servletContextHandler, "servletContextHandler is null");

        File scratchDir = getScratchDir();
        servletContextHandler.setAttribute("javax.servlet.context.tempdir", scratchDir);
        servletContextHandler.setAttribute("org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern",
                ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\.jar$|.*/.*taglibs.*\\.jar$");
        URI baseUri = webRootResourceLocator.getWebRootResourceUri();
        servletContextHandler.setResourceBase(baseUri.toASCIIString());
        servletContextHandler.setAttribute("org.eclipse.jetty.containerInitializers", jspInitializers());
        servletContextHandler.addBean(new ServletContainerInitializersStarter(servletContextHandler), true);
        servletContextHandler.setClassLoader(getUrlClassLoader());

        servletContextHandler.addServlet(jspServletHolder(), "*.jsp");

        servletContextHandler.addServlet(jspFileMappedServletHolder(), "/oozie/");
        servletContextHandler.addServlet(defaultServletHolder(baseUri), "/");
    }

    /**
     * Ensure the jsp engine is initialized correctly
     */
    private List<ContainerInitializer> jspInitializers()
    {
        JettyJasperInitializer sci = new JettyJasperInitializer();
        ContainerInitializer initializer = new ContainerInitializer(sci, null);
        List<ContainerInitializer> initializers = new ArrayList<>();
        initializers.add(initializer);
        return initializers;
    }

    /**
     * Set Classloader of Context to be sane (needed for JSTL)
     * JSP requires a non-System classloader, this simply wraps the
     * embedded System classloader in a way that makes it suitable
     * for JSP to use
     */
    private ClassLoader getUrlClassLoader()
    {
        ClassLoader jspClassLoader = new URLClassLoader(new URL[0], this.getClass().getClassLoader());
        return jspClassLoader;
    }

    /**
     * Create JSP Servlet (must be named "jsp")
     */
    private ServletHolder jspServletHolder()
    {
        ServletHolder holderJsp = new ServletHolder("jsp", JettyJspServlet.class);
        holderJsp.setInitOrder(0);
        holderJsp.setInitParameter("logVerbosityLevel", "DEBUG");
        holderJsp.setInitParameter("fork", "false");
        holderJsp.setInitParameter("xpoweredBy", "false");
        holderJsp.setInitParameter("compilerTargetVM", "1.7");
        holderJsp.setInitParameter("compilerSourceVM", "1.7");
        holderJsp.setInitParameter("keepgenerated", "true");
        return holderJsp;
    }

    /**
     * Create Example of mapping jsp to path spec
     */
    private ServletHolder jspFileMappedServletHolder()
    {
        ServletHolder holderAltMapping = new ServletHolder();
        holderAltMapping.setName("index.jsp");
        holderAltMapping.setForcedPath("/index.jsp");
        return holderAltMapping;
    }

    /**
     * Create Default Servlet (must be named "default")
     */
    private ServletHolder defaultServletHolder(URI baseUri)
    {
        ServletHolder holderDefault = new ServletHolder("default", DefaultServlet.class);
        holderDefault.setInitParameter("resourceBase", baseUri.toASCIIString());
        holderDefault.setInitParameter("dirAllowed", "true");
        return holderDefault;
    }
}
