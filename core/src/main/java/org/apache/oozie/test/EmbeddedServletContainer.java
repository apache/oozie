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

package org.apache.oozie.test;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.servlet.Context;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Map;

/**
 * An embedded servlet container for testing purposes. <p/> It provides reduced functionality, it supports only
 * Servlets. <p/> The servlet container is started in a free port.
 */
public class EmbeddedServletContainer {
    private Server server;
    private String host = null;
    private int port = -1;
    private String contextPath;
    Context context;

    /**
     * Create a servlet container.
     *
     * @param contextPath context path for the servlet, it must not be prefixed or append with "/", for the default
     * context use ""
     */
    public EmbeddedServletContainer(String contextPath) {
        this.contextPath = contextPath;
        server = new Server(0);
        context = new Context();
        context.setContextPath("/" + contextPath);
        server.setHandler(context);
    }

    /**
     * Add a servlet to the container.
     *
     * @param servletPath servlet path for the servlet, it should be prefixed with '/", it may contain a wild card at
     * the end.
     * @param servletClass servlet class
     * @param initParams a mapping of init parameters for the servlet, or null
     */
    public void addServletEndpoint(String servletPath, Class servletClass, Map<String, String> initParams) {
        ServletHolder s = new ServletHolder(servletClass);
        context.addServlet(s, servletPath);
        if (initParams != null) {
            s.setInitParameters(initParams);
        }
    }

    /**
     * Add a servlet to the container.
     *
     * @param servletPath servlet path for the servlet, it should be prefixed with '/", it may contain a wild card at
     * the end.
     * @param servletClass servlet class
     */
    public void addServletEndpoint(String servletPath, Class servletClass) {
        addServletEndpoint(servletPath, servletClass, null);
    }

    /**
     * Add a filter to the container.
     *
     * @param filterPath path for the filter, it should be prefixed with '/", it may contain a wild card at
     * the end.
     * @param filterClass servlet class
     */
    public void addFilter(String filterPath, Class filterClass) {
        context.addFilter(new FilterHolder(filterClass), filterPath, 0);
    }

    /**
     * Start the servlet container. <p/> The container starts on a free port.
     *
     * @throws Exception thrown if the container could not start.
     */
    public void start() throws Exception {
        host = InetAddress.getLocalHost().getHostName();
        ServerSocket ss = new ServerSocket(0);
        port = ss.getLocalPort();
        ss.close();
        server.getConnectors()[0].setHost(host);
        server.getConnectors()[0].setPort(port);
        server.start();
        System.out.println("Running embedded servlet container at: http://" + host + ":" + port);
    }

    /**
     * Return the hostname the servlet container is bound to.
     *
     * @return the hostname.
     */
    public String getHost() {
        return host;
    }

    /**
     * Return the port number the servlet container is bound to.
     *
     * @return the port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Return the full URL (including protocol, host, port, context path, servlet path) for the context path.
     *
     * @return URL to the context path.
     */
    public String getContextURL() {
        return "http://" + host + ":" + port + "/" + contextPath;
    }

    /**
     * Return the full URL (including protocol, host, port, context path, servlet path) for a servlet path.
     *
     * @param servletPath the path which will be expanded to a full URL.
     * @return URL to the servlet.
     */
    public String getServletURL(String servletPath) {
        String path = servletPath;
        if (path.endsWith("*")) {
            path = path.substring(0, path.length() - 1);
        }
        return getContextURL() + path;
    }

    /**
     * Stop the servlet container.
     */
    public void stop() {
        try {
            server.stop();
        }
        catch (Exception e) {
            // ignore exception
        }

        try {
            server.destroy();
        }
        catch (Exception e) {
            // ignore exception
        }

        host = null;
        port = -1;
    }

}
