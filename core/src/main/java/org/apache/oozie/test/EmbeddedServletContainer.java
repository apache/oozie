/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.test;

import org.mortbay.http.HttpContext;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;

import java.net.InetAddress;

/**
 * An embedded servlet container for testing purposes.
 * <p/>
 * It provides reduced functionality, it supports only Servlets.
 * <p/>
 * The servlet container is started in a free port.
 */
public class EmbeddedServletContainer {
    private Server webServer;
    private String host = null;
    private int port = -1;
    private String contextPath;
    HttpContext context;

    /**
     * Create a servlet container.
     *
     * @param contextPath  context path for the servlet, it must not be prefixed or append with
     *                     "/", for the default context use ""
     */
    public EmbeddedServletContainer(String contextPath) {
        this.contextPath = contextPath;
        //TODO figure out how to bind the current IP instead loopback IP
        webServer = new org.mortbay.jetty.Server();

        // create web app context
        context = new HttpContext();
        context.setContextPath(contextPath + "/*");

        // bind context to servlet engine
        webServer.addContext(context);
    }

    /**
     * Add a servlet to the container.
     *
     * @param servletPath  servlet path for the servlet, it should be prefixed with '/", it may
     *                     contain a wild card at the end.
     * @param servletClass servlet class
     */
    public void addServletEndpoint(String servletPath, Class servletClass) {
        // create servlet decision
        ServletHandler handler = new ServletHandler();
        handler.addServlet(servletClass.getName(), servletPath, servletClass.getName());

        // bind servlet decision to context
        context.addHandler(handler);
    }

    /**
     * Start the servlet container.
     * <p/>
     * The container starts on a free port.
     *
     * @throws Exception thrown if the container could not start.
     */
    public void start()  throws Exception {
        SocketListener listener = new SocketListener();
        listener.setPort(0);
        // using IP, if we use name the fully qualified domain is not coming.
        listener.setHost(InetAddress.getLocalHost().getHostAddress());
        webServer.addListener(listener);

        webServer.start();
        port = listener.getPort();
        host = listener.getHost();
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
        webServer.stop();
        }
        catch (Exception e) {
            // ignore exception
        }

        try {
            webServer.destroy();
        }
        catch (Exception e) {
            // ignore exception
        }

        host = null;
        port = -1;
    }

}