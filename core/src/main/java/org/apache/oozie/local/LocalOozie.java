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

package org.apache.oozie.local;

import org.apache.oozie.CoordinatorEngine;
import org.apache.oozie.DagEngine;
import org.apache.oozie.LocalOozieClient;
import org.apache.oozie.LocalOozieClientCoord;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.CoordinatorEngineService;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.servlet.CallbackServlet;
import org.apache.oozie.test.EmbeddedServletContainer;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

/**
 * LocalOozie runs workflows in an embedded Oozie instance . <p/> LocalOozie is meant for development/debugging purposes
 * only.
 */
public class LocalOozie {
    private static EmbeddedServletContainer container;
    private static boolean localOozieActive = false;

    /**
     * Start LocalOozie.
     *
     * @throws Exception if LocalOozie could not be started.
     */
    public synchronized static void start() throws Exception {
        if (localOozieActive) {
            throw new IllegalStateException("LocalOozie is already initialized");
        }

        String log4jFile = System.getProperty(XLogService.LOG4J_FILE, null);
        String oozieLocalLog = System.getProperty("oozielocal.log", null);
        if (log4jFile == null) {
            System.setProperty(XLogService.LOG4J_FILE, "localoozie-log4j.properties");
        }
        if (oozieLocalLog == null) {
            System.setProperty("oozielocal.log", "./oozielocal.log");
        }

        localOozieActive = true;
        new Services().init();

        if (log4jFile != null) {
            System.setProperty(XLogService.LOG4J_FILE, log4jFile);
        }
        else {
            System.getProperties().remove(XLogService.LOG4J_FILE);
        }
        if (oozieLocalLog != null) {
            System.setProperty("oozielocal.log", oozieLocalLog);
        }
        else {
            System.getProperties().remove("oozielocal.log");
        }

        container = new EmbeddedServletContainer("oozie");
        container.addServletEndpoint("/callback", CallbackServlet.class);
        container.start();
        String callbackUrl = container.getServletURL("/callback");
        Services.get().getConf().set(CallbackService.CONF_BASE_URL, callbackUrl);
        XLog.getLog(LocalOozie.class).info("LocalOozie started callback set to [{0}]", callbackUrl);
    }

    /**
     * Stop LocalOozie.
     */
    public synchronized static void stop() {
        RuntimeException thrown = null;
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (RuntimeException ex) {
            thrown = ex;
        }
        container = null;
        XLog.getLog(LocalOozie.class).info("LocalOozie stopped");
        try {
            Services.get().destroy();
        }
        catch (RuntimeException ex) {
            if (thrown != null) {
                thrown = ex;
            }
        }
        localOozieActive = false;
        if (thrown != null) {
            throw thrown;
        }
    }

    /**
     * Return a {@link org.apache.oozie.client.OozieClient} for LocalOozie. <p/> The returned instance is configured
     * with the user name of the JVM (the value of the system property 'user.name'). <p/> The following methods of the
     * client are NOP in the returned instance: {@link org.apache.oozie.client.OozieClient#validateWSVersion}, {@link
     * org.apache.oozie.client.OozieClient#setHeader}, {@link org.apache.oozie.client.OozieClient#getHeader}, {@link
     * org.apache.oozie.client.OozieClient#removeHeader}, {@link org.apache.oozie.client.OozieClient#getHeaderNames} and
     * {@link org.apache.oozie.client.OozieClient#setSafeMode}.
     *
     * @return a {@link org.apache.oozie.client.OozieClient} for LocalOozie.
     */
    public static OozieClient getClient() {
        return getClient(System.getProperty("user.name"));
    }

    /**
     * Return a {@link org.apache.oozie.client.OozieClient} for LocalOozie.
     * <p/>
     * The returned instance is configured with the user name of the JVM (the
     * value of the system property 'user.name').
     * <p/>
     * The following methods of the client are NOP in the returned instance:
     * {@link org.apache.oozie.client.OozieClient#validateWSVersion},
     * {@link org.apache.oozie.client.OozieClient#setHeader},
     * {@link org.apache.oozie.client.OozieClient#getHeader},
     * {@link org.apache.oozie.client.OozieClient#removeHeader},
     * {@link org.apache.oozie.client.OozieClient#getHeaderNames} and
     * {@link org.apache.oozie.client.OozieClient#setSafeMode}.
     *
     * @return a {@link org.apache.oozie.client.OozieClient} for LocalOozie.
     */
    public static OozieClient getCoordClient() {
        return getClientCoord(System.getProperty("user.name"));
    }

    /**
     * Return a {@link org.apache.oozie.client.OozieClient} for LocalOozie configured for a given user.
     * <p/>
     * The following methods of the client are NOP in the returned instance: {@link org.apache.oozie.client.OozieClient#validateWSVersion},
     * {@link org.apache.oozie.client.OozieClient#setHeader}, {@link org.apache.oozie.client.OozieClient#getHeader}, {@link org.apache.oozie.client.OozieClient#removeHeader},
     * {@link org.apache.oozie.client.OozieClient#getHeaderNames} and {@link org.apache.oozie.client.OozieClient#setSafeMode}.
     *
     * @param user user name to use in LocalOozie for running workflows.
     * @return a {@link org.apache.oozie.client.OozieClient} for LocalOozie configured for the given user.
     */
    public static OozieClient getClient(String user) {
        if (!localOozieActive) {
            throw new IllegalStateException("LocalOozie is not initialized");
        }
        ParamChecker.notEmpty(user, "user");
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(user);
        return new LocalOozieClient(dagEngine);
    }

    /**
     * Return a {@link org.apache.oozie.client.OozieClient} for LocalOozie
     * configured for a given user.
     * <p/>
     * The following methods of the client are NOP in the returned instance:
     * {@link org.apache.oozie.client.OozieClient#validateWSVersion},
     * {@link org.apache.oozie.client.OozieClient#setHeader},
     * {@link org.apache.oozie.client.OozieClient#getHeader},
     * {@link org.apache.oozie.client.OozieClient#removeHeader},
     * {@link org.apache.oozie.client.OozieClient#getHeaderNames} and
     * {@link org.apache.oozie.client.OozieClient#setSafeMode}.
     *
     * @param user user name to use in LocalOozie for running coordinator.
     * @return a {@link org.apache.oozie.client.OozieClient} for LocalOozie
     *         configured for the given user.
     */
    public static OozieClient getClientCoord(String user) {
        if (!localOozieActive) {
            throw new IllegalStateException("LocalOozie is not initialized");
        }
        ParamChecker.notEmpty(user, "user");
        CoordinatorEngine coordEngine = Services.get().get(CoordinatorEngineService.class).getCoordinatorEngine(user);
        return new LocalOozieClientCoord(coordEngine);
    }

}
