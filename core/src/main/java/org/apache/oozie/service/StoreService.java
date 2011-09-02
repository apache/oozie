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
package org.apache.oozie.service;

import java.util.Properties;

import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.SLAStore;
import org.apache.oozie.store.Store;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.ErrorCode;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.rest.JsonCoordinatorJob;
import org.apache.oozie.client.rest.JsonWorkflowAction;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.client.rest.JsonSLAEvent;

/**
 * Base service for persistency of jobs and actions.
 */
public class StoreService implements Service, Instrumentable {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "StoreService.";
    private static final String INTSRUMENTATION_GROUP = "jdbc";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    ;
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";

    @SuppressWarnings("unchecked")
    private static EntityManagerFactory factory = Persistence.createEntityManagerFactory("oozie",
                                                                                         new java.util.HashMap());

    /**
     * Return instance of store.
     *
     * @return {@link Store}.
     */
    @SuppressWarnings("unchecked")
    public <S extends Store> S getStore(Class<S> klass) throws StoreException {
        if (WorkflowStore.class.equals(klass)) {
            return (S) Services.get().get(WorkflowStoreService.class).create();
        }
        else {
            if (CoordinatorStore.class.equals(klass)) {
                return (S) Services.get().get(CoordinatorStoreService.class).create();
            }
            else {
                if (SLAStore.class.equals(klass)) {
                    return (S) Services.get().get(SLAStoreService.class).create();
                }
            }
        }
        // to do add checks for other stores - coordinator and SLA stores
        throw new StoreException(ErrorCode.E0607, " can not get store StoreService.getStore(Class)");
    }

    /**
     * Return instance of store with an EntityManager pointing to an existing Store.
     *
     * @return {@link Store}.
     */
    @SuppressWarnings("unchecked")
    public <S extends Store, T extends Store> S getStore(Class<S> klass, T store) throws StoreException {
        if (WorkflowStore.class.equals(klass)) {
            return (S) Services.get().get(WorkflowStoreService.class).create(store);
        }
        else {
            if (CoordinatorStore.class.equals(klass)) {
                return (S) Services.get().get(CoordinatorStoreService.class).create(store);
            }
            else {
                if (SLAStore.class.equals(klass)) {
                    return (S) Services.get().get(SLAStoreService.class).create(store);
                }
            }
        }
        throw new StoreException(ErrorCode.E0607, " StoreService.getStore(Class, store)");
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link StoreService}.
     */
    public Class<? extends Service> getInterface() {
        return StoreService.class;
    }

    /**
     * Initializes the {@link StoreService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        Properties props = new Properties();
        props.put("url", conf.get(CONF_URL, "jdbc:hsqldb:mem:testdb"));
        props.put("driverClassName", conf.get(CONF_DRIVER, "org.hsqldb.jdbcDriver"));
        props.put("url", conf.get(CONF_URL, "jdbc:hsqldb:mem:testdb"));
        props.put("username", conf.get(CONF_USERNAME, "sa"));
        props.put("password", conf.get(CONF_PASSWORD, "").trim());
        props.put("maxActive", conf.get(CONF_MAX_ACTIVE_CONN, "10"));
        EntityManager entityManager = getEntityManager();
        entityManager.find(WorkflowActionBean.class, 1);
        entityManager.find(WorkflowJobBean.class, 1);
        entityManager.find(CoordinatorActionBean.class, 1);
        entityManager.find(CoordinatorJobBean.class, 1);
        entityManager.find(JsonWorkflowAction.class, 1);
        entityManager.find(JsonWorkflowJob.class, 1);
        entityManager.find(JsonCoordinatorAction.class, 1);
        entityManager.find(JsonCoordinatorJob.class, 1);
        entityManager.find(SLAEventBean.class, 1);
        entityManager.find(JsonSLAEvent.class, 1);
        XLog.getLog(getClass()).info(XLog.STD, "*** StoreService *** " + "Initialized all entities!");
        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized
        // one time only
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        XLog.getLog(getClass()).warn("StoreService initialized *** {0}",
                                     spi.getConfiguration().getConnectionProperties());
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    /**
     * Destroy the StoreService
     */
    public void destroy() {
    }

    public void instrument(Instrumentation instr) {
        /*
         * TO DO - sampler call never returns possibly because JPA holds a class
         * level lock - sampler runs from BasiCInstrumentedDataSource
         */
        /*
         * instrumentation = instr;
         * instrumentation.addSampler(INTSRUMENTATION_GROUP,
         * INSTR_ACTIVE_CONNECTIONS1_SAMPLER, 60, 1, new
         * Instrumentation.Variable<Long>() { public Long getValue() {
         * XLog.getLog(getClass()).warn("StoreService Instrumentation"); //
         * InstrumentedBasicDataSource dataSource = new
         * InstrumentedBasicDataSource(); //return (long) 100; // return
         * dataSource.getActiveConnections(); return
         * dataSource.getActiveConnections(); } });
         */
    }

    /**
     * for unit test only Return a raw direct JDBC connection. <p/> The connection is not from the connection pool. <p/>
     * The conection is not instrumented.
     *
     * @return a raw Direct JDBC connection.
     * @throws SQLException thrown if the connection could not be obtained.
     */
    public Connection getRawConnection() throws SQLException {
        String driver = Services.get().getConf().get(CONF_DRIVER, "org.hsqldb.jdbcDriver");
        String url = Services.get().getConf().get(CONF_URL, "jdbc:hsqldb:mem:testdb");
        String user = Services.get().getConf().get(CONF_USERNAME, "sa");
        String password = Services.get().getConf().get(CONF_PASSWORD, "").trim();
        try {
            Class.forName(driver);
        }
        catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Return EntityManager
     */
    public EntityManager getEntityManager() {
        return factory.createEntityManager();
    }
}
