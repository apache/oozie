/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;

import org.apache.oozie.util.IOUtils;
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
public class StoreService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "StoreService.";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";
    public static final String CONF_CREATE_DB_SCHEMA = CONF_PREFIX + "create.db.schema";

    private EntityManagerFactory factory;

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
        String url = conf.get(CONF_URL, "jdbc:hsqldb:mem:oozie;create=true");
        String driver = conf.get(CONF_DRIVER, "org.hsqldb.jdbcDriver");
        String user = conf.get(CONF_USERNAME, "sa");
        String password = conf.get(CONF_PASSWORD, "").trim();
        String maxConn = conf.get(CONF_MAX_ACTIVE_CONN, "10").trim();
        boolean autoSchemaCreation = conf.getBoolean(CONF_CREATE_DB_SCHEMA, true);

        if (!url.startsWith("jdbc:")) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, must start with 'jdbc:'");
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }
        dbType = dbType.substring(0, dbType.indexOf(":"));

        String persistentUnit = "oozie-" + dbType;

        //Checking existince of ORM file for DB type
        String ormFile = "META-INF/" + persistentUnit + "-orm.xml";
        try {
            IOUtils.getResourceAsStream(ormFile, -1);
        }
        catch (IOException ex) {
            throw new ServiceException(ErrorCode.E0609, dbType, ormFile);
        }

        String connProps = "DriverClassName={0},Url={1},Username={2},Password={3},MaxActive={4}";
        connProps = MessageFormat.format(connProps, driver, url, user, password, maxConn);
        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionProperties", connProps);
        if (autoSchemaCreation) {
            props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
        }

        factory = Persistence.createEntityManagerFactory(persistentUnit, props);

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

        XLog.getLog(getClass()).info(XLog.STD, "All entities initialized");
        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized
        // one time only
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        XLog.getLog(getClass()).info("JPA configuration: {0}", spi.getConfiguration().getConnectionProperties());
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    /**
     * Destroy the StoreService
     */
    public void destroy() {
        factory.close();
    }

    /**
     * Return EntityManager
     */
    public EntityManager getEntityManager() {
        return factory.createEntityManager();
    }
}
