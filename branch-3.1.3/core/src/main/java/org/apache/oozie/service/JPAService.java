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
package org.apache.oozie.service;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.rest.JsonBundleJob;
import org.apache.oozie.client.rest.JsonCoordinatorAction;
import org.apache.oozie.client.rest.JsonCoordinatorJob;
import org.apache.oozie.client.rest.JsonSLAEvent;
import org.apache.oozie.client.rest.JsonWorkflowAction;
import org.apache.oozie.client.rest.JsonWorkflowJob;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;

/**
 * Service that manages JPA and executes {@link JPAExecutor}.
 */
public class JPAService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP = "jpa";

    public static final String CONF_DB_SCHEMA = "oozie.db.schema.name";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JPAService.";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_CONN_DATA_SOURCE = CONF_PREFIX + "connection.data.source";

    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";
    public static final String CONF_CREATE_DB_SCHEMA = CONF_PREFIX + "create.db.schema";
    public static final String CONF_VALIDATE_DB_CONN = CONF_PREFIX + "validate.db.connection";
    public static final String CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL = CONF_PREFIX + "validate.db.connection.eviction.interval";
    public static final String CONF_VALIDATE_DB_CONN_EVICTION_NUM = CONF_PREFIX + "validate.db.connection.eviction.num";


    private EntityManagerFactory factory;
    private Instrumentation instr;

    private static XLog LOG;

    /**
     * Return the public interface of the service.
     *
     * @return {@link JPAService}.
     */
    public Class<? extends Service> getInterface() {
        return JPAService.class;
    }

    @Override
    public void instrument(Instrumentation instr) {
        this.instr = instr;
    }

    /**
     * Initializes the {@link JPAService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(JPAService.class);
        Configuration conf = services.getConf();
        String dbSchema = conf.get(CONF_DB_SCHEMA, "oozie");
        String url = conf.get(CONF_URL, "jdbc:derby:${oozie.home.dir}/${oozie.db.schema.name}-db;create=true");
        String driver = conf.get(CONF_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver");
        String user = conf.get(CONF_USERNAME, "sa");
        String password = conf.get(CONF_PASSWORD, "").trim();
        String maxConn = conf.get(CONF_MAX_ACTIVE_CONN, "10").trim();
        String dataSource = conf.get(CONF_CONN_DATA_SOURCE, "org.apache.commons.dbcp.BasicDataSource");
        boolean autoSchemaCreation = conf.getBoolean(CONF_CREATE_DB_SCHEMA, true);
        boolean validateDbConn = conf.getBoolean(CONF_VALIDATE_DB_CONN, false);
        String evictionInterval = conf.get(CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL, "300000").trim();
        String evictionNum = conf.get(CONF_VALIDATE_DB_CONN_EVICTION_NUM, "10").trim();

        if (!url.startsWith("jdbc:")) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, must start with 'jdbc:'");
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }
        dbType = dbType.substring(0, dbType.indexOf(":"));

        String persistentUnit = "oozie-" + dbType;

        // Checking existince of ORM file for DB type
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
        if (autoSchemaCreation) {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
            props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
        }
        else if (validateDbConn) {
            // validation can be done only if the schema already exist, else a
            // connection cannot be obtained to create the schema.
            String interval = "timeBetweenEvictionRunsMillis=" + evictionInterval;
            String num = "numTestsPerEvictionRun=" + evictionNum;
            connProps += ",TestOnBorrow=true,TestOnReturn=true,TestWhileIdle=true," + interval + "," + num;
            connProps += ",ValidationQuery=select count(*) from VALIDATE_CONN";
            connProps = MessageFormat.format(connProps, dbSchema);
        }
        else {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
        }
        props.setProperty("openjpa.ConnectionProperties", connProps);

        props.setProperty("openjpa.ConnectionDriverName", dataSource);

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
        entityManager.find(BundleJobBean.class, 1);
        entityManager.find(JsonBundleJob.class, 1);
        entityManager.find(BundleActionBean.class, 1);

        LOG.info(XLog.STD, "All entities initialized");
        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized one time only
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        LOG.info("JPA configuration: {0}", spi.getConfiguration().getConnectionProperties());
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    /**
     * Destroy the JPAService
     */
    public void destroy() {
        if (factory != null && factory.isOpen()) {
            factory.close();
        }
    }

    /**
     * Execute a {@link JPAExecutor}.
     *
     * @param executor JPAExecutor to execute.
     * @return return value of the JPAExecutor.
     * @throws JPAExecutorException thrown if an jpa executor failed
     */
    public <T> T execute(JPAExecutor<T> executor) throws JPAExecutorException {
        EntityManager em = getEntityManager();
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {
            LOG.trace("Executing JPAExecutor [{0}]", executor.getName());
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP, executor.getName(), 1);
            }
            cron.start();
            em.getTransaction().begin();
            T t = executor.execute(em);
            if (em.getTransaction().isActive()) {
                if (FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection")) {
                    throw new RuntimeException("Skipping Commit for Failover Testing");
                }

                em.getTransaction().commit();
            }
            return t;
        }
        finally {
            cron.stop();
            if (instr != null) {
                instr.addCron(INSTRUMENTATION_GROUP, executor.getName(), cron);
            }
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("JPAExecutor [{0}] ended with an active transaction, rolling back", executor.getName());
                    em.getTransaction().rollback();
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not check/rollback transaction after JPAExecutor [{0}], {1}", executor.getName(), ex
                        .getMessage(), ex);
            }
            try {
                if (em.isOpen()) {
                    em.close();
                }
                else {
                    LOG.warn("JPAExecutor [{0}] closed the EntityManager, it should not!", executor.getName());
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not close EntityManager after JPAExecutor [{0}], {1}", executor.getName(), ex
                        .getMessage(), ex);
            }
        }
    }

    /**
     * Return an EntityManager. Used by the StoreService. Once the StoreService is removed this method must be removed.
     *
     * @return an entity manager
     */
    EntityManager getEntityManager() {
        return factory.createEntityManager();
    }

}
