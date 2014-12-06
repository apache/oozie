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
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;

import org.apache.commons.dbcp.BasicDataSource;
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
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonSLAEvent;
import org.apache.oozie.compression.CodecFactory;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.openjpa.lib.jdbc.DecoratingDataSource;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;

/**
 * Service that manages JPA and executes {@link JPAExecutor}.
 */
@SuppressWarnings("deprecation")
public class JPAService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP_JPA = "jpa";

    public static final String CONF_DB_SCHEMA = "oozie.db.schema.name";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JPAService.";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_CONN_DATA_SOURCE = CONF_PREFIX + "connection.data.source";
    public static final String CONF_CONN_PROPERTIES = CONF_PREFIX + "connection.properties";
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

        final BasicDataSource dataSource = getBasicDataSource();
        if (dataSource != null) {
            instr.addSampler("jdbc", "connections.active", 60, 1, new Instrumentation.Variable<Long>() {
                @Override
                public Long getValue() {
                    return (long) dataSource.getNumActive();
                }
            });
            instr.addSampler("jdbc", "connections.idle", 60, 1, new Instrumentation.Variable<Long>() {
                @Override
                public Long getValue() {
                    return (long) dataSource.getNumIdle();
                }
            });
        }
    }

    private BasicDataSource getBasicDataSource() {
        // Get the BasicDataSource object; it could be wrapped in a DecoratingDataSource
        // It might also not be a BasicDataSource if the user configured something different
        BasicDataSource basicDataSource = null;
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        Object connectionFactory = spi.getConfiguration().getConnectionFactory();
        if (connectionFactory instanceof DecoratingDataSource) {
            DecoratingDataSource decoratingDataSource = (DecoratingDataSource) connectionFactory;
            basicDataSource = (BasicDataSource) decoratingDataSource.getInnermostDelegate();
        } else if (connectionFactory instanceof BasicDataSource) {
            basicDataSource = (BasicDataSource) connectionFactory;
        }
        return basicDataSource;
    }

    /**
     * Initializes the {@link JPAService}.
     *
     * @param services services instance.
     */
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(JPAService.class);
        Configuration conf = services.getConf();
        String dbSchema = ConfigurationService.get(conf, CONF_DB_SCHEMA);
        String url = ConfigurationService.get(conf, CONF_URL);
        String driver = ConfigurationService.get(conf, CONF_DRIVER);
        String user = ConfigurationService.get(conf, CONF_USERNAME);
        String password = ConfigurationService.get(conf, CONF_PASSWORD).trim();
        String maxConn = ConfigurationService.get(conf, CONF_MAX_ACTIVE_CONN).trim();
        String dataSource = ConfigurationService.get(conf, CONF_CONN_DATA_SOURCE);
        String connPropsConfig = ConfigurationService.get(conf, CONF_CONN_PROPERTIES);
        boolean autoSchemaCreation = ConfigurationService.getBoolean(conf, CONF_CREATE_DB_SCHEMA);
        boolean validateDbConn = ConfigurationService.getBoolean(conf, CONF_VALIDATE_DB_CONN);
        String evictionInterval = ConfigurationService.get(conf, CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL).trim();
        String evictionNum = ConfigurationService.get(conf, CONF_VALIDATE_DB_CONN_EVICTION_NUM).trim();

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
        if (connPropsConfig != null) {
            connProps += "," + connPropsConfig;
        }
        props.setProperty("openjpa.ConnectionProperties", connProps);

        props.setProperty("openjpa.ConnectionDriverName", dataSource);

        factory = Persistence.createEntityManagerFactory(persistentUnit, props);

        EntityManager entityManager = getEntityManager();
        entityManager.find(WorkflowActionBean.class, 1);
        entityManager.find(WorkflowJobBean.class, 1);
        entityManager.find(CoordinatorActionBean.class, 1);
        entityManager.find(CoordinatorJobBean.class, 1);
        entityManager.find(SLAEventBean.class, 1);
        entityManager.find(JsonSLAEvent.class, 1);
        entityManager.find(BundleJobBean.class, 1);
        entityManager.find(BundleActionBean.class, 1);
        entityManager.find(SLARegistrationBean.class, 1);
        entityManager.find(SLASummaryBean.class, 1);

        LOG.info(XLog.STD, "All entities initialized");
        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized one time only
        entityManager.getTransaction().begin();
        OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        // Mask the password with '***'
        String logMsg = spi.getConfiguration().getConnectionProperties().replaceAll("Password=.*?,", "Password=***,");
        LOG.info("JPA configuration: {0}", logMsg);
        entityManager.getTransaction().commit();
        entityManager.close();
        try {
            CodecFactory.initialize(conf);
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), ex);
        }
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
                instr.incr(INSTRUMENTATION_GROUP_JPA, executor.getName(), 1);
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
        catch (PersistenceException e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        finally {
            cron.stop();
            if (instr != null) {
                instr.addCron(INSTRUMENTATION_GROUP_JPA, executor.getName(), cron);
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
     * Execute an UPDATE query
     * @param namedQueryName the name of query to be executed
     * @param query query instance to be executed
     * @param em Entity Manager
     * @return Integer that query returns, which corresponds to the number of rows updated
     * @throws JPAExecutorException
     */
    public int executeUpdate(String namedQueryName, Query query, EntityManager em) throws JPAExecutorException {
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Update/Delete Query [{0}]", namedQueryName);
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, namedQueryName, 1);
            }
            cron.start();
            em.getTransaction().begin();
            int ret = query.executeUpdate();
            if (em.getTransaction().isActive()) {
                if (FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection")) {
                    throw new RuntimeException("Skipping Commit for Failover Testing");
                }
                em.getTransaction().commit();
            }
            return ret;
        }
        catch (PersistenceException e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        finally {
            processFinally(em, cron, namedQueryName, true);
        }
    }

    public static class QueryEntry<E extends Enum<E>> {
        E namedQuery;
        Query query;

        public QueryEntry(E namedQuery, Query query) {
            this.namedQuery = namedQuery;
            this.query = query;
        }

        public Query getQuery() {
            return this.query;
        }

        public E getQueryName() {
            return this.namedQuery;
        }
    }

    private void processFinally(EntityManager em, Instrumentation.Cron cron, String name, boolean checkActive) {
        cron.stop();
        if (instr != null) {
            instr.addCron(INSTRUMENTATION_GROUP_JPA, name, cron);
        }
        if (checkActive) {
            try {
                if (em.getTransaction().isActive()) {
                    LOG.warn("[{0}] ended with an active transaction, rolling back", name);
                    em.getTransaction().rollback();
                }
            }
            catch (Exception ex) {
                LOG.warn("Could not check/rollback transaction after [{0}], {1}", name,
                        ex.getMessage(), ex);
            }
        }
        try {
            if (em.isOpen()) {
                em.close();
            }
            else {
                LOG.warn("[{0}] closed the EntityManager, it should not!", name);
            }
        }
        catch (Exception ex) {
            LOG.warn("Could not close EntityManager after [{0}], {1}", name, ex.getMessage(), ex);
        }
    }

    /**
     * Execute multiple update/insert queries in one transaction
     * @param insertBeans list of beans to be inserted
     * @param updateQueryList list of update queries
     * @param deleteBeans list of beans to be deleted
     * @param em Entity Manager
     * @throws JPAExecutorException
     */
    public void executeBatchInsertUpdateDelete(Collection<JsonBean> insertBeans, List<QueryEntry> updateQueryList,
            Collection<JsonBean> deleteBeans, EntityManager em) throws JPAExecutorException {
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Queries in Batch");
            cron.start();
            em.getTransaction().begin();
            if (updateQueryList != null && updateQueryList.size() > 0) {
                for (QueryEntry q : updateQueryList) {
                    if (instr != null) {
                        instr.incr(INSTRUMENTATION_GROUP_JPA, q.getQueryName().name(), 1);
                    }
                    q.getQuery().executeUpdate();
                }
            }
            if (insertBeans != null && insertBeans.size() > 0) {
                for (JsonBean bean : insertBeans) {
                    em.persist(bean);
                }
            }
            if (deleteBeans != null && deleteBeans.size() > 0) {
                for (JsonBean bean : deleteBeans) {
                    em.remove(em.merge(bean));
                }
            }
            if (em.getTransaction().isActive()) {
                if (FaultInjection.isActive("org.apache.oozie.command.SkipCommitFaultInjection")) {
                    throw new RuntimeException("Skipping Commit for Failover Testing");
                }
                em.getTransaction().commit();
            }
        }
        catch (PersistenceException e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
        finally {
            processFinally(em, cron, "batchqueryexecutor", true);
        }
    }

    /**
     * Execute a SELECT query
     * @param namedQueryName the name of query to be executed
     * @param query query instance to be executed
     * @param em Entity Manager
     * @return object that matches the query
     */
    public Object executeGet(String namedQueryName, Query query, EntityManager em) {
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Select Query to Get a Single row  [{0}]", namedQueryName);
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, namedQueryName, 1);
            }

            cron.start();
            Object obj = null;
            try {
                obj = query.getSingleResult();
            }
            catch (NoResultException e) {
                // return null when no matched result
            }
            return obj;
        }
        finally {
            processFinally(em, cron, namedQueryName, false);
        }
    }

    /**
     * Execute a SELECT query to get list of results
     * @param namedQueryName the name of query to be executed
     * @param query query instance to be executed
     * @param em Entity Manager
     * @return list containing results that match the query
     */
    public List<?> executeGetList(String namedQueryName, Query query, EntityManager em) {
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Select Query to Get Multiple Rows [{0}]", namedQueryName);
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, namedQueryName, 1);
            }

            cron.start();
            List<?> resultList = null;
            try {
                resultList = query.getResultList();
            }
            catch (NoResultException e) {
                // return null when no matched result
            }
            return resultList;
        }
        finally {
            processFinally(em, cron, namedQueryName, false);
        }
    }

    /**
     * Return an EntityManager. Used by the StoreService. Once the StoreService is removed this method must be removed.
     *
     * @return an entity manager
     */
    public EntityManager getEntityManager() {
        return factory.createEntityManager();
    }

}
