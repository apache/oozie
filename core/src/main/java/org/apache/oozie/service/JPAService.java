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
import java.util.concurrent.Callable;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.NoResultException;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
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
import org.apache.oozie.command.SkipCommitFaultInjection;
import org.apache.oozie.compression.CodecFactory;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.OperationRetryHandler;
import org.apache.oozie.util.db.PersistenceExceptionSubclassFilterRetryPredicate;
import org.apache.openjpa.lib.jdbc.DecoratingDataSource;
import org.apache.openjpa.persistence.InvalidStateException;
import org.apache.openjpa.persistence.OpenJPAEntityManagerFactorySPI;

/**
 * Service that manages JPA and executes {@link JPAExecutor}.
 */
@SuppressWarnings("deprecation")
public class JPAService implements Service, Instrumentable {
    private static final String INSTRUMENTATION_GROUP_JPA = "jpa";

    public static final long DEFAULT_INITIAL_WAIT_TIME = 100;
    public static final long DEFAULT_MAX_WAIT_TIME = 30_000;
    public static final int DEFAULT_MAX_RETRY_COUNT = 1;

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
    public static final String CONF_OPENJPA_BROKER_IMPL = CONF_PREFIX + "openjpa.BrokerImpl";
    public static final String INITIAL_WAIT_TIME = CONF_PREFIX + "retry.initial-wait-time.ms";
    public static final String MAX_WAIT_TIME = CONF_PREFIX + "maximum-wait-time.ms";
    public static final String MAX_RETRY_COUNT = CONF_PREFIX + "retry.max-retries";
    public static final String SKIP_COMMIT_FAULT_INJECTION_CLASS = SkipCommitFaultInjection.class.getName();

    private EntityManagerFactory factory;
    private Instrumentation instr;

    private static XLog LOG;
    private OperationRetryHandler retryHandler;

    /**
     * Return the public interface of the service.
     *
     * @return {@link JPAService}.
     */
    public Class<? extends Service> getInterface() {
        return JPAService.class;
    }

    @Override
    public void instrument(final Instrumentation instr) {
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
        final OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        final Object connectionFactory = spi.getConfiguration().getConnectionFactory();
        if (connectionFactory instanceof DecoratingDataSource) {
            final DecoratingDataSource decoratingDataSource = (DecoratingDataSource) connectionFactory;
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
    public void init(final Services services) throws ServiceException {
        LOG = XLog.getLog(JPAService.class);
        final Configuration conf = services.getConf();
        final String dbSchema = ConfigurationService.get(conf, CONF_DB_SCHEMA);
        String url = ConfigurationService.get(conf, CONF_URL);
        final String driver = ConfigurationService.get(conf, CONF_DRIVER);
        final String user = ConfigurationService.get(conf, CONF_USERNAME);
        final String password = ConfigurationService.getPassword(conf, CONF_PASSWORD).trim();
        final String maxConn = ConfigurationService.get(conf, CONF_MAX_ACTIVE_CONN).trim();
        final String dataSource = ConfigurationService.get(conf, CONF_CONN_DATA_SOURCE);
        final String connPropsConfig = ConfigurationService.get(conf, CONF_CONN_PROPERTIES);
        final String brokerImplConfig = ConfigurationService.get(conf, CONF_OPENJPA_BROKER_IMPL);
        final boolean autoSchemaCreation = ConfigurationService.getBoolean(conf, CONF_CREATE_DB_SCHEMA);
        final boolean validateDbConn = ConfigurationService.getBoolean(conf, CONF_VALIDATE_DB_CONN);
        final String evictionInterval = ConfigurationService.get(conf, CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL).trim();
        final String evictionNum = ConfigurationService.get(conf, CONF_VALIDATE_DB_CONN_EVICTION_NUM).trim();

        if (!url.startsWith("jdbc:")) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, must start with 'jdbc:'");
        }
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new ServiceException(ErrorCode.E0608, url, "invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }
        dbType = dbType.substring(0, dbType.indexOf(":"));

        final String persistentUnit = "oozie-" + dbType;

        // Checking existince of ORM file for DB type
        final String ormFile = "META-INF/" + persistentUnit + "-orm.xml";
        try {
            IOUtils.getResourceAsStream(ormFile, -1);
        }
        catch (final IOException ex) {
            throw new ServiceException(ErrorCode.E0609, dbType, ormFile);
        }

        // support for mysql replication urls "jdbc:mysql:replication://master:port,slave:port[,slave:port]/db"
        if (url.startsWith("jdbc:mysql:replication")) {
            url = "\"".concat(url).concat("\"");
            LOG.info("A jdbc replication url is provided. Url: [{0}]", url);
        }


        String connProps = "DriverClassName={0},Url={1},MaxActive={2}";
        connProps = MessageFormat.format(connProps, driver, url, maxConn);
        final Properties props = new Properties();
        if (autoSchemaCreation) {
            connProps += ",TestOnBorrow=false,TestOnReturn=false,TestWhileIdle=false";
            props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema(ForeignKeys=true)");
        }
        else if (validateDbConn) {
            // validation can be done only if the schema already exist, else a
            // connection cannot be obtained to create the schema.
            final String interval = "timeBetweenEvictionRunsMillis=" + evictionInterval;
            final String num = "numTestsPerEvictionRun=" + evictionNum;
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
        props.setProperty("openjpa.ConnectionPassword", password);
        props.setProperty("openjpa.ConnectionUserName", user);
        props.setProperty("openjpa.ConnectionDriverName", dataSource);
        if (!StringUtils.isEmpty(brokerImplConfig)) {
            props.setProperty("openjpa.BrokerImpl", brokerImplConfig);
            LOG.info("Setting openjpa.BrokerImpl to {0}", brokerImplConfig);
        }

        initRetryHandler();

        factory = Persistence.createEntityManagerFactory(persistentUnit, props);

        final EntityManager entityManager = getEntityManager();
        findRetrying(entityManager, WorkflowActionBean.class, 1);
        findRetrying(entityManager, WorkflowJobBean.class, 1);
        findRetrying(entityManager, CoordinatorActionBean.class, 1);
        findRetrying(entityManager, CoordinatorJobBean.class, 1);
        findRetrying(entityManager, SLAEventBean.class, 1);
        findRetrying(entityManager, JsonSLAEvent.class, 1);
        findRetrying(entityManager, BundleActionBean.class, 1);
        findRetrying(entityManager, BundleJobBean.class, 1);
        findRetrying(entityManager, SLARegistrationBean.class, 1);
        findRetrying(entityManager, SLASummaryBean.class, 1);

        LOG.info(XLog.STD, "All entities initialized");
        // need to use a pseudo no-op transaction so all entities, datasource
        // and connection pool are initialized one time only
        entityManager.getTransaction().begin();
        final OpenJPAEntityManagerFactorySPI spi = (OpenJPAEntityManagerFactorySPI) factory;
        // Mask the password with '***'
        final String logMsg = spi.getConfiguration().getConnectionProperties().replaceAll("Password=.*?,", "Password=***,");
        LOG.info("JPA configuration: {0}", logMsg);
        entityManager.getTransaction().commit();
        entityManager.close();
        try {
            CodecFactory.initialize(conf);
        }
        catch (final Exception ex) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), ex);
        }

    }

    private void initRetryHandler() {
        final long initialWaitTime = ConfigurationService.getInt(INITIAL_WAIT_TIME, (int) DEFAULT_INITIAL_WAIT_TIME);
        final long maxWaitTime = ConfigurationService.getInt(MAX_WAIT_TIME, (int) DEFAULT_MAX_WAIT_TIME);
        final int maxRetryCount = ConfigurationService.getInt(MAX_RETRY_COUNT, DEFAULT_MAX_RETRY_COUNT);

        LOG.info(XLog.STD, "Failing database operations will be retried {0} times, with an initial sleep time of {1} ms,"
                + "max sleep time {2} ms", maxRetryCount, initialWaitTime, maxWaitTime);
        retryHandler = new OperationRetryHandler(maxRetryCount,
                initialWaitTime,
                maxWaitTime,
                new PersistenceExceptionSubclassFilterRetryPredicate());
    }

    private void findRetrying(final EntityManager entityManager, final Class entityClass, final int primaryKey)
            throws ServiceException {
        try {
            retryHandler.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (!entityManager.getTransaction().isActive()) {
                        entityManager.getTransaction().begin();
                    }

                    entityManager.find(entityClass, primaryKey);

                    if (entityManager.getTransaction().isActive()) {
                        entityManager.getTransaction().commit();
                    }
                    return null;
                }
            });
        }
        catch (final Exception e) {
            throw new ServiceException(ErrorCode.E0603, e);
        }
    }

    /**
     * Destroy the JPAService
     */
    public void destroy() {
        if (factory != null && factory.isOpen()) {
            try {
                factory.close();
            }
            catch (final InvalidStateException ise) {
                LOG.warn("Cannot close EntityManagerFactory. [ise.message={0}]", ise.getMessage());
            }
        }
    }

    /**
     * Execute a {@link JPAExecutor}.
     *
     * @param executor JPAExecutor to execute.
     * @return return value of the JPAExecutor.
     * @throws JPAExecutorException thrown if an jpa executor failed
     */
    public <T> T execute(final JPAExecutor<T> executor) throws JPAExecutorException {
        final EntityManager em = getEntityManager();
        final Instrumentation.Cron cron = new Instrumentation.Cron();
        try {
            LOG.trace("Executing JPAExecutor [{0}]", executor.getName());
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, executor.getName(), 1);
            }
            cron.start();

            return retryHandler.executeWithRetry(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    if (!em.getTransaction().isActive()) {
                        em.getTransaction().begin();
                    }

                    final T t = executor.execute(em);

                    checkAndCommit(em.getTransaction());

                    return t;
                }
            });
        }
        catch (final Exception e) {
            throw getTargetException(e);
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
            catch (final Exception ex) {
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
            catch (final Exception ex) {
                LOG.warn("Could not close EntityManager after JPAExecutor [{0}], {1}", executor.getName(), ex
                        .getMessage(), ex);
            }
        }
    }

    private void checkAndCommit(final EntityTransaction tx) throws JPAExecutorException {
        if (tx.isActive()) {
            if (FaultInjection.isActive(SKIP_COMMIT_FAULT_INJECTION_CLASS)) {
                throw new JPAExecutorException(ErrorCode.E0603, "Skipping Commit for Failover Testing");
            }

            tx.commit();
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
    public int executeUpdate(final String namedQueryName, final Query query, final EntityManager em) throws JPAExecutorException {
        final Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Update/Delete Query [{0}]", namedQueryName);
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, namedQueryName, 1);
            }
            cron.start();

            return retryHandler.executeWithRetry(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    if (!em.getTransaction().isActive()) {
                        em.getTransaction().begin();
                    }
                    final int ret = query.executeUpdate();

                    checkAndCommit(em.getTransaction());

                    return ret;
                }
            });
        }
        catch (final Exception e) {
            throw getTargetException(e);
        }
        finally {
            processFinally(em, cron, namedQueryName, true);
        }
    }

    public static class QueryEntry<E extends Enum<E>> {
        E namedQuery;
        Query query;

        public QueryEntry(final E namedQuery, final Query query) {
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

    private void processFinally(final EntityManager em,
                                final Instrumentation.Cron cron,
                                final String name,
                                final boolean checkActive) {
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
            catch (final Exception ex) {
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
        catch (final Exception ex) {
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
    public void executeBatchInsertUpdateDelete(final Collection<JsonBean> insertBeans, final List<QueryEntry> updateQueryList,
            final Collection<JsonBean> deleteBeans, final EntityManager em) throws JPAExecutorException {
        final Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Queries in Batch");
            cron.start();

            retryHandler.executeWithRetry(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    if (em.getTransaction().isActive()) {
                        try {
                            em.getTransaction().rollback();
                        }
                        catch (final Exception e) {
                            LOG.warn("Rollback failed - ignoring");
                        }
                    }

                    em.getTransaction().begin();

                    if (CollectionUtils.isNotEmpty(updateQueryList)) {
                        for (final QueryEntry q : updateQueryList) {
                            if (instr != null) {
                                instr.incr(INSTRUMENTATION_GROUP_JPA, q.getQueryName().name(), 1);
                            }
                            q.getQuery().executeUpdate();
                        }
                    }

                    if (CollectionUtils.isNotEmpty(insertBeans)) {
                        for (final JsonBean bean : insertBeans) {
                            em.persist(bean);
                        }
                    }

                    if (CollectionUtils.isNotEmpty(deleteBeans)) {
                        for (final JsonBean bean : deleteBeans) {
                            em.remove(em.merge(bean));
                        }
                    }

                    checkAndCommit(em.getTransaction());

                    return null;
                }
            });
        }
        catch (final Exception e) {
            throw getTargetException(e);
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
    public Object executeGet(final String namedQueryName, final Query query, final EntityManager em) throws JPAExecutorException {
        final Instrumentation.Cron cron = new Instrumentation.Cron();
        try {
            LOG.trace("Executing Select Query to Get a Single row  [{0}]", namedQueryName);
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, namedQueryName, 1);
            }

            cron.start();

            return retryHandler.executeWithRetry(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Object obj = null;
                    try {
                        obj = query.getSingleResult();
                    }
                    catch (final NoResultException e) {
                        LOG.info("No results found");
                        // return null when no matched result
                    }
                    return obj;
                }
            });
        }
        catch (final Exception e) {
            throw getTargetException(e);
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
    public List<?> executeGetList(final String namedQueryName, final Query query, final EntityManager em)
            throws JPAExecutorException {
        final Instrumentation.Cron cron = new Instrumentation.Cron();
        try {

            LOG.trace("Executing Select Query to Get Multiple Rows [{0}]", namedQueryName);
            if (instr != null) {
                instr.incr(INSTRUMENTATION_GROUP_JPA, namedQueryName, 1);
            }

            cron.start();

            return retryHandler.executeWithRetry(new Callable<List<?>>() {
                @Override
                public List<?> call() throws Exception {
                    List<?> resultList = null;
                    try {
                        resultList = query.getResultList();
                    }
                    catch (final NoResultException e) {
                        LOG.info("No results found");
                        // return null when no matched result
                    }
                    return resultList;
                }
            });
        }
        catch (final Exception e) {
            throw getTargetException(e);
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

    private JPAExecutorException getTargetException(final Exception e) {
        if (e instanceof JPAExecutorException) {
            return (JPAExecutorException) e;
        }
        else {
            return new JPAExecutorException(ErrorCode.E0603, e.getMessage());
        }
    }
}
