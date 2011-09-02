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

import org.apache.oozie.store.OozieSchema;
import org.apache.oozie.service.DBLiteWorkflowStoreService;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The datasource service provides access to the SQL datasource used by Oozie.
 * <p/>
 * The returned datasource and connections are instrumented.
 * <p/>
 * The datasource checks at startup that if the database support select for update or not.
 * <p/>
 * The following configuration parameters control the datasource service:
 * <p/>
 * Regardless JNDI being set or not the following properties must be set:
 * <p/>
 * {@link #CONF_DRIVER} driver class.
 * <p/>
 * {@link #CONF_URL} database JDBC URL.
 * <p/>
 * {@link #CONF_USERNAME} database user.
 * <p/>
 * {@link #CONF_PASSWORD} database password.
 * <p/>
 * If JNDI is not set, the following property must be set:
 * <p/>
 * {@link #CONF_MAX_ACTIVE_CONN} max number of action JDBC connections.
 */
public class DataSourceService implements Service, Instrumentable {
    private static final String INTRUMENTATION_GROUP = "jdbc";
    private static final String INSTR_ACTIVE_CONNECTIONS_SAMPLER = "connections.active";

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "DataSourceService.";
    public static final String CONF_DRIVER = CONF_PREFIX + "jdbc.driver";
    public static final String CONF_URL = CONF_PREFIX + "jdbc.url";
    public static final String CONF_USERNAME = CONF_PREFIX + "jdbc.username";
    public static final String CONF_PASSWORD = CONF_PREFIX + "jdbc.password";
    public static final String CONF_MAX_ACTIVE_CONN = CONF_PREFIX + "pool.max.active.conn";

    private BasicDataSource ownDataSource;
    private DataSourceProxy dataSourceProxy;
    private AtomicLong connectionCount = new AtomicLong();

    /**
     * Initialize the datasource service.
     *
     * @param services services instance.
     * @throws ServiceException thrown if the service could not be initialized.
     */
    @SuppressWarnings({"ThrowFromFinallyBlock"})
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        String dbName = conf.get(DBLiteWorkflowStoreService.CONF_SCHEMA_NAME, "oozie");
        OozieSchema.setOozieDbName(dbName);
        String validation_query = OozieSchema.getValidationQuery(dbName);
        Properties props = new Properties();
        props.put("driverClassName", conf.get(CONF_DRIVER, "org.hsqldb.jdbcDriver"));
        props.put("url", conf.get(CONF_URL, "jdbc:hsqldb:mem:testdb"));
        props.put("username", conf.get(CONF_USERNAME, "sa"));
        props.put("password", conf.get(CONF_PASSWORD, "").trim());
        props.put("maxActive", conf.get(CONF_MAX_ACTIVE_CONN, "10"));
        if(validation_query != null) {
            props.put("testOnBorrow", "true");
            props.put("validationQuery", validation_query);
        }
        try {
            ownDataSource = (BasicDataSource) BasicDataSourceFactory.createDataSource(props);
            dataSourceProxy = createProxy(ownDataSource);
        }
        catch (Exception ex) {
            throw new ServiceException(ErrorCode.E0141, ex.getMessage(), ex);
        }
    }

    /**
     * Destroy the service.
     */
    public void destroy() {
        if (ownDataSource != null) {
            try {
                ownDataSource.close();
            }
            catch (SQLException ex) {
                XLog log = new XLog(LogFactory.getLog(getClass()));
                log.warn("Failed to close datasource, {0}", ex.getMessage(), ex);

            }
            ownDataSource = null;
        }
        dataSourceProxy = null;
    }

    /**
     * Return the public interface of the service.
     *
     * @return {@link DataSourceService}.
     */
    public Class<? extends Service> getInterface() {
        return DataSourceService.class;
    }

    /**
     * Instrument the service.
     *
     * @param instr instrumentation instance.
     */
    public void instrument(Instrumentation instr) {
        instr.addSampler(INTRUMENTATION_GROUP, INSTR_ACTIVE_CONNECTIONS_SAMPLER, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return connectionCount.get();
            }
        });
    }

    /**
     * Return a raw direct JDBC connection.
     * <p/>
     * The connection is not from the connection pool.
     * <p/>
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
     * Return a managed JDBC connection.
     *
     * @return a managed JDBC connection.
     * @throws SQLException thrown if the managed connection could not be obtained.
     */
    public Connection getConnection() throws SQLException {
        return dataSourceProxy.getDataSource().getConnection();
    }

    private DataSourceProxy createProxy(DataSource datasource) {
        DataSourceProxy proxy = new DataSourceProxy(datasource);
        proxy.dsProxy = (DataSource) Proxy
                .newProxyInstance(datasource.getClass().getClassLoader(), new Class[]{DataSource.class}, proxy);
        return proxy;
    }

    private class DataSourceProxy implements InvocationHandler {
        private final DataSource datasource;
        private DataSource dsProxy;

        private DataSourceProxy(DataSource datasource) {
            this.datasource = datasource;
        }

        public Class<? extends DataSource> getTargetClass() {
            return datasource.getClass();
        }

        public DataSource getDataSource() {
            return dsProxy;
        }

        public DataSource getRawDataSource() {
            return datasource;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object result;
            try {
                result = method.invoke(datasource, args);
                if (method.getName().equals("getConnection")) {
                    connectionCount.incrementAndGet();
                    result = Proxy.newProxyInstance(result.getClass().getClassLoader(), new Class[]{Connection.class},
                                                    new ConnectionProxy((Connection) result));
                }
            }
            catch (InvocationTargetException ite) {
                throw ite.getTargetException();
            }
            return result;
        }


        private class ConnectionProxy implements InvocationHandler {

            private final Connection connection;

            private ConnectionProxy(Connection connection) {
                this.connection = connection;
            }

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Object result;
                try {
                    if (method.getName().equals("close")) {
                        connectionCount.decrementAndGet();
                    }
                    result = method.invoke(connection, args);
                }
                catch (InvocationTargetException ite) {
                    throw ite.getTargetException();
                }
                return result;
            }

        }

    }

}