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

package org.apache.oozie.util.db;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverConnectionFactory;
import org.apache.commons.dbcp.SQLNestedException;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

public class BasicDataSourceWrapper extends BasicDataSource {

    /**
     * Fixing a bug within {@link BasicDataSource#createConnectionFactory()} for {@code driverClassName} to have real effect.
     * <p>
     * Because we use currently Apache Commons DBCP 1.4.0 that has a bug not considering {@code driverClassName}, thus, we're unable
     * to create a JDBC driver using a user-provided driver class name (we try to do that by setting explicitly a value for
     * {@code openJpa.connectionProperties="DriverClassName=..."}), unless we perform the exact same fix that is applied by the DBCP
     * patch.
     * <p>
     * Note: when DBCP 1.4.1 will be released, and Oozie will update to 1.4.1, we can remove this class.
     * <p>
     * Please see <a href="https://issues.apache.org/jira/browse/DBCP-333">the DBCP bug</a> and
     * <a href="https://github.com/apache/commons-dbcp/blob/DBCP_1_4_x_BRANCH/src/java/org/apache/commons/dbcp/BasicDataSource.java#L1588-L1660">
     * the fixed method </a>
     * for details.
     * <p>
     * Please also see how OpenJPA
     * <a href="http://openjpa.apache.org/builds/2.2.1/apache-openjpa/docs/ref_guide_integration_dbcp.html"> is integrated</a>
     * with DBCP.
     */
    protected ConnectionFactory createConnectionFactory() throws SQLException {
        // Load the JDBC driver class
        Class driverFromCCL = null;
        if (driverClassName != null) {
            try {
                try {
                    if (driverClassLoader == null) {
                        driverFromCCL = Class.forName(driverClassName);
                    } else {
                        driverFromCCL = Class.forName(driverClassName, true, driverClassLoader);
                    }
                } catch (ClassNotFoundException cnfe) {
                    driverFromCCL = Thread.currentThread(
                    ).getContextClassLoader().loadClass(
                            driverClassName);
                }
            } catch (Throwable t) {
                String message = "Cannot load JDBC driver class '" +
                        driverClassName + "'";
                logWriter.println(message);
                t.printStackTrace(logWriter);
                throw new SQLNestedException(message, t);
            }
        }

        // Create a JDBC driver instance
        Driver driver = null;
        try {
            if (driverFromCCL == null) {
                driver = DriverManager.getDriver(url);
            } else {
                // Usage of DriverManager is not possible, as it does not
                // respect the ContextClassLoader
                driver = (Driver) driverFromCCL.newInstance();
                if (!driver.acceptsURL(url)) {
                    throw new SQLException("No suitable driver", "08001");
                }
            }
        } catch (Throwable t) {
            String message = "Cannot create JDBC driver of class '" +
                    (driverClassName != null ? driverClassName : "") +
                    "' for connect URL '" + url + "'";
            logWriter.println(message);
            t.printStackTrace(logWriter);
            throw new SQLNestedException(message, t);
        }

        // Can't test without a validationQuery
        if (validationQuery == null) {
            setTestOnBorrow(false);
            setTestOnReturn(false);
            setTestWhileIdle(false);
        }

        // Set up the driver connection factory we will use
        String user = username;
        if (user != null) {
            connectionProperties.put("user", user);
        } else {
            log("DBCP DataSource configured without a 'username'");
        }

        String pwd = password;
        if (pwd != null) {
            connectionProperties.put("password", pwd);
        } else {
            log("DBCP DataSource configured without a 'password'");
        }

        ConnectionFactory driverConnectionFactory = new DriverConnectionFactory(driver, url, connectionProperties);
        return driverConnectionFactory;
    }
}
