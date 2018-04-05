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

import com.mysql.jdbc.Driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Used for runtime database error injection on MySQL.
 * <p>
 * Necessary steps:
 * <ul>
 *     <li>set {@code oozie.service.JPAService.connection.data.source} to
 *     {@code org.apache.oozie.util.db.BasicDataSourceWrapper} within {@code oozie-site.xml}</li>
 *     <li>set {@code oozie.service.JPAService.jdbc.driver} to {@code org.apache.oozie.util.db.FailingMySQLDriverWrapper}
 *      within {@code oozie-site.xml}</li>
 *     <li>restart Oozie server</li>
 *     <li>submit / start some workflows, coordinators etc.</li>
 *     <li>see any of those {@code JPAException} instances with following message prefix:
 *     {@code Deliberately failing to prepare statement.}</li>
 * </ul>
 */
public class FailingMySQLDriverWrapper extends Driver {
    public FailingMySQLDriverWrapper() throws SQLException {
        super();
    }

    public Connection connect(final String url,
                              final Properties info) throws SQLException {
        return new FailingConnectionWrapper(super.connect(url, info), FailingHSQLDBDriverWrapper.DEFAULT_FAILURE_PERCENT, null);
    }
}