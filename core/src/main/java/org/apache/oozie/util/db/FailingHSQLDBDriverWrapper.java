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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import com.google.common.base.Predicate;

public class FailingHSQLDBDriverWrapper extends org.hsqldb.jdbcDriver {

    public static final String USE_FAILING_DRIVER = "oozie.sql.use.failing.driver";
    public static final int DEFAULT_FAILURE_PERCENT = 5;
    private int failurePercent;

    public FailingHSQLDBDriverWrapper () {
        this(DEFAULT_FAILURE_PERCENT);
    }

    public FailingHSQLDBDriverWrapper(int failurePercent) {
        this.failurePercent = failurePercent;
    }

    public Connection connect(final String url,
                              final Properties info) throws SQLException {
        if (Boolean.parseBoolean(System.getProperty(USE_FAILING_DRIVER, "false"))) {
            return new FailingConnectionWrapper(super.connect(url, info), failurePercent, FailingDBHelperForTest.dbPredicate);
        }
        return super.connect(url, info);
    }
}
