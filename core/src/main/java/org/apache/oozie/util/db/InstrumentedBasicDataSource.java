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
package org.apache.oozie.util.db;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

public class InstrumentedBasicDataSource extends BasicDataSource {
    private static AtomicLong activeConnections = new AtomicLong();
    private static final String INTSRUMENTATION_GROUP = "jdbc";
    private static Instrumentation instrumentation;
    private static final String INSTR_ACTIVE_CONNECTIONS_SAMPLER = "connections.active";

    static {
        instrumentation = Services.get().get(InstrumentationService.class).get();
        defineSampler(INSTR_ACTIVE_CONNECTIONS_SAMPLER, activeConnections);
    }

    public InstrumentedBasicDataSource() {
    }

    /**
     * Define an instrumentation sampler. <p/> Sampling period is 60 seconds, the sampling frequency is 1 second. <p/>
     * The instrumentation group used is {@link #INSTRUMENTATION_GROUP}.
     *
     * @param samplerName sampler name.
     * @param samplerCounter sampler counter.
     */
    private static void defineSampler(String samplerName, final AtomicLong samplerCounter) {
        instrumentation.addSampler(INTSRUMENTATION_GROUP, samplerName, 60, 1, new Instrumentation.Variable<Long>() {
            public Long getValue() {
                return samplerCounter.get();
            }
        });
    }

    private class ConnectionProxy implements InvocationHandler {
        private final Connection connection;

        private ConnectionProxy(Connection connection) {
            // activeConnections.incrementAndGet();
            activeConnections.set((long) getNumActive());
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object result;
            try {
                if (method.getName().equals("close")) {
                    // activeConnections.decrementAndGet();
                    activeConnections.set((long) getNumActive());
                }
                result = method.invoke(connection, args);
            }
            catch (InvocationTargetException ite) {
                throw ite.getTargetException();
            }
            return result;
        }

    }

    public Connection getConnection() throws SQLException {
        Connection conn = super.getConnection();
        InvocationHandler handler = new ConnectionProxy(conn);
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{Connection.class},
                                                   handler);
    }

    public static AtomicLong getActiveConnections() {
        return activeConnections;
    }

}
