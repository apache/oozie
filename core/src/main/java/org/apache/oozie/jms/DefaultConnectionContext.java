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

package org.apache.oozie.jms;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.oozie.util.XLog;

public class DefaultConnectionContext implements ConnectionContext {

    protected Connection connection;
    protected String connectionFactoryName;
    private static XLog LOG = XLog.getLog(ConnectionContext.class);

    @Override
    public void createConnection(Properties props) throws NamingException, JMSException {
        Context jndiContext = new InitialContext(props);
        connectionFactoryName = (String) jndiContext.getEnvironment().get("connectionFactoryNames");
        if (connectionFactoryName == null || connectionFactoryName.trim().length() == 0) {
            connectionFactoryName = "ConnectionFactory";
        }
        ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(connectionFactoryName);
        LOG.info("Connecting with the following properties \n" + jndiContext.getEnvironment().toString());
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException je) {
                    LOG.error("Error in JMS connection", je);
                }
            });
        }
        catch (JMSException e1) {
            LOG.error(e1.getMessage(), e1);
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception e2) {
                    LOG.error(e2.getMessage(), e2);
                }
                finally {
                    connection = null;
                }
            }
            throw e1;
        }
    }

    @Override
    public boolean isConnectionInitialized() {
        return connection != null;
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException {
        connection.setExceptionListener(exceptionListener);
    }

    @Override
    public Session createSession(int sessionOpts) throws JMSException {
        if (connection == null) {
            throw new JMSException ("Connection is not initialized");
        }
        return connection.createSession(false, sessionOpts);
    }

    @Override
    public MessageConsumer createConsumer(Session session, String topicName) throws JMSException {
        Topic topic = session.createTopic(topicName);
        MessageConsumer consumer = session.createConsumer(topic);
        return consumer;
    }

    @Override
    public MessageProducer createProducer(Session session, String topicName) throws JMSException {
        Topic topic = session.createTopic(topicName);
        MessageProducer producer = session.createProducer(topic);
        return producer;
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close the connection " + connection, e);
            }
            finally {
                connection = null;
            }
        }
        th = null;
    }

    private ThreadLocal<Session> th = new ThreadLocal<Session>();

    @Override
    public Session createThreadLocalSession(final int sessionOpts) throws JMSException {
        Session session = th.get();
        if (session != null) {
            return session;
        }
        th.remove();
        session = createSession(sessionOpts);
        th.set(session);
        return session;
    }

    @Override
    public MessageConsumer createConsumer(Session session, String topicName, String selector) throws JMSException {
        Topic topic = session.createTopic(topicName);
        MessageConsumer consumer = session.createConsumer(topic, selector);
        return consumer;
    }

}
