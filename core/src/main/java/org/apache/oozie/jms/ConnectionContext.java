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
import javax.naming.NamingException;

/**
 *  Maintains a JMS connection for creating session, consumer and producer
 */
public interface ConnectionContext {

    /**
     * Create connection factory using properties
     * @param props the properties used for creating jndi context
     * @return
     * @throws NamingException
     */
    public ConnectionFactory createConnectionFactory(Properties props) throws NamingException;

    /**
     * Create connection using connection Factory
     * @param connFactory
     * @throws JMSException
     */
    public void createConnection(ConnectionFactory connFactory) throws JMSException;

    /**
    * Set the exception Listener
    * @param exceptionListener
    */
    public void setExceptionListener(ExceptionListener exceptionListener) throws JMSException;

    /**
     * Checks whether connection is initialized or not
     * @return
     */
    public boolean isConnectionInitialized();

    /**
     * Creates session using the specified session opts
     * @param sessionOpts
     * @return
     * @throws JMSException
     */
    public Session createSession(int sessionOpts) throws JMSException;

    /**
     * Creates consumer using session and topic name
     * @param session
     * @param topicName
     * @return
     * @throws JMSException
     */
    public MessageConsumer createConsumer(Session session, String topicName) throws JMSException;

    /**
     * Creates producer using session and topic
     * @param session
     * @param topicName
     * @return
     * @throws JMSException
     */
    public MessageProducer createProducer(Session session, String topicName) throws JMSException;

    /**
     * Retrieves the connection for this connection context
     * @return
     */
    public Connection getConnection();

    /**
     * Retrieves the conneciton factory name for this context
     * @return
     */
    public String getConnectionFactoryName();

    /**
     * Closes the connection
     */
    public void close();

}
