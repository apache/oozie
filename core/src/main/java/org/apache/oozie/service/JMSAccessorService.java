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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.util.MappingRule;
import org.apache.oozie.util.XLog;

/**
 * This class will 1. Create/Manage JMS connections using user configured
 * properties 2. Create/Manage session for specific connection/topic. 3. Provide
 * a way to create a subscriber and publisher 4. Pure JMS complian
 * (implementation independent but primarily tested against Apace ActiveMQ) For
 * connection property, it reads property from oozie-site.xml. Since it supports
 * multiple connections, each property will be grouped with fixed tag. the
 * caller will use the tag to accees the connection/session/subscriber/producer.
 */
public class JMSAccessorService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JMSAccessorService.";
    public static final String JMS_CONNECTION_FACTORY = CONF_PREFIX + "jms.connectionFactory";
    public static final String JMS_CONNECTIONS_PROPERTIES = CONF_PREFIX + "connections";
    public static final String SESSION_OPTS = CONF_PREFIX + "jms.sessionOpts";
    public static final String DEFAULT_SERVER_ENDPOINT = "default";
    private String defaultConnection = null;


    private static XLog LOG;
    private Configuration conf;
    private List<MappingRule> mappingRules = null;
    ConcurrentHashMap<String, ConnectionContext> connSessionMap = new ConcurrentHashMap<String, ConnectionContext>();
    HashMap<String, Properties> hmConnProps = new HashMap<String, Properties>();

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        initializeMappingRules();
    }


    protected void initializeMappingRules() {
        String connectionString = conf.get(JMS_CONNECTIONS_PROPERTIES);
        if (connectionString != null) {
            String[] connections = connectionString.split("\\s*,\\s*");
            mappingRules = new ArrayList<MappingRule>(connections.length);
            for (String connection : connections) {
                String[] values = connection.split("=", 2);
                String key = values[0].replaceAll("^\\s+|\\s+$", "");
                String value = values[1].replaceAll("^\\s+|\\s+$", "");
                if (key.equals("default")) {
                    defaultConnection = value;
                }
                else {
                    mappingRules.add(new MappingRule(key, value));
                }
            }
        }
        else {
            LOG.warn("No JMS connection defined");
        }
    }

    /**
     * Checks if the connection exists or not. If it doens't exists, creates a new one.
     * Returns false if the connection cannot be established
     * @param serverName
     * @return
     */
    public boolean getOrCreateConnection(String serverName) {
        if (!isExistsConnection(serverName)) {
            // Get JNDI properties related to the JMS server name
            Properties props = getJMSServerProps(serverName);
            if (props != null) {
                Connection conn = null;
                try {
                    conn = getConnection(props);
                }
                catch (ServiceException se) {
                    LOG.warn("Could not create connection " + se.getErrorCode() + se.getMessage());
                    return false;
                }
                LOG.info("Connection established to JMS Server for " + serverName);
                connSessionMap.put(serverName, new ConnectionContext(conn));
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether connection to JMS server already exists
     * @param serverName
     * @return
     */
    public boolean isExistsConnection(String serverName) {
        if (connSessionMap.containsKey(serverName)) {
            LOG.info("Connection exists to JMS Server for " + serverName);
            return true; // connection already exists
        }
        else {
            return false;
        }
    }


    protected Properties getJMSServerProps(String serverName) {
        Properties props = null;
        if (hmConnProps.containsKey(serverName)) {
            props = hmConnProps.get(serverName);
            return props;
        }
        String jmsServerMapping = getJMSServerMapping(serverName);
        LOG.trace("\n JMS Server Mapping for server "+ serverName + "is " + jmsServerMapping);
        if (jmsServerMapping == null) {
            return null;
        }
        else {
            props = getJMSPropsFromConf(jmsServerMapping);
            if (props != null) {
                hmConnProps.put(serverName, props);
            }
            return props;
        }
    }

    protected String getJMSServerMapping(String serverName) {
        for (MappingRule mr : mappingRules) {
            String jmsServerMapping = mr.applyRule(serverName);
            if (jmsServerMapping != null) {
                return jmsServerMapping;
            }
        }
        return defaultConnection;

    }
    /**
     * Returns Consumer object for specific service end point and topic name
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine the JMS connection properties
     * @param topicName : topic to listen on
     * @return : MessageConsumer to receive JMS message
     * @throws JMSException
     */
    public MessageConsumer getMessageConsumer(String endPoint, String topicName) throws JMSException {
        ConnectionContext connCtx = getConnectionContext(endPoint);
        MessageConsumer ret = null;
        if (connCtx != null) {
            ret = connCtx.getConsumer(topicName);
        }
        return ret;
    }

    /**
     * Returns Producer object for specific service end point and topic name
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine the JMS connection properties
     * @param topicName : topic to send message
     * @return : MessageProducer to send JMS message
     * @throws JMSException
     */
    public MessageProducer getMessageProducer(String endPoint, String topicName) throws JMSException {
        ConnectionContext connCtx = getConnectionContext(endPoint);
        MessageProducer ret = null;
        if (connCtx != null) {
            ret = connCtx.getProducer(topicName);
        }
        return ret;
    }

    /**
     * Returns JMS session object for specific service end point and topic name
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine the JMS connection properties
     * @param topicName : topic to listen on
     * @return : Session to send/receive JMS message
     * @throws JMSException
     */
    public Session getSession(String endPoint, String topicName) throws JMSException {
        ConnectionContext connCtx = getConnectionContext(endPoint);
        Session ret = null;
        if (connCtx != null) {
            ret = connCtx.getSession(topicName);
        }
        return ret;
    }

    /**
     * Returns JMS connection context object for specific service end point
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine the JMS connection properties
     * @return : Connection context to send/receive JMS message
     */
    public ConnectionContext getConnectionContext(String endPoint) {
        ConnectionContext ret = null;
        if (connSessionMap.containsKey(endPoint)) {
            ret = connSessionMap.get(endPoint);
        }
        else {
            LOG.error("Connection doesn't exist for end point " + endPoint);
        }
        return ret;
    }

    /**
     * Remove JMS session object for specific service end point and topic name
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine the JMS connection properties
     * @param topicName : topic to listen on
     * @throws JMSException
     */
    public void removeSession(String endPoint, String topicName) throws JMSException {
        ConnectionContext connCtx = getConnectionContext(endPoint);
        if (connCtx != null) {
            connCtx.returnSession(topicName);
            connCtx.removeTopicReceiver(topicName);
        }
        return;
    }


    protected Properties getJMSPropsFromConf(String kVal) {
        Properties props = new Properties();
        String[] propArr = kVal.split(";");
        for (String pair : propArr) {
            String[] kV = pair.split("#");
            if (kV.length > 1) {
                props.put(kV[0].trim(), kV[1].trim());
            }
            else {
                LOG.info("Unformatted properties. Expected key#value : " + pair);
            }
        }
        if (props.isEmpty()) {
            return null;
        }
        return props;
    }

    @Override
    public void destroy() {
        // TODO Remove topic sessions based on no demand
        LOG.info("Destroying JMSAccessor service ");
        for (Entry<String, ConnectionContext> entry : connSessionMap.entrySet()) {
            try {
                entry.getValue().getConnection().close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close the connection for " + entry.getKey(), e);
            }
        }

    }

    @Override
    public Class<? extends Service> getInterface() {
        return JMSAccessorService.class;
    }

    /*
     * Look up connection factory Create connection
     */
    protected synchronized Connection getConnection(Properties props) throws ServiceException {

        Connection conn = null;
        try {
            Context jndiContext = getJndiContext(props);
            String connFacName = (String) jndiContext.getEnvironment().get(JMS_CONNECTION_FACTORY);
            if (connFacName == null || connFacName.trim().length() == 0) {
                connFacName = "ConnectionFactory";
            }
            ConnectionFactory connFac = (ConnectionFactory) jndiContext.lookup(connFacName);
            LOG.info("Connecting with the following properties \n" + jndiContext.getEnvironment().toString());
            conn = connFac.createConnection();
            conn.start();
            conn.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException je) {
                    LOG.error(je);
                }
            });

        }
        catch (Exception e1){
            LOG.error(e1.getMessage(), e1);
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (Exception e2) {
                    LOG.error(e2.getMessage(), e2);
                }
            }
        }
        return conn;
    }

    /*
     * Create a JNDI API InitialContext object
     */
    private Context getJndiContext(Properties props) throws ServiceException {
        Context ctx;
        try {
            ctx = new InitialContext(props);
        }
        catch (NamingException e) {
            LOG.warn("Unable to initialize the context :", e);
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), e.getMessage(), e);
        }
        return ctx;
    }

    /**
     * Get receiver for a registered topic
     *
     * @param endPoint
     * @param topicName
     * @return MessageReceiver
     * @throws JMSException
     */
    public MessageReceiver getTopicReceiver(String endPoint, String topicName) {
        MessageReceiver recvr = null;
        ConnectionContext connCtx = getConnectionContext(endPoint);
        if(connCtx != null) {
            recvr = connCtx.getTopicReceiver(topicName);
        }
        else {
            LOG.info("No connection exists to endpoint: " + endPoint);
        }
        return recvr;
    }

    /**
     * Get receiver for a registered topic and "default" server endpoint
     *
     * @param topicName
     * @return MessageReceiver
     * @throws JMSException
     */
    public MessageReceiver getTopicReceiver(String topicName) throws JMSException {
        return getTopicReceiver(DEFAULT_SERVER_ENDPOINT, topicName);
    }

    /**
     * Add a new listener for topic
     * @param recvr
     * @param endPoint
     * @param topicName
     * @throws ServiceException
     */
    public void addTopicReceiver(MessageReceiver recvr, String endPoint, String topicName) throws ServiceException {
        ConnectionContext connCtx = getConnectionContext(endPoint);
        if(connCtx != null) {
            connCtx.getTopicReceiverMap().put(topicName, recvr);
        }
        else {
            throw new ServiceException(ErrorCode.E1506, "Connection to endpoint:" + endPoint + " NULL. Message Consumer not added to map");
        }
    }


    /**
     * This class maintains a JMS connection and map of topic to Session. Only
     * one session per topic.
     */
    class ConnectionContext {
        Connection connection;
        HashMap<String, Session> hmSessionTopic = new HashMap<String, Session>();
        /*
         * Map of topic to corresponding message receiver
         * We only need to register one receiver (consumer) per topic
         */
        private ConcurrentHashMap<String, MessageReceiver> topicReceiverMap = new ConcurrentHashMap<String, MessageReceiver>();

        public ConnectionContext(Connection conn) {
            this.connection = conn;
        }

        /**
         * If there is no existing session for a specific topic name, this
         * method creates a new session. Otherwise, return the existing session
         *
         * @param topic : Name of the topic
         * @return a new/exiting JMS session
         * @throws JMSException
         */
        public Session getSession(String topic) throws JMSException {
            Session ret;
            if (hmSessionTopic.containsKey(topic)) {
                ret = hmSessionTopic.get(topic);
            }
            else {
                int sessionOpts = conf.getInt(SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
                ret = connection.createSession(false, sessionOpts);
                hmSessionTopic.put(topic, ret);
            }
            return ret;
        }

        /**
         * Returns a new MessageConsumer object.
         * It is the caller responsibility to close the MessageConsumer when done
         *
         * @param topicName : Name of the topic
         * @return MessageConsumer
         * @throws JMSException
         */
        public MessageConsumer getConsumer(String topicName) throws JMSException {
            Session session = getSession(topicName);
            Topic topic = session.createTopic(topicName);
            MessageConsumer consumer = session.createConsumer(topic);
            return consumer;
        }

        /**
         * Returns a new MessageProducer object.
         * It is the caller responsibility to close the MessageProducer when done
         *
         * @param topicName : Name of the topic
         * @return MessageProducer
         * @throws JMSException
         */
        public MessageProducer getProducer(String topicName) throws JMSException {
            Session session = getSession(topicName);
            Topic topic = session.createTopic(topicName);
            MessageProducer producer = session.createProducer(topic);
            return producer;
        }

        /**
         * Close an existing session and remove from the Map
         *
         * @param topic : Name of a topic
         * @throws JMSException
         */
        public void returnSession(String topic) throws JMSException {
            if (hmSessionTopic.containsKey(topic)) {
                Session sess = hmSessionTopic.get(topic);
                sess.close();
                hmSessionTopic.remove(topic);
            }
            else {
                LOG.info("Topic " + topic + " doesn't have any active session to close ");
            }
        }

        /**
         * @return JMS connection
         */
        public Connection getConnection() {
            return connection;
        }

        /**
         * Set JMS connection
         *
         * @param connection
         */
        public void setConnection(Connection connection) {
            this.connection = connection;
        }

        /**
         * Get the listener registered for a topicName
         * @return MessageReceiver
         */
        public MessageReceiver getTopicReceiver(String topicName) {
            MessageReceiver recvr = null;
            if(topicReceiverMap.containsKey(topicName)) {
                recvr = topicReceiverMap.get(topicName);
            }
            return recvr;
        }

        /**
         * Remove the topic -> listener mapping entry
         *
         * @param topicName
         */
        public void removeTopicReceiver(String topicName) {
            if (topicReceiverMap.containsKey(topicName)) {
                topicReceiverMap.remove(topicName);
            }
            else {
                LOG.debug("No receiver to remove corresponding to topic: " + topicName);
            }
        }

        /**
         * @return the topicReceiverMap
         */
        public ConcurrentHashMap<String, MessageReceiver> getTopicReceiverMap() {
            return topicReceiverMap;
        }

    }

}
