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

import java.util.HashMap;
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

    private static XLog LOG;
    private Configuration conf;
    ConcurrentHashMap<String, ConnectionContext> connSessionMap = new ConcurrentHashMap<String, ConnectionContext>();
    HashMap<String, Properties> hmConnProps = new HashMap<String, Properties>();

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        parseConfiguration(conf);
        establishConnections();
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

    private void establishConnections() throws ServiceException {
        for (String key : hmConnProps.keySet()) {
            connSessionMap.put(key, new ConnectionContext(getConnection(hmConnProps.get(key))));
        }
    }

    private void parseConfiguration(Configuration conf) {
        String[] keyVals = conf.getStrings(JMS_CONNECTIONS_PROPERTIES, "");
        for (String kVal : keyVals) {
            LOG.info("Key=value " + kVal);
            if (kVal.trim().length() > 0) {
                addToHM(kVal);
            }
        }
    }

    private void addToHM(String kVal) {
        int pos = kVal.indexOf("=");
        Properties props = new Properties();
        if (pos > 0) {
            String val = kVal.substring(pos + 1);
            String[] propArr = val.split(";");
            for (String pair : propArr) {
                String[] kV = pair.split("#");
                if (kV.length > 1) {
                    props.put(kV[0].trim(), kV[1].trim());
                }
                else {
                    LOG.info("Unformatted properties. Expected key#value : " + pair);
                }
            }
            String key = kVal.substring(0, pos);
            LOG.info(key + ": Adding " + props);
            hmConnProps.put(key.trim(), props);
        }
        else {
            LOG.info("Unformatted properties. Expected two parts : " + kVal);
        }
    }

    @Override
    public void destroy() {
        // TODO Remove topic sessions based on no demand

    }

    @Override
    public Class<? extends Service> getInterface() {
        return JMSAccessorService.class;
    }

    /*
     * Look up connection factory Create connection
     */
    private Connection getConnection(Properties props) throws ServiceException {

        Connection conn;
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
        catch (NamingException e) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), e.getMessage(), e);
        }
        catch (JMSException e) {
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), e.getMessage(), e);
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

    @Override
    public void finalize() {
        LOG.info("Finalizing ");
        for (Entry<String, ConnectionContext> entry : connSessionMap.entrySet()) {
            try {
                entry.getValue().getConnection().close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close the connection for " + entry.getKey(), e);
            }
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
