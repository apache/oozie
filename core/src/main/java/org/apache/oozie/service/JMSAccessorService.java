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

import java.net.URI;
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
import org.apache.oozie.jms.MessageHandler;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.util.MappingRule;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

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
    private static final String DELIMITER = "#";
    private static XLog LOG;

    private String defaultConnection = null;
    private Configuration conf;
    private List<MappingRule> mappingRules = null;
    private ConcurrentHashMap<String, ConnectionContext> connectionMap = new ConcurrentHashMap<String, ConnectionContext>();
    private ConcurrentHashMap<String, MessageReceiver> receiversMap = new ConcurrentHashMap<String, MessageReceiver>();
    private HashMap<String, Properties> hmConnProps = new HashMap<String, Properties>();

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
     * Determine whether a given source URI publishes JMS messages
     *
     * @param sourceURI URI of the publisher
     * @return true if we have JMS mapping for the source URI, else false
     */
    public boolean isKnownPublisher(URI sourceURI) {
        return getJMSServerMapping(sourceURI.getAuthority()) != null;
    }

    /**
     * Register for notifications on a JMS topic from the specified publisher.
     *
     * @param publisherURI URI of the publisher of JMS messages. Used to determine the JMS
     *        connection end point.
     * @param topic Topic in which the JMS messages are published
     * @param msgHandler Handler which will process the messages received on the topic
     */
    public void registerForNotification(URI publisherURI, String topic, MessageHandler msgHandler) {
        try {
            String server = publisherURI.getAuthority();
            String key = server + DELIMITER + topic;
            if (receiversMap.containsKey(key)) {
                return;
            }
            synchronized (receiversMap) {
                if (!receiversMap.containsKey(key)) {
                    ConnectionContext connCtxt = createConnection(server);
                    Session session = connCtxt.createSession();
                    MessageConsumer consumer = connCtxt.createConsumer(session, topic);
                    MessageReceiver receiver = new MessageReceiver(msgHandler, session, consumer);
                    consumer.setMessageListener(receiver);
                    LOG.info("Registered a listener for topic {0} from publisher {1}", topic, server);
                    receiversMap.put(key, receiver);
                }
            }
        }
        catch (Exception e) {
            //TODO: Exponentially backed off retry in case of connection failure.
            LOG.warn("Connection to JMS server failed for publisher {0}", publisherURI, e);
        }
    }

    /**
     * Unregister from listening to JMS messages on a topic.
     *
     * @param publisherURI URI of the publisher of JMS messages. Used to determine the JMS
     *        connection end point.
     * @param topic Topic in which the JMS messages are published
     */
    public void unregisterFromNotification(URI publisherURI, String topic) {
        unregisterFromNotification(publisherURI.getAuthority(), topic);
    }

    /**
     * Unregister from listening to JMS messages on a topic.
     *
     * @param publisherAuthority host:port of the publisher of JMS messages. Used to determine the JMS
     *        connection end point.
     * @param topic Topic in which the JMS messages are published
     */
    public void unregisterFromNotification(String publisherAuthority, String topic) {
        LOG.info("Unregistering JMS listener. Clossing session for {0} and topic {1}", publisherAuthority, topic);
        MessageReceiver receiver = receiversMap.remove(publisherAuthority + DELIMITER + topic);
        if (receiver != null) {
            try {
                receiver.getSession().close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close session " + receiver.getSession(), e);
            }
        }
        else {
            LOG.warn("Received unregister for {0} and topic {1}, but no matching session", publisherAuthority, topic);
        }
    }

    /**
     * Determine if currently listening to JMS messages on a topic.
     *
     * @param publisherAuthority host:port of the publisher of JMS messages. Used to determine the JMS
     *        connection end point.
     * @param topic Topic in which the JMS messages are published
     * @return true if listening to the topic, else false
     */
    public boolean isListeningToTopic(String publisherAuthority, String topic) {
        return receiversMap.get(publisherAuthority + DELIMITER + topic) != null;
    }

    protected ConnectionContext createConnection(String publisherAuthority) throws ServiceException {
        ConnectionContext connCtxt = connectionMap.get(publisherAuthority);
        if (connCtxt == null) {
            Properties props = getJMSServerProps(publisherAuthority);
            if (props != null) {
                Connection conn = getConnection(props);
                LOG.info("Connection established to JMS Server for publisher " + publisherAuthority);
                connCtxt = new ConnectionContext(conn);
                connectionMap.put(publisherAuthority, connCtxt);
            }
        }
        return connCtxt;
    }

    protected Properties getJMSServerProps(String serverName) {
        Properties props = null;
        if (hmConnProps.containsKey(serverName)) {
            props = hmConnProps.get(serverName);
            return props;
        }
        String jmsServerMapping = getJMSServerMapping(serverName);
        LOG.trace("\n JMS Server Mapping for publisher " + serverName + "is " + jmsServerMapping);
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

    @VisibleForTesting
    MessageReceiver getMessageReceiver(String publisher, String topic) {
        return receiversMap.get(publisher + DELIMITER + topic);
    }

    @Override
    public void destroy() {
        LOG.info("Destroying JMSAccessor service ");
        LOG.info("Closing JMS sessions");
        for (MessageReceiver receiver : receiversMap.values()) {
            try {
                receiver.getSession().close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close session " + receiver.getSession(), e);
            }
        }
        receiversMap.clear();

        LOG.info("Closing JMS connections");
        for (Entry<String, ConnectionContext> entry : connectionMap.entrySet()) {
            try {
                entry.getValue().getConnection().close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close the connection for " + entry.getKey(), e);
            }
        }
        connectionMap.clear();
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
        catch (Exception e1) {
            LOG.error(e1.getMessage(), e1);
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (Exception e2) {
                    LOG.error(e2.getMessage(), e2);
                }
            }
            throw new ServiceException(ErrorCode.E0100, getClass().getName(), e1.getMessage(), e1);
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
     * This class maintains a JMS connection and map of topic to Session. Only
     * one session per topic.
     */
    public class ConnectionContext {
        private Connection connection;

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
        public Session createSession() throws JMSException {
            int sessionOpts = conf.getInt(SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
            return connection.createSession(false, sessionOpts);
        }

        /**
         * Returns a new MessageConsumer object.
         * It is the caller responsibility to close the MessageConsumer when done
         *
         * @param topicName : Name of the topic
         * @return MessageConsumer
         * @throws JMSException
         */
        public MessageConsumer createConsumer(Session session, String topicName) throws JMSException {
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
        public MessageProducer createProducer(Session session, String topicName) throws JMSException {
            Topic topic = session.createTopic(topicName);
            MessageProducer producer = session.createProducer(topic);
            return producer;
        }

        /**
         * @return JMS connection
         */
        public Connection getConnection() {
            return connection;
        }

        public void close() {

            try {
                connection.close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close the connection " + connection, e);
            }
        }

    }

}
