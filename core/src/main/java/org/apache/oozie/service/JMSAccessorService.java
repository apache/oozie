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

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.NamingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.jms.DefaultConnectionContext;
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
    public static final String JMS_CONNECTIONS_PROPERTIES = CONF_PREFIX + "connections";
    public static final String JMS_CONNECTION_CONTEXT_IMPL = CONF_PREFIX +"connectioncontext.impl";
    public static final String SESSION_OPTS = CONF_PREFIX + "jms.sessionOpts";
    public static final String DEFAULT_SERVER_ENDPOINT = "default";
    private static final String DELIMITER = "#";
    private static XLog LOG;

    private String defaultConnection = null;
    private Configuration conf;
    private int sessionOpts;
    private List<MappingRule> mappingRules = null;
    private ConcurrentHashMap<String, ConnectionContext> connectionMap = new ConcurrentHashMap<String, ConnectionContext>();
    private ConcurrentHashMap<String, MessageReceiver> receiversMap = new ConcurrentHashMap<String, MessageReceiver>();
    private HashMap<String, Properties> hmConnProps = new HashMap<String, Properties>();

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        initializeMappingRules();
        sessionOpts = conf.getInt(SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
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
                    ConnectionContext connCtxt = getConnectionContext(server);
                    if (!connCtxt.isConnectionInitialized()){
                        Properties props = getJMSServerProps(server);
                        if (props == null) {
                            LOG.warn("Connection not established to JMS server for server [{0}] as JMS connection properties are not correctly defined",
                                    server);
                            return;
                        }
                        ConnectionFactory connFactory = connCtxt.createConnectionFactory(props);
                        connCtxt.createConnection(connFactory);
                    }
                    MessageReceiver receiver = setupJMSReceiver(connCtxt, topic, msgHandler);
                    LOG.info("Registered a listener for topic {0} from publisher {1}", topic, server);
                    receiversMap.put(key, receiver);
                }
            }
        }
        catch (Exception e) {
            // TODO: Exponentially backed off retry in case of connection
            // failure.
            LOG.warn("Connection to JMS server failed for publisher {0}", publisherURI, e);
        }
    }

    private MessageReceiver setupJMSReceiver(ConnectionContext connCtxt, String topic, MessageHandler msgHandler)
            throws NamingException, JMSException {
        Session session = connCtxt.createSession(sessionOpts);
        MessageConsumer consumer = connCtxt.createConsumer(session, topic);
        MessageReceiver receiver = new MessageReceiver(msgHandler, session, consumer);
        consumer.setMessageListener(receiver);
        return receiver;
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

    protected ConnectionContext getConnectionContext(String publisherAuthority) {
        ConnectionContext connCtxt = connectionMap.get(publisherAuthority);
        if (connCtxt == null) {
            connCtxt = getConnectionContextImpl();
            connectionMap.put(publisherAuthority, connCtxt);
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
                return null;
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
                entry.getValue().close();
        }
        connectionMap.clear();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return JMSAccessorService.class;
    }


    private ConnectionContext getConnectionContextImpl() {
        Class<?> defaultClazz = conf.getClass(JMS_CONNECTION_CONTEXT_IMPL, DefaultConnectionContext.class);
        ConnectionContext connCtx = null;
        if (defaultClazz == DefaultConnectionContext.class) {
            connCtx = new DefaultConnectionContext();
        }
        else {
            connCtx = (ConnectionContext) ReflectionUtils.newInstance(defaultClazz, null);
        }
        return connCtx;
    }

}
