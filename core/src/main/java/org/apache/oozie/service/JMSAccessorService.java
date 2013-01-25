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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.jms.DefaultConnectionContext;
import org.apache.oozie.jms.JMSExceptionListener;
import org.apache.oozie.jms.MessageHandler;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.util.MappingRule;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class will 1. Create/Manage JMS connections using user configured properties 2.
 * Create/Manage session for specific connection/topic. 3. Provide a way to create a subscriber and
 * publisher 4. Pure JMS complian (implementation independent but primarily tested against Apace
 * ActiveMQ) For connection property, it reads property from oozie-site.xml. Since it supports
 * multiple connections, each property will be grouped with fixed tag. the caller will use the tag
 * to accees the connection/session/subscriber/producer.
 */
public class JMSAccessorService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JMSAccessorService.";
    public static final String JMS_CONNECTIONS_PROPERTIES = CONF_PREFIX + "connections";
    public static final String JMS_CONNECTION_CONTEXT_IMPL = CONF_PREFIX + "connectioncontext.impl";
    public static final String SESSION_OPTS = CONF_PREFIX + "jms.sessionOpts";
    public static final String CONF_RETRY_INITIAL_DELAY = CONF_PREFIX + "retry.initial.delay";
    public static final String CONF_RETRY_MULTIPLIER = CONF_PREFIX + "retry.multiplier";
    public static final String CONF_RETRY_MAX_ATTEMPTS = CONF_PREFIX + "retry.max.attempts";
    public static final String DEFAULT_SERVER_ENDPOINT = "default";
    private static XLog LOG;

    private String defaultConnectString = null;
    private Configuration conf;
    private int sessionOpts;
    private int retryInitialDelay;
    private int retryMultiplier;
    private int retryMaxAttempts;
    private List<MappingRule> mappingRules = null;

    /**
     * Map of publisher(host:port) to JMS connect string
     */
    private Map<String, String> publisherConnectStringMap = new HashMap<String, String>();
    /**
     * Map of JMS connect string to established JMS Connection
     */
    private ConcurrentMap<String, ConnectionContext> connectionMap = new ConcurrentHashMap<String, ConnectionContext>();
    /**
     * Map of publisher(host:port) to topic names to MessageReceiver
     */
    private ConcurrentMap<String, Map<String, MessageReceiver>> receiversMap = new ConcurrentHashMap<String, Map<String, MessageReceiver>>();

    /**
     * Map of JMS connect string to last connection attempt time
     */
    private Map<String, ConnectionAttempt> retryConnectionsMap = new HashMap<String, ConnectionAttempt>();

    /**
     * Map of publisher(host:port) to topic names that need to be registered for listening to the
     * MessageHandler that process message of the topic.
     */
    private Map<String, Map<String, MessageHandler>> retryTopicsMap = new HashMap<String, Map<String, MessageHandler>>();

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        initializeMappingRules();
        sessionOpts = conf.getInt(SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
        retryInitialDelay = conf.getInt(CONF_RETRY_INITIAL_DELAY, 60); // initial delay in seconds
        retryMultiplier = conf.getInt(CONF_RETRY_MULTIPLIER, 2);
        retryMaxAttempts = conf.getInt(CONF_RETRY_MAX_ATTEMPTS, 10);
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
                    defaultConnectString = value;
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
        if (publisherConnectStringMap.containsKey(sourceURI.getAuthority())) {
            return true;
        }
        else {
            return getJMSServerConnectString(sourceURI.getAuthority()) != null;
        }
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
        String publisherAuthority = publisherURI.getAuthority();
        if (isTopicInRetryList(publisherAuthority, topic)) {
            return;
        }
        if (isConnectionInRetryList(publisherAuthority)) {
            queueTopicForRetry(publisherAuthority, topic, msgHandler);
            return;
        }
        Map<String, MessageReceiver> topicsMap = getReceiversTopicsMap(publisherAuthority);
        if (topicsMap.containsKey(topic)) {
            return;
        }
        synchronized (topicsMap) {
            if (!topicsMap.containsKey(topic)) {
                String jmsConnectString = getJMSServerConnectString(publisherAuthority);
                ConnectionContext connCtxt = createConnectionContext(jmsConnectString);
                if (connCtxt == null) {
                    queueTopicForRetry(publisherAuthority, topic, msgHandler);
                    queueConnectionForRetry(jmsConnectString);
                    return;
                }
                MessageReceiver receiver = registerForTopic(connCtxt, publisherAuthority, topic, msgHandler);
                if (receiver == null) {
                    queueTopicForRetry(publisherAuthority, topic, msgHandler);
                    queueConnectionForRetry(jmsConnectString);
                }
                else {
                    LOG.info("Registered a listener for topic {0} from publisher {1}", topic, publisherAuthority);
                    topicsMap.put(topic, receiver);
                }
            }
        }
    }

    /**
     * Unregister from listening to JMS messages on a topic.
     *
     * @param publisherAuthority host:port of the publisher of JMS messages. Used to determine the
     *        JMS connection end point.
     * @param topic Topic in which the JMS messages are published
     */
    public void unregisterFromNotification(String publisherAuthority, String topic) {
        LOG.info("Unregistering JMS listener. Clossing session for {0} and topic {1}", publisherAuthority, topic);

        if (isTopicInRetryList(publisherAuthority, topic)) {
            removeTopicFromRetryList(publisherAuthority, topic);
        }
        else {
            Map<String, MessageReceiver> topicsMap = receiversMap.get(publisherAuthority);
            MessageReceiver receiver = topicsMap.remove(topic);
            if (receiver != null) {
                try {
                    receiver.getSession().close();
                }
                catch (JMSException e) {
                    LOG.warn("Unable to close session " + receiver.getSession(), e);
                }
            }
            else {
                LOG.warn(
                        "Received request to unregister from topic [{0}] from publisher [{1}], but no matching session.",
                        topic, publisherAuthority);
            }
            if (topicsMap.isEmpty()) {
                receiversMap.remove(publisherAuthority);
            }
        }
    }

    private Map<String, MessageReceiver> getReceiversTopicsMap(String publisherAuthority) {
        Map<String, MessageReceiver> topicsMap = receiversMap.get(publisherAuthority);
        if (topicsMap == null) {
            topicsMap = new HashMap<String, MessageReceiver>();
            Map<String, MessageReceiver> exists = receiversMap.putIfAbsent(publisherAuthority, topicsMap);
            if (exists != null) {
                topicsMap = exists;
            }
        }
        return topicsMap;
    }

    /**
     * Determine if currently listening to JMS messages on a topic.
     *
     * @param publisherAuthority host:port of the publisher of JMS messages. Used to determine the
     *        JMS connection end point.
     * @param topic Topic in which the JMS messages are published
     * @return true if listening to the topic, else false
     */
    @VisibleForTesting
    boolean isListeningToTopic(String publisherAuthority, String topic) {
        Map<String, MessageReceiver> topicsMap = receiversMap.get(publisherAuthority);
        return (topicsMap != null && topicsMap.containsKey(topic));
    }

    @VisibleForTesting
    boolean isConnectionInRetryList(String publisherAuthority) {
        String jmsConnectString = getJMSServerConnectString(publisherAuthority);
        return retryConnectionsMap.containsKey(jmsConnectString);
    }

    @VisibleForTesting
    boolean isTopicInRetryList(String publisherAuthority, String topic) {
        Map<String, MessageHandler> topicsMap = retryTopicsMap.get(publisherAuthority);
        return topicsMap != null && topicsMap.containsKey(topic);
    }

    // For unit testing
    @VisibleForTesting
    int getNumConnectionAttempts(String publisherAuthority) {
        String jmsConnectString = getJMSServerConnectString(publisherAuthority);
        return retryConnectionsMap.get(jmsConnectString).getNumAttempt();
    }

    private void queueConnectionForRetry(String jmsConnectString) {
        if (!retryConnectionsMap.containsKey(jmsConnectString)) {
            LOG.info("Queueing connection {0} for retry", jmsConnectString);
            retryConnectionsMap.put(jmsConnectString, new ConnectionAttempt(0, retryInitialDelay));
            scheduleRetry(jmsConnectString, retryInitialDelay);
        }
    }

    private void queueTopicForRetry(String publisherAuthority, String topic, MessageHandler msgHandler) {
        LOG.info("Queueing topic {0} from publisher {1} for retry", topic, publisherAuthority);
        Map<String, MessageHandler> topicsMap = retryTopicsMap.get(publisherAuthority);
        if (topicsMap == null) {
            topicsMap = new HashMap<String, MessageHandler>();
            retryTopicsMap.put(publisherAuthority, topicsMap);
        }
        topicsMap.put(topic, msgHandler);
    }

    private void removeTopicFromRetryList(String publisherAuthority, String topic) {
        LOG.info("Removing topic {0} from publisher {1} from retry list", topic, publisherAuthority);
        Map<String, MessageHandler> topicsMap = retryTopicsMap.get(publisherAuthority);
        if (topicsMap != null) {
            topicsMap.remove(topic);
            if (topicsMap.isEmpty()) {
                retryTopicsMap.remove(publisherAuthority);
            }
        }
    }

    private MessageReceiver registerForTopic(ConnectionContext connCtxt, String publisherAuthority, String topic,
            MessageHandler msgHandler) {
        try {
            Session session = connCtxt.createSession(sessionOpts);
            MessageConsumer consumer = connCtxt.createConsumer(session, topic);
            MessageReceiver receiver = new MessageReceiver(msgHandler, session, consumer);
            consumer.setMessageListener(receiver);
            return receiver;
        }
        catch (JMSException e) {
            LOG.warn("Error while registering to listen to topic {0} from publisher {1}", topic, publisherAuthority, e);
            return null;
        }
    }

    protected ConnectionContext createConnectionContext(String jmsConnectString) {
        ConnectionContext connCtxt = connectionMap.get(jmsConnectString);
        if (connCtxt == null) {
            Properties props = getJMSPropsFromConf(jmsConnectString);
            if (props != null) {
                try {
                    connCtxt = getConnectionContextImpl();
                    connCtxt.createConnection(connCtxt.createConnectionFactory(props));
                    connCtxt.setExceptionListener(new JMSExceptionListener(jmsConnectString, connCtxt));
                    connectionMap.put(jmsConnectString, connCtxt);
                    LOG.info("Connection established to JMS Server for [{0}]", jmsConnectString);
                }
                catch (Exception e) {
                    LOG.warn("Exception while establishing connection to JMS Server for [{0}]", jmsConnectString, e);
                    return null;
                }
            }
            else {
                LOG.warn("JMS connection string is not configured properly - [{0}]", jmsConnectString);
            }
        }
        return connCtxt;
    }

    protected String getJMSServerConnectString(String publisherAuthority) {
        if (publisherConnectStringMap.containsKey(publisherAuthority)) {
            return publisherConnectStringMap.get(publisherAuthority);
        }
        else {
            for (MappingRule mr : mappingRules) {
                String jmsConnectString = mr.applyRule(publisherAuthority);
                if (jmsConnectString != null) {
                    publisherConnectStringMap.put(publisherAuthority, jmsConnectString);
                    return jmsConnectString;
                }
            }
            publisherConnectStringMap.put(publisherAuthority, defaultConnectString);
            return defaultConnectString;
        }
    }

    protected Properties getJMSPropsFromConf(String jmsConnectString) {
        Properties props = new Properties();
        String[] propArr = jmsConnectString.split(";");
        for (String pair : propArr) {
            String[] kV = pair.split("#");
            if (kV.length > 1) {
                props.put(kV[0].trim(), kV[1].trim());
            }
            else {
                LOG.warn("Unformatted properties. Expected key#value : " + pair);
                return null;
            }
        }
        if (props.isEmpty()) {
            return null;
        }
        return props;
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

    @VisibleForTesting
    MessageReceiver getMessageReceiver(String publisher, String topic) {
        Map<String, MessageReceiver> topicsMap = receiversMap.get(publisher);
        if (topicsMap != null) {
            return topicsMap.get(topic);
        }
        return null;
    }

    @Override
    public void destroy() {
        LOG.info("Destroying JMSAccessor service ");
        LOG.info("Closing JMS sessions");
        for (Map<String, MessageReceiver> topicsMap : receiversMap.values()) {
            for (MessageReceiver receiver : topicsMap.values()) {
                try {
                    receiver.getSession().close();
                }
                catch (JMSException e) {
                    LOG.warn("Unable to close session " + receiver.getSession(), e);
                }
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

    /**
     * Reestablish connection for the given JMS connect string
     * @param jmsConnectString JMS connect string
     */
    public void reestablishConnection(String jmsConnectString) {
        connectionMap.remove(jmsConnectString);
        queueConnectionForRetry(jmsConnectString);
        for (Entry<String, String> entry : publisherConnectStringMap.entrySet()) {
            if (entry.getValue().equals(jmsConnectString)) {
                String publisherAuthority = entry.getKey();
                Map<String, MessageReceiver> topicsMap = receiversMap.remove(publisherAuthority);
                if (topicsMap != null) {
                    Map<String, MessageHandler> retryTopics = retryTopicsMap.get(publisherAuthority);
                    if (retryTopics == null) {
                        retryTopics = new HashMap<String, MessageHandler>();
                        retryTopicsMap.put(publisherAuthority, retryTopics);
                    }
                    for (Entry<String, MessageReceiver> topicEntry : topicsMap.entrySet()) {
                        MessageReceiver receiver = topicEntry.getValue();
                        retryTopics.put(topicEntry.getKey(), receiver.getMessageHandler());
                        try {
                            receiver.getSession().close();
                        }
                        catch (JMSException e) {
                            LOG.warn("Unable to close session " + receiver.getSession(), e);
                        }
                    }
                }
            }
        }
    }

    private void scheduleRetry(String jmsConnectString, long delay) {
        LOG.info("Scheduling retry of connection [{0}] in [{1}] seconds", jmsConnectString, delay);
        JMSRetryRunnable runnable = new JMSRetryRunnable(jmsConnectString);
        SchedulerService scheduler = Services.get().get(SchedulerService.class);
        scheduler.schedule(runnable, delay, SchedulerService.Unit.SEC);
    }

    @VisibleForTesting
    boolean retryConnection(String jmsConnectString) {
        ConnectionAttempt attempt = retryConnectionsMap.get(jmsConnectString);
        if (attempt.getNumAttempt() >= retryMaxAttempts) {
            LOG.info("Not attempting jms connection [{0}] again. Reached max attempts [{1}]", jmsConnectString,
                    retryMaxAttempts);
            return false;
        }
        LOG.info("Attempting retry of connection [{0}]", jmsConnectString);
        attempt.setNumAttempt(attempt.getNumAttempt() + 1);
        attempt.setNextDelay(attempt.getNextDelay() * retryMultiplier);
        ConnectionContext connCtxt = createConnectionContext(jmsConnectString);
        boolean removeRetryConnection = true;
        if (connCtxt == null) {
            removeRetryConnection = false;
        }
        else {
            for (Entry<String, String> entry : publisherConnectStringMap.entrySet()) {
                if (entry.getValue().equals(jmsConnectString)) {
                    String publisherAuthority = entry.getKey();
                    Map<String, MessageHandler> retryTopics = retryTopicsMap.get(publisherAuthority);
                    if (retryTopics != null) {
                        List<String> topicsToRemoveList = new ArrayList<String>();
                        // For each topic in the retry list, try to register for the topic
                        for (Entry<String, MessageHandler> topicEntry : retryTopics.entrySet()) {
                            String topic = topicEntry.getKey();
                            Map<String, MessageReceiver> topicsMap = getReceiversTopicsMap(publisherAuthority);
                            if (topicsMap.containsKey(topic)) {
                                continue;
                            }
                            synchronized (topicsMap) {
                                if (!topicsMap.containsKey(topic)) {
                                    MessageReceiver receiver = registerForTopic(connCtxt, publisherAuthority, topic,
                                            topicEntry.getValue());
                                    if (receiver == null) {
                                        queueTopicForRetry(publisherAuthority, topic, topicEntry.getValue());
                                        removeRetryConnection = false;
                                    }
                                    else {
                                        topicsMap.put(topic, receiver);
                                        topicsToRemoveList.add(topic);
                                        LOG.info("Registered a listener for topic {0} from publisher {1}", topic,
                                                publisherAuthority);
                                    }
                                }
                            }
                        }
                        for (String topic : topicsToRemoveList) {
                            retryTopics.remove(topic);
                        }
                    }
                    if (retryTopics.isEmpty()) {
                        retryTopicsMap.remove(publisherAuthority);
                    }
                }
            }
        }
        if (removeRetryConnection) {
            retryConnectionsMap.remove(jmsConnectString);
        }
        else {
            scheduleRetry(jmsConnectString, attempt.getNextDelay());
        }
        return true;
    }

    private static class ConnectionAttempt {
        private int numAttempt;
        private int nextDelay;

        public ConnectionAttempt(int numAttempt, int nextDelay) {
            this.numAttempt = numAttempt;
            this.nextDelay = nextDelay;
        }

        public int getNumAttempt() {
            return numAttempt;
        }

        public void setNumAttempt(int numAttempt) {
            this.numAttempt = numAttempt;
        }

        public int getNextDelay() {
            return nextDelay;
        }

        public void setNextDelay(int nextDelay) {
            this.nextDelay = nextDelay;
        }

    }

    public class JMSRetryRunnable implements Runnable {

        private String jmsConnectString;

        public JMSRetryRunnable(String jmsConnectString) {
            this.jmsConnectString = jmsConnectString;
        }

        public String getJmsConnectString() {
            return jmsConnectString;
        }

        @Override
        public void run() {
            retryConnection(jmsConnectString);
        }

    }

}
