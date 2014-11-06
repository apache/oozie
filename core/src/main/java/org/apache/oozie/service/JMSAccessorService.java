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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.jms.DefaultConnectionContext;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.jms.JMSExceptionListener;
import org.apache.oozie.jms.MessageHandler;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.util.XLog;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class will <ul>
 * <li> Create/Manage JMS connections using user configured JNDI properties. </li>
 * <li> Create/Manage session for specific connection/topic and reconnects on failures. </li>
 * <li> Provide a way to create a subscriber and publisher </li>
 * <li> Pure JMS compliant (implementation independent but primarily tested against Apache ActiveMQ). </li>
 * </ul>
 */
public class JMSAccessorService implements Service {
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "JMSAccessorService.";
    public static final String JMS_CONNECTION_CONTEXT_IMPL = CONF_PREFIX + "connectioncontext.impl";
    public static final String SESSION_OPTS = CONF_PREFIX + "jms.sessionOpts";
    public static final String CONF_RETRY_INITIAL_DELAY = CONF_PREFIX + "retry.initial.delay";
    public static final String CONF_RETRY_MULTIPLIER = CONF_PREFIX + "retry.multiplier";
    public static final String CONF_RETRY_MAX_ATTEMPTS = CONF_PREFIX + "retry.max.attempts";
    private static XLog LOG;

    private Configuration conf;
    private int sessionOpts;
    private int retryInitialDelay;
    private int retryMultiplier;
    private int retryMaxAttempts;
    private ConnectionContext jmsProducerConnContext;

    /**
     * Map of JMS connection info to established JMS Connection
     */
    private ConcurrentMap<JMSConnectionInfo, ConnectionContext> connectionMap =
            new ConcurrentHashMap<JMSConnectionInfo, ConnectionContext>();
    /**
     * Map of JMS connection info to topic names to MessageReceiver
     */
    private ConcurrentMap<JMSConnectionInfo, Map<String, MessageReceiver>> receiversMap =
            new ConcurrentHashMap<JMSConnectionInfo, Map<String, MessageReceiver>>();

    /**
     * Map of JMS connection info to connection retry information
     */
    private Map<JMSConnectionInfo, ConnectionRetryInfo> retryConnectionsMap = new HashMap<JMSConnectionInfo, ConnectionRetryInfo>();

    @Override
    public void init(Services services) throws ServiceException {
        LOG = XLog.getLog(getClass());
        conf = services.getConf();
        sessionOpts = conf.getInt(SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
        retryInitialDelay = conf.getInt(CONF_RETRY_INITIAL_DELAY, 60); // initial delay in seconds
        retryMultiplier = conf.getInt(CONF_RETRY_MULTIPLIER, 2);
        retryMaxAttempts = conf.getInt(CONF_RETRY_MAX_ATTEMPTS, 10);
    }

    /**
     * Register for notifications on a JMS topic.
     *
     * @param connInfo Information to connect to a JMS compliant messaging service.
     * @param topic Topic in which the JMS messages are published
     * @param msgHandler Handler which will process the messages received on the topic
     */
    public void registerForNotification(JMSConnectionInfo connInfo, String topic, MessageHandler msgHandler) {
        if (!isTopicInRetryList(connInfo, topic)) {
            if (isConnectionInRetryList(connInfo)) {
                queueTopicForRetry(connInfo, topic, msgHandler);
            }
            else {
                Map<String, MessageReceiver> topicsMap = getReceiversTopicsMap(connInfo);
                if (!topicsMap.containsKey(topic)) {
                    synchronized (topicsMap) {
                        if (!topicsMap.containsKey(topic)) {
                            ConnectionContext connCtxt = createConnectionContext(connInfo);
                            if (connCtxt == null) {
                                queueTopicForRetry(connInfo, topic, msgHandler);
                                return;
                            }
                            MessageReceiver receiver = registerForTopic(connInfo, connCtxt, topic, msgHandler);
                            if (receiver == null) {
                                queueTopicForRetry(connInfo, topic, msgHandler);
                            }
                            else {
                                LOG.info("Registered a listener for topic {0} on {1}", topic, connInfo);
                                topicsMap.put(topic, receiver);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Unregister from listening to JMS messages on a topic.
     *
     * @param connInfo Information to connect to the JMS compliant messaging service.
     * @param topic Topic in which the JMS messages are published
     */
    public void unregisterFromNotification(JMSConnectionInfo connInfo, String topic) {
        LOG.info("Unregistering JMS listener. Clossing session for {0} and topic {1}", connInfo, topic);

        if (isTopicInRetryList(connInfo, topic)) {
            removeTopicFromRetryList(connInfo, topic);
        }
        else {
            Map<String, MessageReceiver> topicsMap = receiversMap.get(connInfo);
            if (topicsMap != null) {
                MessageReceiver receiver = null;
                synchronized (topicsMap) {
                    receiver = topicsMap.remove(topic);
                    if (topicsMap.isEmpty()) {
                        receiversMap.remove(connInfo);
                    }
                }
                if (receiver != null) {
                    try {
                        receiver.getSession().close();
                    }
                    catch (JMSException e) {
                        LOG.warn("Unable to close session " + receiver.getSession(), e);
                    }
                }
                else {
                    LOG.warn("Received request to unregister from topic [{0}] on [{1}], but no matching session.",
                            topic, connInfo);
                }
            }
        }
    }

    private Map<String, MessageReceiver> getReceiversTopicsMap(JMSConnectionInfo connInfo) {
        Map<String, MessageReceiver> topicsMap = receiversMap.get(connInfo);
        if (topicsMap == null) {
            topicsMap = new HashMap<String, MessageReceiver>();
            Map<String, MessageReceiver> exists = receiversMap.putIfAbsent(connInfo, topicsMap);
            if (exists != null) {
                topicsMap = exists;
            }
        }
        return topicsMap;
    }

    /**
     * Determine if currently listening to JMS messages on a topic.
     *
     * @param connInfo Information to connect to the JMS compliant messaging service.
     * @param topic Topic in which the JMS messages are published
     * @return true if listening to the topic, else false
     */
    @VisibleForTesting
    boolean isListeningToTopic(JMSConnectionInfo connInfo, String topic) {
        Map<String, MessageReceiver> topicsMap = receiversMap.get(connInfo);
        return (topicsMap != null && topicsMap.containsKey(topic));
    }

    @VisibleForTesting
    boolean isConnectionInRetryList(JMSConnectionInfo connInfo) {
        return retryConnectionsMap.containsKey(connInfo);
    }

    @VisibleForTesting
    boolean isTopicInRetryList(JMSConnectionInfo connInfo, String topic) {
        ConnectionRetryInfo connRetryInfo = retryConnectionsMap.get(connInfo);
        if (connRetryInfo == null) {
            return false;
        }
        else {
            Map<String, MessageHandler> topicsMap = connRetryInfo.getTopicsToRetry();
            return topicsMap.containsKey(topic);
        }
    }

    // For unit testing
    @VisibleForTesting
    int getNumConnectionAttempts(JMSConnectionInfo connInfo) {
        return retryConnectionsMap.get(connInfo).getNumAttempt();
    }

    private ConnectionRetryInfo queueConnectionForRetry(JMSConnectionInfo connInfo) {
        ConnectionRetryInfo connRetryInfo = retryConnectionsMap.get(connInfo);
        if (connRetryInfo == null) {
            LOG.info("Queueing connection {0} for retry", connInfo);
            connRetryInfo = new ConnectionRetryInfo(0, retryInitialDelay);
            retryConnectionsMap.put(connInfo, connRetryInfo);
            scheduleRetry(connInfo, retryInitialDelay);
        }
        return connRetryInfo;
    }

    private ConnectionRetryInfo queueTopicForRetry(JMSConnectionInfo connInfo, String topic, MessageHandler msgHandler) {
        LOG.info("Queueing topic {0} for {1} for retry", topic, connInfo);
        ConnectionRetryInfo connRetryInfo = queueConnectionForRetry(connInfo);
        Map<String, MessageHandler> topicsMap = connRetryInfo.getTopicsToRetry();
        topicsMap.put(topic, msgHandler);
        return connRetryInfo;
    }

    private void removeTopicFromRetryList(JMSConnectionInfo connInfo, String topic) {
        LOG.info("Removing topic {0} from {1} from retry list", topic, connInfo);
        ConnectionRetryInfo connRetryInfo = retryConnectionsMap.get(connInfo);
        if (connRetryInfo != null) {
            Map<String, MessageHandler> topicsMap = connRetryInfo.getTopicsToRetry();
            topicsMap.remove(topic);
        }
    }

    private MessageReceiver registerForTopic(JMSConnectionInfo connInfo, ConnectionContext connCtxt, String topic,
            MessageHandler msgHandler) {
        try {
            Session session = connCtxt.createSession(sessionOpts);
            MessageConsumer consumer = connCtxt.createConsumer(session, topic);
            MessageReceiver receiver = new MessageReceiver(msgHandler, session, consumer);
            consumer.setMessageListener(receiver);
            return receiver;
        }
        catch (JMSException e) {
            LOG.warn("Error while registering to listen to topic {0} from {1}", topic, connInfo, e);
            return null;
        }
    }

    public ConnectionContext createConnectionContext(JMSConnectionInfo connInfo) {
        ConnectionContext connCtxt = connectionMap.get(connInfo);
        if (connCtxt == null) {
            try {
                connCtxt = getConnectionContextImpl();
                connCtxt.createConnection(connInfo.getJNDIProperties());
                connCtxt.setExceptionListener(new JMSExceptionListener(connInfo, connCtxt, true));
                connectionMap.put(connInfo, connCtxt);
                LOG.info("Connection established to JMS Server for [{0}]", connInfo);
            }
            catch (Exception e) {
                LOG.warn("Exception while establishing connection to JMS Server for [{0}]", connInfo, e);
                return null;
            }
        }
        return connCtxt;
    }

    public ConnectionContext createProducerConnectionContext(JMSConnectionInfo connInfo) {
        if (jmsProducerConnContext != null && jmsProducerConnContext.isConnectionInitialized()) {
            return jmsProducerConnContext;
        }
        else {
            synchronized (this) {
                if (jmsProducerConnContext == null || !jmsProducerConnContext.isConnectionInitialized()) {
                    try {
                        jmsProducerConnContext = getConnectionContextImpl();
                        jmsProducerConnContext.createConnection(connInfo.getJNDIProperties());
                        jmsProducerConnContext.setExceptionListener(new JMSExceptionListener(connInfo,
                                jmsProducerConnContext, false));
                        LOG.info("Connection established to JMS Server for [{0}]", connInfo);
                    }
                    catch (Exception e) {
                        LOG.warn("Exception while establishing connection to JMS Server for [{0}]", connInfo, e);
                        return null;
                    }
                }
            }
        }
        return jmsProducerConnContext;
    }

    private ConnectionContext getConnectionContextImpl() {
        Class<?> defaultClazz = ConfigurationService.getClass(conf, JMS_CONNECTION_CONTEXT_IMPL);
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
    MessageReceiver getMessageReceiver(JMSConnectionInfo connInfo, String topic) {
        Map<String, MessageReceiver> topicsMap = receiversMap.get(connInfo);
        if (topicsMap != null) {
            return topicsMap.get(topic);
        }
        return null;
    }

    @Override
    public void destroy() {
        LOG.info("Destroying JMSAccessor service ");
        receiversMap.clear();

        LOG.info("Closing JMS connections");
        for (ConnectionContext conn : connectionMap.values()) {
            conn.close();
        }
        if (jmsProducerConnContext != null) {
            jmsProducerConnContext.close();
        }
        connectionMap.clear();
    }

    @Override
    public Class<? extends Service> getInterface() {
        return JMSAccessorService.class;
    }

    /**
     * Reestablish connection for the given JMS connect information
     * @param connInfo JMS connection info
     */
    public void reestablishConnection(JMSConnectionInfo connInfo) {
        // Queue the connection and topics for retry
        connectionMap.remove(connInfo);
        ConnectionRetryInfo connRetryInfo = queueConnectionForRetry(connInfo);
        Map<String, MessageReceiver> listeningTopicsMap = receiversMap.remove(connInfo);
        if (listeningTopicsMap != null) {
            Map<String, MessageHandler> retryTopicsMap = connRetryInfo.getTopicsToRetry();
            for (Entry<String, MessageReceiver> topicEntry : listeningTopicsMap.entrySet()) {
                MessageReceiver receiver = topicEntry.getValue();
                retryTopicsMap.put(topicEntry.getKey(), receiver.getMessageHandler());
            }
        }
    }

    private void scheduleRetry(JMSConnectionInfo connInfo, long delay) {
        LOG.info("Scheduling retry of connection [{0}] in [{1}] seconds", connInfo, delay);
        JMSRetryRunnable runnable = new JMSRetryRunnable(connInfo);
        SchedulerService scheduler = Services.get().get(SchedulerService.class);
        scheduler.schedule(runnable, delay, SchedulerService.Unit.SEC);
    }

    @VisibleForTesting
    boolean retryConnection(JMSConnectionInfo connInfo) {
        ConnectionRetryInfo connRetryInfo = retryConnectionsMap.get(connInfo);
        if (connRetryInfo.getNumAttempt() >= retryMaxAttempts) {
            LOG.info("Not attempting connection [{0}] again. Reached max attempts [{1}]", connInfo, retryMaxAttempts);
            return false;
        }
        LOG.info("Attempting retry of connection [{0}]", connInfo);
        connRetryInfo.setNumAttempt(connRetryInfo.getNumAttempt() + 1);
        connRetryInfo.setNextDelay(connRetryInfo.getNextDelay() * retryMultiplier);
        ConnectionContext connCtxt = createConnectionContext(connInfo);
        boolean shouldRetry = false;
        if (connCtxt == null) {
            shouldRetry = true;
        }
        else {
            Map<String, MessageHandler> retryTopicsMap = connRetryInfo.getTopicsToRetry();
            Map<String, MessageReceiver> listeningTopicsMap = getReceiversTopicsMap(connInfo);
            List<String> topicsToRemoveList = new ArrayList<String>();
            // For each topic in the retry list, try to register the MessageHandler for that topic
            for (Entry<String, MessageHandler> topicEntry : retryTopicsMap.entrySet()) {
                String topic = topicEntry.getKey();
                if (listeningTopicsMap.containsKey(topic)) {
                    continue;
                }
                synchronized (listeningTopicsMap) {
                    if (!listeningTopicsMap.containsKey(topic)) {
                        MessageReceiver receiver = registerForTopic(connInfo, connCtxt, topic, topicEntry.getValue());
                        if (receiver == null) {
                            LOG.warn("Failed to register a listener for topic {0} on {1}", topic, connInfo);
                        }
                        else {
                            listeningTopicsMap.put(topic, receiver);
                            topicsToRemoveList.add(topic);
                            LOG.info("Registered a listener for topic {0} on {1}", topic, connInfo);
                        }
                    }
                }
            }
            for (String topic : topicsToRemoveList) {
                retryTopicsMap.remove(topic);
            }
            if (retryTopicsMap.isEmpty()) {
                shouldRetry = false;
            }
        }

        if (shouldRetry) {
            scheduleRetry(connInfo, connRetryInfo.getNextDelay());
        }
        else {
            retryConnectionsMap.remove(connInfo);
        }
        return true;
    }

    private static class ConnectionRetryInfo {
        private int numAttempt;
        private int nextDelay;
        private Map<String, MessageHandler> retryTopicsMap;

        public ConnectionRetryInfo(int numAttempt, int nextDelay) {
            this.numAttempt = numAttempt;
            this.nextDelay = nextDelay;
            this.retryTopicsMap = new HashMap<String, MessageHandler>();
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

        public Map<String, MessageHandler> getTopicsToRetry() {
            return retryTopicsMap;
        }

    }

    public class JMSRetryRunnable implements Runnable {

        private JMSConnectionInfo connInfo;

        public JMSRetryRunnable(JMSConnectionInfo connInfo) {
            this.connInfo = connInfo;
        }

        public JMSConnectionInfo getJMSConnectionInfo() {
            return connInfo;
        }

        @Override
        public void run() {
            retryConnection(connInfo);
        }

    }

}
