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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.MetadataServiceException;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class MessageReceiver implements MessageListener {
    private MessageConsumer consumer;
    private MessageHandler msgHandler;
    private static XLog LOG;

    public MessageReceiver(MessageHandler handler) {
        LOG = XLog.getLog(getClass());
        this.msgHandler = handler;
    }

    /**
     * Get the message consumer object for this message receiver
     *
     * @return MessageConsumer
     */
    public MessageConsumer getConsumer() {
        return this.consumer;
    }

    /**
     * Register a JMS message listener for a specific topic and default end
     * point
     *
     * @param topicName : topic name
     * @throws JMSException
     */
    public void registerTopic(String topicName) throws JMSException {
        registerTopic(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, topicName);
    }

    /**
     * Register a JMS message listener for a specific topic and end point
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine the JMS connection properties
     * @param topicName
     * @throws JMSException
     */
    public void registerTopic(String endPoint, String topicName) throws JMSException {
        JMSAccessorService jas = Services.get().get(JMSAccessorService.class);
        try {
            consumer = jas.getMessageConsumer(endPoint, topicName);
            if(consumer != null) {
                consumer.setMessageListener(this);
                LOG.info("Listener registered for end point:" + endPoint + " topic:" + topicName);
                jas.addTopicReceiver(this, endPoint, topicName);
            }
            else {
                throw new ServiceException(ErrorCode.E1506);
            }
        }
        catch (ServiceException e) {
            LOG.error("Error in registering listener for topic:" + topicName);
            throw new JMSException(e.getMessage());
        }
    }

    /**
     * Unregister and close an existing session with default endpoint
     *
     * @param topicName : name of a topic
     * @throws JMSException
     */
    public void unRegisterTopic(String topicName) throws JMSException {
        unRegisterTopic(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, topicName);
    }

    /**
     * Unregister and close an existing session with default endpoint
     *
     * @param endPoint : Service end-point (preferably HCatalog server address)
     *        to determine
     * @param topicName : name of a topic
     * @throws JMSException
     */
    public void unRegisterTopic(String endPoint, String topicName) throws JMSException {
        Services.get().get(JMSAccessorService.class).removeSession(endPoint, topicName);
        LOG.info("Unregister endPoint :" + endPoint + " topic :" + topicName);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    @Override
    public synchronized void onMessage(Message msg) {
        LOG.trace("Received a JMS message ");
        if (msgHandler != null) {
            try {
                msgHandler.process(msg);
            }
            catch (MetadataServiceException e) {
                LOG.warn("Unable to process message from bus ", e);
            }
        }
        else {
            LOG.info("Message handler none. Unprocessed messsage " + msg);
        }
    }

    @Override
    public void finalize() {
        // Close the session during finalizing
        if (consumer != null) {
            try {
                consumer.close();
            }
            catch (JMSException e) {
                LOG.warn("Unable to close the consumer ", e);
            }
        }
    }
}
