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

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.sla.listener.SLAEventListener;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.message.SLAMessage;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.event.messaging.MessageFactory;
import org.apache.oozie.event.messaging.MessageSerializer;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class JMSSLAEventListener extends SLAEventListener {

    private JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
    private JMSTopicService jmsTopicService = Services.get().get(JMSTopicService.class);
    private JMSConnectionInfo connInfo;
    private int jmsSessionOpts;
    private int jmsDeliveryMode;
    private int jmsExpirationDate;
    private ConnectionContext jmsContext;
    private static XLog LOG;

    @Override
    public void init(Configuration conf) throws Exception {
        String jmsProps = conf.get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
        connInfo = new JMSConnectionInfo(jmsProps);
        LOG = XLog.getLog(getClass());
        jmsSessionOpts = conf.getInt(JMSJobEventListener.JMS_SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
        jmsDeliveryMode = conf.getInt(JMSJobEventListener.JMS_DELIVERY_MODE, DeliveryMode.PERSISTENT);
        jmsExpirationDate = conf.getInt(JMSJobEventListener.JMS_EXPIRATION_DATE, 0);
    }

    @Override
    public void onStartMiss(SLAEvent event) {
        sendSLANotification(event);
    }

    @Override
    public void onEndMiss(SLAEvent event) {
        sendSLANotification(event);
    }

    @Override
    public void onDurationMiss(SLAEvent event) {
        sendSLANotification(event);
    }

    @Override
    public void onStartMet(SLAEvent event) {
        sendSLANotification(event);
    }

    @Override
    public void onEndMet(SLAEvent event) {
        sendSLANotification(event);
    }

    @Override
    public void onDurationMet(SLAEvent event) {
        sendSLANotification(event);
    }

    protected void sendSLANotification(SLAEvent event) {
        SLAMessage slaMsg = MessageFactory.createSLAMessage(event);
        MessageSerializer serializer = MessageFactory.getMessageSerializer();
        String messageBody = serializer.getSerializedObject(slaMsg);
        String serializerMessageFormat = serializer.getMessageFormat();
        String topicName = getTopic(event);
        sendJMSMessage(slaMsg, messageBody, topicName, serializerMessageFormat);
    }

    protected void sendJMSMessage(SLAMessage slaMsg, String messageBody, String topicName,
            String messageFormat) {
        jmsContext = jmsService.createProducerConnectionContext(connInfo);
        if (jmsContext != null) {
            try {
                Session session = jmsContext.createThreadLocalSession(jmsSessionOpts);
                TextMessage textMessage = session.createTextMessage(messageBody);
                textMessage.setStringProperty(JMSHeaderConstants.EVENT_STATUS, slaMsg.getEventStatus().toString());
                textMessage.setStringProperty(JMSHeaderConstants.SLA_STATUS, slaMsg.getSLAStatus().toString());
                textMessage.setStringProperty(JMSHeaderConstants.APP_TYPE, slaMsg.getAppType().toString());
                textMessage.setStringProperty(JMSHeaderConstants.MESSAGE_TYPE, slaMsg.getMessageType().toString());
                textMessage.setStringProperty(JMSHeaderConstants.APP_NAME, slaMsg.getAppName());
                textMessage.setStringProperty(JMSHeaderConstants.USER, slaMsg.getUser());
                textMessage.setStringProperty(JMSHeaderConstants.MESSAGE_FORMAT, messageFormat);
                LOG.trace("Event related JMS text body [{0}]", textMessage.getText());
                LOG.trace("Event related JMS message [{0}]", textMessage.toString());
                MessageProducer producer = jmsContext.createProducer(session, topicName);
                producer.setDeliveryMode(jmsDeliveryMode);
                producer.setTimeToLive(jmsExpirationDate);
                producer.send(textMessage);
                producer.close();
            }
            catch (JMSException jmse) {
                LOG.error("Exception happened while sending event related jms message :" + messageBody, jmse);
            }
        }
        else {
            LOG.warn("No connection. Not sending message" + messageBody);
        }
    }

    public String getTopic(SLAEvent event) {
        if (jmsTopicService != null) {
            return jmsTopicService.getTopic(event.getAppType(), event.getUser(), event.getId(), event.getParentId());
        }
        else {
            throw new RuntimeException("JMSTopicService is not initialized");
        }
    }

    @Override
    public void destroy() {
    }
}
