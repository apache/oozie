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

import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.client.event.message.CoordinatorActionMessage;
import org.apache.oozie.client.event.message.JobMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.event.messaging.MessageFactory;
import org.apache.oozie.event.messaging.MessageSerializer;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

/**
 * Class to send JMS notifications related to job events.
 *
 */
public class JMSJobEventListener extends JobEventListener {
    private JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);
    private JMSTopicService jmsTopicService = Services.get().get(JMSTopicService.class);
    private JMSConnectionInfo connInfo;
    public static final String JMS_CONNECTION_PROPERTIES = "oozie.jms.producer.connection.properties";
    public static final String JMS_SESSION_OPTS = "oozie.jms.producer.session.opts";
    public static final String JMS_DELIVERY_MODE = "oozie.jms.delivery.mode";
    public static final String JMS_EXPIRATION_DATE = "oozie.jms.expiration.date";
    private int jmsSessionOpts;
    private int jmsDeliveryMode;
    private int jmsExpirationDate;
    private ConnectionContext jmsContext;
    private static XLog LOG;

    @Override
    public void init(Configuration conf) {
        LOG = XLog.getLog(getClass());
        String jmsProps = ConfigurationService.get(conf, JMS_CONNECTION_PROPERTIES);
        LOG.info("JMS producer connection properties [{0}]", jmsProps);
        connInfo = new JMSConnectionInfo(jmsProps);
        jmsSessionOpts = conf.getInt(JMS_SESSION_OPTS, Session.AUTO_ACKNOWLEDGE);
        jmsDeliveryMode = conf.getInt(JMS_DELIVERY_MODE, DeliveryMode.PERSISTENT);
        jmsExpirationDate = conf.getInt(JMS_EXPIRATION_DATE, 0);

    }

    protected void sendMessage(Map<String, String> messageProperties, String messageBody, String topicName,
            String messageFormat) {
        jmsContext = jmsService.createProducerConnectionContext(connInfo);
        if (jmsContext != null) {
            try {
                Session session = jmsContext.createThreadLocalSession(jmsSessionOpts);
                TextMessage textMessage = session.createTextMessage(messageBody);
                for (Map.Entry<String, String> property : messageProperties.entrySet()) {
                    textMessage.setStringProperty(property.getKey(), property.getValue());
                }
                textMessage.setStringProperty(JMSHeaderConstants.MESSAGE_FORMAT, messageFormat);
                LOG.trace("Event related JMS text body [{0}]", textMessage.getText());
                LOG.trace("Event related JMS entire message [{0}]", textMessage.toString());
                MessageProducer producer = jmsContext.createProducer(session, topicName);
                producer.setDeliveryMode(jmsDeliveryMode);
                producer.setTimeToLive(jmsExpirationDate);
                producer.send(textMessage);
                producer.close();
            }
            catch (JMSException jmse) {
                LOG.error("Exception happened while sending event related jms message", jmse);
            }
        }

    }

    @Override
    public void onWorkflowJobEvent(WorkflowJobEvent event) {
        WorkflowJobMessage wfJobMessage = MessageFactory.createWorkflowJobMessage(event);
        serializeJMSMessage(wfJobMessage, getTopic(event));

    }

    @Override
    public void onCoordinatorActionEvent(CoordinatorActionEvent event) {
        CoordinatorActionMessage coordActionMessage = MessageFactory.createCoordinatorActionMessage(event);
        serializeJMSMessage(coordActionMessage, getTopic(event));
    }

    private void serializeJMSMessage(JobMessage jobMessage, String topicName) {
        MessageSerializer serializer = MessageFactory.getMessageSerializer();
        String messageBody = serializer.getSerializedObject(jobMessage);
        sendMessage(jobMessage.getMessageProperties(), messageBody, topicName, serializer.getMessageFormat());
    }

    protected String getTopic(WorkflowJobEvent event) {
        if (jmsTopicService != null) {
            return jmsTopicService.getTopic(event.getAppType(), event.getUser(), event.getId(), event.getParentId());
        }
        else {
            throw new RuntimeException("JMSTopicService is not initialized");
        }
    }

    protected String getTopic(CoordinatorActionEvent event) {
        if (jmsTopicService != null) {
            return jmsTopicService.getTopic(event.getAppType(), event.getUser(), event.getId(), event.getParentId());
        }
        else {
            throw new RuntimeException("JMSTopicService is not initialized");
        }
    }

    @Override
    public void onWorkflowActionEvent(WorkflowActionEvent wae) {
    }

    @Override
    public void onCoordinatorJobEvent(CoordinatorJobEvent wje) {
    }

    @Override
    public void onBundleJobEvent(BundleJobEvent wje) {
    }

    @Override
    public void destroy() {
}

}
