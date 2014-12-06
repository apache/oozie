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

package org.apache.oozie.client.event.jms;

import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.message.CoordinatorActionMessage;
import org.apache.oozie.client.event.message.EventMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;
import org.apache.oozie.client.event.message.SLAMessage;
import org.apache.oozie.AppType;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.JMSException;

/**
 * Class to deserialize the jms message to java object
 */
public abstract class MessageDeserializer {

    /**
     * Constructs the event message from JMS message
     *
     * @param message the JMS message
     * @return EventMessage
     * @throws JMSException
     */
    @SuppressWarnings("unchecked")
    public <T extends EventMessage> T getEventMessage(Message message) throws JMSException {
        TextMessage textMessage = (TextMessage) message;
        String appTypeString = textMessage.getStringProperty(JMSHeaderConstants.APP_TYPE);
        String msgType = textMessage.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE);
        String messageBody = textMessage.getText();
        T eventMsg = null;

        if (appTypeString == null || appTypeString.isEmpty() || messageBody == null || messageBody.isEmpty()) {
            throw new IllegalArgumentException("Could not extract OozieEventMessage. "
                    + "AppType and/or MessageBody is null/empty." + "Apptype is " + appTypeString + " MessageBody is "
                    + messageBody);
        }

        if (MessageType.valueOf(msgType) == MessageType.JOB) {
            switch (AppType.valueOf(appTypeString)) {
                case WORKFLOW_JOB:
                    WorkflowJobMessage wfJobMsg = getDeserializedObject(messageBody, WorkflowJobMessage.class);
                    wfJobMsg.setProperties(textMessage);
                    eventMsg = (T) wfJobMsg;
                    break;
                case COORDINATOR_ACTION:
                    CoordinatorActionMessage caActionMsg = getDeserializedObject(messageBody,
                            CoordinatorActionMessage.class);
                    caActionMsg.setProperties(textMessage);
                    eventMsg = (T) caActionMsg;
                    break;
                default:
                    throw new UnsupportedOperationException("Conversion of " + appTypeString
                            + " to Event message is not supported");
            }
        }
        else if (MessageType.valueOf(msgType) == MessageType.SLA) {
            SLAMessage SLAMsg = getDeserializedObject(messageBody, SLAMessage.class);
            SLAMsg.setProperties(textMessage);
            eventMsg = (T) SLAMsg;
        }

        return eventMsg;
    }

    protected abstract <T> T getDeserializedObject(String s, Class<T> clazz);

}
