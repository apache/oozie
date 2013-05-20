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

import org.apache.oozie.client.event.message.CoordinatorActionMessage;
import org.apache.oozie.client.event.message.EventMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;
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
    public <T extends EventMessage> T getEventMessage(Message message) throws JMSException {
        TextMessage textMessage = (TextMessage) message;
        String appTypeString = textMessage.getStringProperty(JMSHeaderConstants.APP_TYPE);
        String messageBody = textMessage.getText();

        if (appTypeString == null || appTypeString.isEmpty() || messageBody == null || messageBody.isEmpty()) {
            throw new IllegalArgumentException("Could not extract OozieEventMessage. "
                    + "AppType and/or MessageBody is null/empty." + "Apptype is " + appTypeString + " MessageBody is "
                    + messageBody);
        }
        switch (AppType.valueOf(appTypeString)) {
            case WORKFLOW_JOB:
                WorkflowJobMessage wfJobMsg = getDeserializedObject(messageBody, WorkflowJobMessage.class);
                return setProperties(wfJobMsg, textMessage);
            case COORDINATOR_ACTION:
                CoordinatorActionMessage caActionMsg = getDeserializedObject(messageBody,
                        CoordinatorActionMessage.class);
                return setProperties(caActionMsg, textMessage);
            default:
                throw new UnsupportedOperationException("Conversion of " + appTypeString
                        + " to Event message is not supported");
        }

    }

    protected abstract <T> T getDeserializedObject(String s, Class<T> clazz);

    /**
     * Set the JMS selector properties for the workflow job message object
     *
     * @param wfJobMessage the workflow job message
     * @param message the JMS message
     * @return WorkflowJobMessage
     * @throws JMSException
     */
    protected abstract <T extends EventMessage> T setProperties(WorkflowJobMessage wfJobMessage, Message message)
            throws JMSException;

    /**
     * Set the JMS selector properties for coordinator action message
     *
     * @param coordActionMessage the coordinator action message
     * @param message the JMS message
     * @return CoordinatorActionMessage
     * @throws JMSException
     */
    protected abstract <T extends EventMessage> T setProperties(CoordinatorActionMessage coordActionMessage,
            Message message) throws JMSException;

}
