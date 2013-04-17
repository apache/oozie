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

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.oozie.client.event.Event.AppType;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.message.CoordinatorActionMessage;
import org.apache.oozie.client.event.message.WorkflowJobMessage;

/**
 * Message deserializer to convert from JSON to java object
 */
public class JSONMessageDeserializer extends MessageDeserializer {

    @Override
    public <T> T getDeserializedObject(String messageBody, Class<T> clazz) {
        try {
            return mapper.readValue(messageBody, clazz);
        }
        catch (Exception exception) {
            throw new IllegalArgumentException("Could not deserialize the JMS message using "
                    + clazz.getCanonicalName(), exception);
        }
    }

    @Override
    public WorkflowJobMessage setPropertiesForObject(WorkflowJobMessage workflowJobMsg, Message message)
            throws JMSException {
        workflowJobMsg.setAppType(AppType.valueOf(message.getStringProperty(JMSHeaderConstants.APP_TYPE)));
        workflowJobMsg.setMessageType(MessageType.valueOf(message.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE)));
        workflowJobMsg.setEventStatus(EventStatus.valueOf(message.getStringProperty(JMSHeaderConstants.EVENT_STATUS)));
        workflowJobMsg.setAppName(message.getStringProperty(JMSHeaderConstants.APP_NAME));
        workflowJobMsg.setUser(message.getStringProperty(JMSHeaderConstants.USER));
        return workflowJobMsg;

    }

    @Override
    public CoordinatorActionMessage setPropertiesForObject(CoordinatorActionMessage coordActionMsg, Message message)
            throws JMSException {
        coordActionMsg.setAppType(AppType.valueOf(message.getStringProperty(JMSHeaderConstants.APP_TYPE)));
        coordActionMsg.setMessageType(MessageType.valueOf(message.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE)));
        coordActionMsg.setEventStatus(EventStatus.valueOf(message.getStringProperty(JMSHeaderConstants.EVENT_STATUS)));
        coordActionMsg.setAppName(message.getStringProperty(JMSHeaderConstants.APP_NAME));
        coordActionMsg.setUser(message.getStringProperty(JMSHeaderConstants.USER));
        return coordActionMsg;
    }

}
