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

package org.apache.oozie.client.event.message;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.oozie.client.event.Event;
import org.apache.oozie.client.event.Event.MessageType;
import org.apache.oozie.client.event.JobEvent.EventStatus;
import org.apache.oozie.client.event.jms.JMSHeaderConstants;
import org.apache.oozie.AppType;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Base class which holds attributes for event message
 *
 */
public abstract class EventMessage {

    private AppType appType;
    private Event.MessageType messageType;

    /**
     * Default constructor for constructing event
     */
    public EventMessage() {
        //Required for jackson
    }

    /**
     * Constructs the event message using message type and app type
     * @param messageType the message type
     * @param appType the app type
     */
    protected EventMessage(MessageType messageType, AppType appType) {
        this.messageType = messageType;
        this.appType = appType;
    }

    /**
     * Sets the appType for a event
     * @param appType
     */
    public void setAppType(AppType appType) {
        this.appType = appType;
    }

    /**
     * Returns the appType for a event
     * @return the AppType
     */
    @JsonIgnore
    public AppType getAppType() {
        return appType;
    }

    /**
     * Sets the message type for a event
     * @param messageType
     */
    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    /**
     * Returns the message type for a event
     * @return the MessageType
     */
    @JsonIgnore
    public MessageType getMessageType() {
        return messageType;
    }

    /**
     * Set the JMS selector properties for message object
     * @param message
     * @throws JMSException
     */
    @JsonIgnore
    public void setProperties(Message message) throws JMSException {
        setAppType(AppType.valueOf(message.getStringProperty(JMSHeaderConstants.APP_TYPE)));
        setMessageType(MessageType.valueOf(message.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE)));
    }

}
