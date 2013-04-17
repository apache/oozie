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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.oozie.client.event.message.EventMessage;

/**
 * Client utility to convert JMS message to EventMessage object
 */
public class JMSMessagingUtils {

    private static final String DESERIALIZER_PROP = "oozie.msg.deserializer.";

    /**
     * Constructs the EventMessage object from JMS message
     *
     * @param msg the JMS message
     * @return the EventMessage
     * @throws IOException
     * @throws JMSException
     */
    public static EventMessage getEventMessage(Message msg) throws IOException, JMSException {
        if (msg == null) {
            throw new IllegalArgumentException("Could not extract EventMessage as JMS message is null");
        }
        TextMessage textMessage = (TextMessage) msg;
        String msgFormat = msg.getStringProperty(JMSHeaderConstants.MESSAGE_FORMAT);

        Properties jmsDeserializerInfo = new Properties();
        InputStream is = JMSMessagingUtils.class.getClassLoader().getResourceAsStream("oozie_client.properties");

        if (is == null) {
            System.out.println("Using default deserializer");
            return new JSONMessageDeserializer().getEventMessage(textMessage);
        }
        else {
            jmsDeserializerInfo.load(is);
            MessageDeserializer deserializer = getDeserializer((String) jmsDeserializerInfo.get(DESERIALIZER_PROP
                    + msgFormat));
            return deserializer.getEventMessage(textMessage);
        }
    }

    private static MessageDeserializer getDeserializer(String deserializerString) {
        if (deserializerString == null) {
            return new JSONMessageDeserializer();
        }
        else {
            try {
                MessageDeserializer msgDeserializer = (MessageDeserializer) Class.forName(deserializerString)
                        .newInstance();
                return msgDeserializer;
            }
            catch (Exception cnfe) {
                throw new IllegalArgumentException("Could not access class " + deserializerString, cnfe);
            }

        }
    }
}