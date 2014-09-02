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
    private static MessageDeserializer deserializer;
    private static Properties jmsDeserializerInfo;
    private static final String CLIENT_PROPERTIES = "oozie_client.properties";

    static {
        InputStream is = JMSMessagingUtils.class.getClassLoader().getResourceAsStream(CLIENT_PROPERTIES);
        if (is == null) {
            System.out.println("Using default JSON Deserializer");
            deserializer = new JSONMessageDeserializer();
        }
        else {
            jmsDeserializerInfo = new Properties();
            try {
                jmsDeserializerInfo.load(is);
                is.close();
            }
            catch (IOException ioe) {
                throw new RuntimeException("I/O error occured for " + CLIENT_PROPERTIES, ioe);
            }
        }

    }

    /**
     * Constructs the EventMessage object from JMS message
     *
     * @param <T>
     * @param msg the JMS message
     * @return the EventMessage
     * @throws IOException
     * @throws JMSException
     */
    public static <T extends EventMessage> T getEventMessage(Message msg) throws IOException, JMSException {
        if (msg == null) {
            throw new IllegalArgumentException("Could not extract EventMessage as JMS message is null");
        }
        if (deserializer == null) {
            String msgFormat = msg.getStringProperty(JMSHeaderConstants.MESSAGE_FORMAT);
            deserializer = getDeserializer(msgFormat);
        }
        return deserializer.getEventMessage(msg);
    }

    private static MessageDeserializer getDeserializer(String msgFormat) throws IOException {
        String deserializerString = (String) jmsDeserializerInfo.get(DESERIALIZER_PROP + msgFormat);
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
