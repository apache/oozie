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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.oozie.util.XLog;

public class MessageReceiver implements MessageListener {

    private static XLog LOG = XLog.getLog(MessageReceiver.class);

    private MessageHandler msgHandler;
    private Session session;
    private MessageConsumer consumer;

    public MessageReceiver(MessageHandler handler, Session session, MessageConsumer consumer) {
        this.msgHandler = handler;
        this.session = session;
        this.consumer = consumer;
    }

    /**
     * Get the JMS message consumer object for this message receiver
     *
     * @return MessageConsumer
     */
    public MessageConsumer getConsumer() {
        return this.consumer;
    }

    /**
     * Get the MessageHandler that will process the message
     *
     * @return message handler
     */
    public MessageHandler getMessageHandler() {
        return msgHandler;
    }

    /**
     * Get the JMS session for this message receiver
     *
     * @return JMS session
     */
    public Session getSession() {
        return session;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    @Override
    public void onMessage(Message msg) {
        LOG.trace("Received a JMS message ");
        msgHandler.process(msg);
    }

}
