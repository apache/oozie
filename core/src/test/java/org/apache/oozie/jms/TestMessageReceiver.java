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
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;

import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;

public class TestMessageReceiver extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = super.setupServicesForHCatalog();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testMessage() throws Exception {
        String topicName = "test-topic";
        MessageReceiver recvr = new MessageReceiver(new MessageHandler() {
            public void process(Message msg) {
                if (msg instanceof ObjectMessage) {
                    System.out.println("Object Message: " + msg);
                }
                else if (msg instanceof MapMessage) {
                    System.out.println("MapMessage : " + msg);
                }
                else {
                    try {
                        System.out.println("Unexpected message type " + msg.getJMSType());
                    }
                    catch (JMSException e) {
                        System.out.println("Unable to read the type " + e);
                    }
                }
            }
        });
        recvr.registerTopic(topicName);
        Thread.sleep(1000);
        recvr.unRegisterTopic(topicName);
    }

}
