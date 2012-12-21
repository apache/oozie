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

import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.service.JMSAccessorService;
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

    public void testRegisterTopic() throws Exception {
        try {
            String topicName = "test-topic";
            MessageReceiver recvr = new MessageReceiver(new MessageHandler() {
                public void process(Message msg) {
                }
            });
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String serverEndPoint = "hcat://hcat.server.com:5080";
            jmsService.getOrCreateConnection(serverEndPoint);
            recvr.registerTopic(serverEndPoint, topicName);
            Thread.sleep(1000);
            recvr.unRegisterTopic(serverEndPoint, topicName);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("\n Unexpected exception " + e);
        }
    }

}
