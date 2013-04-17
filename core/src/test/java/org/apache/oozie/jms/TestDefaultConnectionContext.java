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

import javax.jms.Session;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultConnectionContext extends XTestCase {

    private Services services;
    private Session session1;
    private Session session2;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES,
                JMSAccessorService.class.getName() + "," + JMSTopicService.class.getName());
        conf.set(JMSJobEventListener.JMS_CONNECTION_PROPERTIES, "java.naming.factory.initial#"
                + ActiveMQConnFactory + ";" + "java.naming.provider.url#" + localActiveMQBroker
                + ";connectionFactoryNames#" + "ConnectionFactory");
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testThreadLocalSession() {
        String jmsProps = services.getConf().get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
        JMSConnectionInfo connInfo = new JMSConnectionInfo(jmsProps);
        ConnectionContext jmsContext = Services.get().get(JMSAccessorService.class)
                .createConnectionContext(connInfo, false);
        Thread th = new Thread(new SessionThread(jmsContext));
        th.start();
        try {
            th.join();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(session1, session2);

        ThreadLocal<Session> threadLocal1 = jmsContext.createThreadLocalSession(Session.AUTO_ACKNOWLEDGE);
        Session session3 = threadLocal1.get();
        ThreadLocal<Session> threadLocal2 = jmsContext.createThreadLocalSession(Session.AUTO_ACKNOWLEDGE);
        Session session4 = threadLocal2.get();
        // As session3 and session4 are created by same threads, they should be
        // equal
        assertTrue(session3.equals(session4));
        // As session1 and session3 are created by diff threads, they shoudn't
        // be equal
        assertFalse(session1.equals(session3));
        threadLocal1.remove();
        ThreadLocal<Session> threadLocal3 = jmsContext.createThreadLocalSession(Session.AUTO_ACKNOWLEDGE);
        Session session5 = threadLocal3.get();
        assertFalse(session3.equals(session5));

    }

    class SessionThread implements Runnable {

        private ConnectionContext connContext;

        SessionThread(ConnectionContext connContext) {
            this.connContext = connContext;
        }

        @Override
        public void run() {
            ThreadLocal<Session> th1 = connContext.createThreadLocalSession(Session.AUTO_ACKNOWLEDGE);
            session1 = th1.get();
            ThreadLocal<Session> th2 = connContext.createThreadLocalSession(Session.AUTO_ACKNOWLEDGE);
            session2 = th2.get();
        }

    }

}
