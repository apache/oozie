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
package org.apache.oozie.service;

import java.util.Arrays;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.test.XTestCase;

public class TestJMSAccessorService extends XTestCase {
    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_CLASSES,
                StringUtils.join(",", Arrays.asList(JMSAccessorService.class.getName())));
        conf.set(
                JMSAccessorService.JMS_CONNECTIONS_PROPERTIES,
                "default=java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#vm://localhost?broker.persistent=false,");
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    public void testService() {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        Assert.assertNotNull(jmsService);
    }

    public void testConsumer() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        MessageConsumer consumer = null;
        try {
            consumer = jmsService.getMessageConsumer(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assert (consumer != null);
        }
        finally {
            if (consumer != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    public void testProducer() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        MessageProducer producer = null;
        try {
            producer = jmsService.getMessageProducer(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assert (producer != null);
        }
        finally {
            if (producer != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    public void testSession() throws Exception {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        Session sess = null;
        try {
            sess = jmsService.getSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            assert (sess != null);
        }
        finally {
            if (sess != null) {
                jmsService.removeSession(JMSAccessorService.DEFAULT_SERVER_ENDPOINT, "test-topic");
            }
        }
    }

    public void testConnection() {
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        JMSAccessorService.ConnectionContext conCtx = jmsService
                .getConnectionContext(JMSAccessorService.DEFAULT_SERVER_ENDPOINT);
        assert (conCtx.getConnection() != null);
    }
}
