
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

import java.util.Properties;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JMSConnectionInfoBean;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestJMSServerInfo extends XDataTestCase {

    private Services services;

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().set(
                JMSJobEventListener.JMS_CONNECTION_PROPERTIES,
                "java.naming.factory.initial#" + ActiveMQConnFactory + ";" + "java.naming.provider.url#"
                        + localActiveMQBroker + ";" + "connectionFactoryNames#" + "ConnectionFactory");
        services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, JMSTopicService.class.getName());
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testJMSConnectionInfo() {
        try {
            JMSServerInfo jmsServerInfo = new DefaultJMSServerInfo();
            String connectionProperties = Services.get().getConf()
                    .get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
            WorkflowJobBean wfj = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            JMSConnectionInfoBean jmsBean = jmsServerInfo.getJMSConnectionInfoBean(connectionProperties, wfj.getId());
            assertEquals(wfj.getUser(), jmsBean.getTopicName());
            Properties props = jmsBean.getJNDIProperties();
            assertEquals(ActiveMQConnFactory, props.get("java.naming.factory.initial"));
            assertEquals(localActiveMQBroker, props.get("java.naming.provider.url"));
            assertEquals("ConnectionFactory", props.get("connectionFactoryNames"));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
