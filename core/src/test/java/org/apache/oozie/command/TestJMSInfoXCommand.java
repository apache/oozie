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
package org.apache.oozie.command;

import java.util.Properties;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JMSConnectionInfoBean;
import org.apache.oozie.jms.JMSJobEventListener;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.workflow.WorkflowInstance;
import org.junit.Test;

public class TestJMSInfoXCommand extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    @Test
    public void testConnectionNotEnabled() {
        try {
            WorkflowJobBean wfj = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            new JMSInfoXCommand(wfj.getId()).call();
        }
        catch (CommandException e) {
            assertEquals(ErrorCode.E1601, e.getErrorCode());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testJMSConnectionInfo() {
        try {
            services.destroy();
            services = new Services();
            services.getConf().set(
                    JMSJobEventListener.JMS_CONNECTION_PROPERTIES,
                    "java.naming.factory.initial#" + ActiveMQConnFactory + ";" + "java.naming.provider.url#"
                            + localActiveMQBroker + ";" + "connectionFactoryNames#" + "ConnectionFactory");
            services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, JMSTopicService.class.getName());
            services.init();

            WorkflowJobBean wfj = addRecordToWfJobTable(WorkflowJob.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);
            JMSInfoXCommand jmsInfoCmd = new JMSInfoXCommand(wfj.getId());
            JMSConnectionInfoBean jmsBean = jmsInfoCmd.call();
            assertEquals("test", jmsBean.getTopicName());
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
