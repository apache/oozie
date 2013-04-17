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

import org.apache.oozie.client.rest.JMSConnectionInfoBean;
import org.apache.oozie.service.JMSTopicService;
import org.apache.oozie.service.Services;

/**
 * Default implementation to retrieve the JMS ConnectionInfo bean
 */
public class DefaultJMSServerInfo implements JMSServerInfo {
    private JMSTopicService jmsTopicService = Services.get().get(JMSTopicService.class);

    @Override
    public JMSConnectionInfoBean getJMSConnectionInfoBean(String connectionProperties, String jobId) throws Exception {
        JMSConnectionInfoBean jmsBean = new JMSConnectionInfoBean();
        JMSConnectionInfo jmsInfo = new JMSConnectionInfo(connectionProperties);
        jmsBean.setJNDIProperties(jmsInfo.getJNDIProperties());
        if (jmsTopicService != null) {
            jmsBean.setTopicName(jmsTopicService.getTopic(jobId));
        }
        else {
            throw new Exception(
                    "Topic name cannot be retrieved as JMSTopicService is not initialized. JMS notification"
                            + "may not be enabled");
        }
        return jmsBean;
    }

}
