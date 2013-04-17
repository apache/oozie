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

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.JMSConnectionInfoBean;
import org.apache.oozie.jms.DefaultJMSServerInfo;
import org.apache.oozie.jms.JMSJobEventListener;
import org.apache.oozie.jms.JMSServerInfo;
import org.apache.oozie.service.JMSAccessorService;
import org.apache.oozie.service.Services;
import org.apache.hadoop.conf.Configuration;

/**
 * Command to create and retrieve the JMS ConnectionInfo bean
 *
 */
public class JMSInfoXCommand extends XCommand<JMSConnectionInfoBean> {

    private final String jobId;
    private final Configuration conf = Services.get().getConf();

    /**
     * Constructor for creating the JMSInfoXcommand
     * @param jobId
     */
    public JMSInfoXCommand(String jobId) {
        super("jms_info", "jms_info", 0);
        this.jobId = jobId;
    }

    protected JMSConnectionInfoBean execute() throws CommandException {
        String connectionProperties = conf.get(JMSJobEventListener.JMS_CONNECTION_PROPERTIES);
        Class<?> defaultClazz = conf.getClass(JMSAccessorService.JMS_PRODUCER_CONNECTION_INFO_IMPL, DefaultJMSServerInfo.class);
        JMSServerInfo jmsServerInfo = null;
        try {
            if (defaultClazz == DefaultJMSServerInfo.class) {
                jmsServerInfo = new DefaultJMSServerInfo();
            }
            else {
                jmsServerInfo = (JMSServerInfo) ReflectionUtils.newInstance(defaultClazz, null);
            }
            JMSConnectionInfoBean jmsBean = jmsServerInfo.getJMSConnectionInfoBean(connectionProperties, jobId);
            return jmsBean;
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1601, e.getMessage(), e);
        }

    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    public String getEntityKey() {
        return jobId;
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

}
