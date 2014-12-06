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

package org.apache.oozie.dependency.hcat;

import java.util.List;
import java.util.Map;

import javax.jms.Message;

import org.apache.hive.hcatalog.messaging.AddPartitionMessage;
import org.apache.hive.hcatalog.messaging.HCatEventMessage;
import org.apache.hive.hcatalog.messaging.jms.MessagingUtils;
import org.apache.oozie.jms.MessageHandler;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

public class HCatMessageHandler implements MessageHandler {

    private static XLog LOG = XLog.getLog(HCatMessageHandler.class);

    private final String server;
    private final PartitionDependencyManagerService pdmService;

    public HCatMessageHandler(String server) {
        this.server = server;
        this.pdmService = Services.get().get(PartitionDependencyManagerService.class);
    }

    /**
     * Process JMS message produced by HCat.
     *
     * @param msg : to be processed
     */
    @Override
    public void process(Message msg) {
        try {
            HCatEventMessage hcatMsg = MessagingUtils.getMessage(msg);
            if (hcatMsg.getEventType().equals(HCatEventMessage.EventType.ADD_PARTITION)) {
                // Parse msg components
                AddPartitionMessage partMsg = (AddPartitionMessage) hcatMsg;
                String db = partMsg.getDB();
                String table = partMsg.getTable();
                LOG.info("Partition available event: db [{0}]  table [{1}] partitions [{2}]", db, table,
                        partMsg.getPartitions());
                List<Map<String, String>> partitions = partMsg.getPartitions();
                for (int i = 0; i < partitions.size(); i++) {
                    pdmService.partitionAvailable(this.server, db, table, partitions.get(i));
                }
            }
            else {
                LOG.debug("Ignoring message of event type [{0}] ", hcatMsg.getEventType());
            }
        }
        catch (Exception e) {
            LOG.warn("Error processing JMS message", e);
        }
    }

}
