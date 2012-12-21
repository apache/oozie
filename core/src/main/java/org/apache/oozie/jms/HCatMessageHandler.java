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

import java.util.List;
import java.util.Map;

import javax.jms.Message;

import org.apache.hcatalog.messaging.AddPartitionMessage;
import org.apache.hcatalog.messaging.HCatEventMessage;
import org.apache.hcatalog.messaging.jms.MessagingUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.MetadataServiceException;
import org.apache.oozie.service.PartitionDependencyManagerService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.PartitionWrapper;
import org.apache.oozie.util.XLog;

public class HCatMessageHandler implements MessageHandler {

    private PartitionWrapper msgPartition;
    private static XLog log;
    public static final String THRIFT_SCHEME = "thrift";

    public HCatMessageHandler() {
        log = XLog.getLog(getClass());
    }

    /**
     * Process JMS message produced by HCat.
     *
     * @param msg : to be processed
     * @throws MetadataServiceException
     */
    @Override
    public void process(Message msg) throws MetadataServiceException {
        log.debug("About to process the JMS message ");
        try {
            HCatEventMessage hcatMsg = MessagingUtils.getMessage(msg);
            if (hcatMsg.getEventType().equals(HCatEventMessage.EventType.ADD_PARTITION)) {
                // Parse msg components
                AddPartitionMessage partMsg = (AddPartitionMessage) hcatMsg;
                String server = partMsg.getServer();
                int index = server.indexOf("://");
                server = server.substring(index + 3);
                String db = partMsg.getDB();
                String table = partMsg.getTable();
                log.info("ADD event type db [{0}]  table [{1}] partitions [{2}]", db, table, partMsg.getPartitions());
                PartitionDependencyManagerService pdms = Services.get().get(PartitionDependencyManagerService.class);
                if (pdms != null) {
                    // message is batched. therefore iterate through partitions
                    List<Map<String, String>> partitions = partMsg.getPartitions();
                    for (int i = 0; i < partitions.size(); i++) {
                        msgPartition = new PartitionWrapper(server, db, table, partitions.get(i));
                        if (!pdms.partitionAvailable(msgPartition)) {
                            log.warn(
                                    "Partition map not updated. Message might be incorrect or partition [{0}] might be non-relevant",
                                    msgPartition.toString());
                        }
                        else {
                            log.debug("Partition [{0}] updated from missing -> available in partition map",
                                    msgPartition.toString());
                        }
                    }
                }
                else {
                    log.error("Partition dependency map is NULL");
                }
            }
            else if (hcatMsg.getEventType().equals(HCatEventMessage.EventType.DROP_PARTITION)) {
                log.info("Message is of type [{0}]", HCatEventMessage.EventType.DROP_PARTITION.toString());
            }
            else if (hcatMsg.getEventType().equals(HCatEventMessage.EventType.DROP_TABLE)) {
                log.info("Message is of type [{0}]", HCatEventMessage.EventType.DROP_TABLE.toString());
            }
            else {
                log.info("Unknown event type [{0}] ", hcatMsg.getEventType());
            }
        }
        catch (IllegalArgumentException iae) {
            throw new MetadataServiceException(ErrorCode.E1505, iae);
        }

    }

}
