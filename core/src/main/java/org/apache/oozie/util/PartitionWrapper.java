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

package org.apache.oozie.util;

import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

/**
 * Class to hold the partition related information required by Oozie missing
 * dependency service
 */
public class PartitionWrapper {
    private String serverName;
    private String dbName;
    private String tableName;
    private Map<String, String> partition;
    private static String CONCATENATOR = "#";

    public PartitionWrapper(Map<String, String> partition) {
        this("DEFAULT", "DEFAULT", "DEFAULT", partition);
    }

    public PartitionWrapper(String server, String db, String table, Map<String, String> partition) {
        this.serverName = server;
        this.dbName = db;
        this.tableName = table;
        setPartition(partition);
    }

    public PartitionWrapper(HCatURI hcatUri) {
        this(hcatUri.getServerEndPoint(), hcatUri.getDb(), hcatUri.getTable(), hcatUri.getPartitionMap());
    }

    public PartitionWrapper(String partURI) throws URISyntaxException {
        this(new HCatURI(partURI));
    }

    /**
     * @return the server name
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * @param server the instance to set
     */
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    /**
     * @return the db name
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * @param dbName the dbName to set
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * @return the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param table the table to set
     */
    public void setTableName(String table) {
        this.tableName = table;
    }

    /**
     * @return the partition
     */
    public Map<String, String> getPartition() {
        return partition;
    }

    /**
     * @param partition the partition to set
     */
    public void setPartition(Map<String, String> partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return HCatURI.getHCatURI(serverName, dbName, tableName, partition);
    }

    @Override
    public boolean equals(Object obj) {
        PartitionWrapper pw = (PartitionWrapper) obj;
        Iterator<Map.Entry<String, String>> it1 = partition.entrySet().iterator();
        Map<String, String> p = pw.getPartition();
        boolean equals = true;
        if (this.serverName.equals(pw.serverName) && this.dbName.equals(pw.dbName)
                && this.tableName.equals(pw.tableName) && partition.size() == p.size()) {
            while (it1.hasNext()) {
                String key = it1.next().getKey();
                if (!(p.containsKey(key) && p.get(key).equals(partition.get(key)))) {
                    equals = false;
                }
            }
        }
        else {
            equals = false;
        }
        return equals;
    }

    @Override
    public int hashCode() {
        return serverName.hashCode() + dbName.hashCode() + tableName.hashCode() + partition.hashCode();
    }

    public static String makePrefix(String serverName, String dbName) {
        return serverName + CONCATENATOR + dbName;
    }

}
