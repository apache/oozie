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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility class to parse HCatalog URI
 */
public class HCatURI {

    public static final String PARTITION_SEPARATOR = ";";
    public static final String PARTITION_KEYVAL_SEPARATOR = "=";
    public static final String PATH_SEPARATOR = "/";
    public static final String PARTITION_VALUE_QUOTE = "'";

    private URI uri;
    private String db;
    private String table;
    private Map<String, String> partitions;

    /**
     * Constructor using default configuration
     *
     * @param s HCat URI String
     * @throws URISyntaxException
     */
    public HCatURI(String s) throws URISyntaxException {
        this(new URI(s));
    }

    public HCatURI(URI uri) throws URISyntaxException {
        parse(uri);
    }

    private void parse(URI uri) throws URISyntaxException {

        this.uri = uri;

        if (uri.getAuthority() == null) {
            throw new URISyntaxException(uri.toString(), "Server host and port are missing");
        }

        String[] paths = uri.getPath().split(PATH_SEPARATOR);

        if (paths.length != 4) {
            throw new URISyntaxException(uri.toString(), "URI path is not in expected format");
        }

        db = paths[1];
        table = paths[2];
        String partRaw = paths[3];

        if (db == null || db.length() == 0) {
            throw new URISyntaxException(uri.toString(), "DB name is missing");
        }
        if (table == null || table.length() == 0) {
            throw new URISyntaxException(uri.toString(), "Table name is missing");
        }
        if (partRaw == null || partRaw.length() == 0) {
            throw new URISyntaxException(uri.toString(), "Partition details are missing");
        }

        partitions = new LinkedHashMap<String, String>();
        String[] parts = partRaw.split(PARTITION_SEPARATOR);
        for (String part : parts) {
            if (part == null || part.length() == 0) {
                continue;
            }
            String[] keyVal = part.split(PARTITION_KEYVAL_SEPARATOR);
            if (keyVal.length != 2) {
                throw new URISyntaxException(uri.toString(), "Partition key value pair is not specified properly in ("
                        + part + ")");
            }
            partitions.put(keyVal[0], keyVal[1]);
        }
    }

    public URI getURI() {
        return uri;
    }

    public String toURIString() {
        return uri.toString();
    }

    /**
     * @return fully qualified server address
     */
    public String getServerEndPoint() {
        return uri.getScheme() + "://" + uri.getAuthority();
    }

    /**
     * @return server host:port
     */
    public String getServer() {
        return uri.getAuthority();
    }

    /**
     * @return DB name
     */
    public String getDb() {
        return db;
    }

    /**
     * @return table name
     */
    public String getTable() {
        return table;
    }

    /**
     * @return partitions map
     */
    public Map<String, String> getPartitionMap() {
        return partitions;
    }


    /**
     * @param key partition key
     * @return partition value
     */
    public String getPartitionValue(String key) {
        return partitions.get(key);
    }

    /**
     * @param key partition key to set
     * @param value partition value to set
     */
    public void setPartition(String key, String value) {
        partitions.put(key, value);
    }

    /**
     * @param key partition key
     * @return if partitions map includes the key or not
     */
    public boolean hasPartition(String key) {
        return partitions.containsKey(key);
    }

    public static String getHCatURI(String server, String db, String table, Map<String, String> partitions) {
        return getHCatURI("hcat", server, db, table, partitions);
    }

    /**
     * static method to create HCatalog URI String
     *
     * @param server
     * @param db
     * @param table
     * @param partitions Partition Map
     * @return
     */
    public static String getHCatURI(String scheme, String server, String db, String table, Map<String, String> partitions) {

        StringBuilder sb = new StringBuilder();
        sb.append(scheme);
        sb.append("://");
        sb.append(server);
        sb.append(PATH_SEPARATOR);
        sb.append(db);
        sb.append(PATH_SEPARATOR);
        sb.append(table);
        sb.append(PATH_SEPARATOR);
        for (Entry<String, String> entry : partitions.entrySet()) {
            sb.append(entry.getKey());
            sb.append(PARTITION_KEYVAL_SEPARATOR);
            sb.append(entry.getValue());
            sb.append(PARTITION_SEPARATOR);
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return (uri == null) ? 0 : uri.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HCatURI other = (HCatURI) obj;
        return uri.equals(other.uri);
    }

    /**
     * Convert the partition map to filter string. Each key value pair is
     * separated by AND
     *
     * @param type pig/java/mr/hive
     * @return filter string
     */
    public String toPartitionFilter(String type) {
        StringBuilder filter = new StringBuilder();
        String comparator = null;
        filter.append("(");
        comparator = type.equalsIgnoreCase("pig") ? "==" : "=";
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (filter.length() > 1) {
                filter.append(" AND ");
            }
            filter.append(entry.getKey());
            filter.append(comparator);
            filter.append(PARTITION_VALUE_QUOTE);
            filter.append(entry.getValue());
            filter.append(PARTITION_VALUE_QUOTE);
        }
        filter.append(")");
        return filter.toString();
    }

    /**
     * Get the partition as string for HCatStorer argument.
     *
     * @return filter string
     */
    public String toPartitionString() {
        StringBuilder filter = new StringBuilder();
        filter.append("'");
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (filter.length() > 1) {
                filter.append(",");
            }
            filter.append(entry.getKey());
            filter.append("=");
            filter.append(entry.getValue());
        }
        filter.append("'");
        return filter.toString();
    }

    /**
     * Get the entire partition value string from partition map.
     * In case of type hive-export, it can be used to create entire partition value string
     * that can be used in Hive query for partition export/import.
     *
     * type hive-export
     * @return partition value string
     */
    public String toPartitionValueString(String type) {
        StringBuilder value = new StringBuilder();
        if (type.equals("hive-export")) {
            String comparator = "=";
            String separator = ",";
            for (Map.Entry<String, String> entry : partitions.entrySet()) {
                if (value.length() > 1) {
                    value.append(separator);
                }
                value.append(entry.getKey());
                value.append(comparator);
                value.append(PARTITION_VALUE_QUOTE);
                value.append(entry.getValue());
                value.append(PARTITION_VALUE_QUOTE);
            }
        } else {
            throw new RuntimeException("Unsupported type: " + type);
        }
        return value.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("URI: ");
        sb.append(uri.toString());
        sb.append("\n");
        sb.append("SCHEME: ");
        sb.append(uri.getScheme());
        sb.append("\n");
        sb.append("SERVER: ");
        sb.append(getServer());
        sb.append("\n");
        sb.append("DB: ");
        sb.append(getDb());
        sb.append("\n");
        sb.append("TABLE: ");
        sb.append(getTable());
        int partcnt = 0;
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            sb.append("\n");
            sb.append("PARTITION(" + partcnt + "): ");
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            partcnt++;
        }
        return sb.toString();
    }
}
