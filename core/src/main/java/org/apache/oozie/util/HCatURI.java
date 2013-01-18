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
import java.util.HashMap;
import java.util.Iterator;
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

        partitions = new HashMap<String, String>();
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
    public boolean equals(Object obj) {
        HCatURI uri = (HCatURI) obj;
        boolean equals = true;
        Map<String, String> p = this.getPartitionMap();

        if (this.getServer().equals(uri.getServer()) && this.db.equals(uri.getDb()) && this.table.equals(uri.getTable())
                && p.size() == uri.getPartitionMap().size()) {
            Iterator<Map.Entry<String, String>> it1 = uri.getPartitionMap().entrySet().iterator();
            while (it1.hasNext()) {
                Map.Entry<String, String> entry = it1.next();
                String key = entry.getKey();
                if (!(p.containsKey(key) && p.get(key).equals(entry.getValue()))) {
                    equals = false;
                }
            }
        }
        else {
            equals = false;
        }
        return equals;
    }

    /**
     * Convert the partition map to filter string. Each key value pair is
     * separated by AND
     *
     * @return filter string
     */
    public String toPigPartitionFilter() {
        StringBuilder filter = new StringBuilder();
        filter.append("(");
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (filter.length() > 1) {
                filter.append(" AND ");
            }
            filter.append(entry.getKey());
            filter.append("==");
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
    public String toPartitionStringHCatStorer() {
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
     * Convert the partition map to filter string. Each key value pair is
     * separated by AND
     *
     * @return filter string
     */
    public String toFilter() {
        StringBuilder filter = new StringBuilder();
        for (Map.Entry<String, String> entry : partitions.entrySet()) {
            if (filter.length() > 0) {
                filter.append(" AND ");
            }
            filter.append(entry.getKey());
            filter.append("=");
            filter.append(PARTITION_VALUE_QUOTE);
            filter.append(entry.getValue());
            filter.append(PARTITION_VALUE_QUOTE);
        }
        return filter.toString();
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
