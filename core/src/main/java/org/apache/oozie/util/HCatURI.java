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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class HCatURI {

    public static final String PREFIX_HCAT = "oozie.service.MetaAccessorService.hcat";
    public static final String DEFAULT_SERVER = PREFIX_HCAT + ".server";
    public static final String DEFAULT_DB = PREFIX_HCAT + ".db";
    public static final String DEFAULT_TABLE = PREFIX_HCAT + ".table";
    public static final String PARTITION_SEPARATOR = ";";
    public static final String PARTITION_KEYVAL_SEPARATOR = "=";
    public static final String PATH_SEPARATOR = "/";

    private URI uri;
    private String server;
    private String db;
    private String table;
    private HashMap<String, String> partitions;

    public HCatURI(String s, Configuration conf) throws URISyntaxException {
        parse(s, conf);
    }

    public HCatURI(String s) throws URISyntaxException {
        this(s, null);
    }

    private void parse(String s, Configuration conf) throws URISyntaxException {

        uri = new URI(s);

        server = getValidConf(uri.getAuthority(), conf, DEFAULT_SERVER);
        if (server == null) {
            throw new URISyntaxException(uri.toString(), "HCat Server Name is missing");
        }

        String[] paths = uri.getPath().split(PATH_SEPARATOR, 4);

        if (paths.length != 4) {
            throw new URISyntaxException(uri.toString(), "DB and Table names are not specified properly");
        }

        db = getValidConf(paths[1], conf, DEFAULT_DB);
        if (db == null) {
            throw new URISyntaxException(uri.toString(), "DB name is missing");
        }

        table = getValidConf(paths[2], conf, DEFAULT_TABLE);
        if (table == null) {
            throw new URISyntaxException(uri.toString(), "Table name is missing");
        }

        partitions = new HashMap<String, String>();
        String partRaw = uri.getQuery();
        if (partRaw == null || partRaw.length() == 0) {
            throw new URISyntaxException(uri.toString(), "Partition name is missing");
        }

        String[] parts = partRaw.split(PARTITION_SEPARATOR, -1);
        for (String part : parts) {
            if (part == null || part.length() == 0) {
                continue;
            }
            String[] keyVal = part.split(PARTITION_KEYVAL_SEPARATOR, -1);
            if (keyVal.length != 2) {
                throw new URISyntaxException(uri.toString(), "Parition key value pair is not specified properly in ("
                        + part + ")");
            }
            partitions.put(keyVal[0], keyVal[1]);
        }
    }

    private String getValidConf(String a, Configuration conf, String key) {
        if (a == null || a.length() == 0) {
            if (conf != null) {
                return conf.get(key);
            }
            else {
                return null;
            }
        }
        else {
            return a;
        }
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public HashMap<String, String> getPartitionMap() {
        return partitions;
    }

    public void setPartitionMap(HashMap<String, String> partitions) {
        this.partitions = partitions;
    }

    public String getParitionValue(String key) {
        return partitions.get(key);
    }

    public String setParition(String key, String value) {
        return partitions.put(key, value);
    }

    public boolean hasPartition(String key) {
        return partitions.containsKey(key);
    }

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
