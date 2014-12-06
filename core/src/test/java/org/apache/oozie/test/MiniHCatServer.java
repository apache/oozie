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

package org.apache.oozie.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatClient.DropDBMode;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.oozie.util.HCatURI;
import org.apache.oozie.util.XLog;
import org.junit.Assert;

public class MiniHCatServer {

    public static enum RUNMODE {
        LOCAL, SERVER
    };

    private static XLog LOG = XLog.getLog(MiniHCatServer.class);
    private static final Random RANDOM = new Random();
    private RUNMODE mode;
    private Configuration hadoopConf;
    private int msPort;
    private HiveConf hiveConf;
    private HCatClient hcatClient;
    private Thread serverThread;
    private Map<String, String> sysProps;

    public MiniHCatServer(RUNMODE mode, Configuration hadoopConf) throws Exception {
        this.mode = mode;
        this.hadoopConf = hadoopConf;
        sysProps = new HashMap<String, String>();
    }

    public void start() throws Exception {
        if (mode.equals(RUNMODE.LOCAL)) {
            initLocalMetastoreConf();
        }
        else {
            this.msPort = RANDOM.nextInt(100) + 30000;
            startMetastoreServer();
            initMetastoreServerConf();
        }
        hcatClient = HCatClient.create(hiveConf);
        resetDefaultDBCreation();
    }

    public void shutdown() throws Exception {
        resetSystemProperties();
        hcatClient.close();
        if (mode.equals(RUNMODE.SERVER)) {
            // No clean way to stop hive metastore server.
            serverThread.stop();
        }
    }

    private void initLocalMetastoreConf() throws IOException {
        hiveConf = new HiveConf(hadoopConf, this.getClass());
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new File("target/warehouse").getAbsolutePath());
        hiveConf.set("hive.metastore.local", "true"); // For hive 0.9
        hiveConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:target/metastore_db;create=true");

        setSystemProperty("hive.metastore.local", "true");
        setSystemProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, new File("target/warehouse").getAbsolutePath());
        setSystemProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
                "jdbc:derby:target/metastore_db;create=true");
        File derbyLogFile = new File("target/derby.log");
        derbyLogFile.createNewFile();
        setSystemProperty("derby.stream.error.file", derbyLogFile.getPath());
    }

    private void initMetastoreServerConf() throws Exception {

        hiveConf = new HiveConf(hadoopConf, this.getClass());
        hiveConf.set("hive.metastore.local", "false"); // For hive 0.9
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    }

    private void startMetastoreServer() throws Exception {
        final HiveConf serverConf = new HiveConf(hadoopConf, this.getClass());
        serverConf.set("hive.metastore.local", "false");
        serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:target/metastore_db;create=true");
        //serverConf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname, NotificationListener.class.getName());
        File derbyLogFile = new File("target/derby.log");
        derbyLogFile.createNewFile();
        setSystemProperty("derby.stream.error.file", derbyLogFile.getPath());
        serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    HiveMetaStore.startMetaStore(msPort, ShimLoader.getHadoopThriftAuthBridge(), serverConf);
                    LOG.info("Started metastore server on port " + msPort);
                }
                catch (Throwable e) {
                    LOG.error("Metastore Thrift Server threw an exception...", e);
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
        Thread.sleep(10000L);
    }

    public static void resetDefaultDBCreation() throws Exception {
        // Need to do this, else default db will not be created for local metastores.
        // TestHiveMain will fail with InvalidObjectException(message:There is no database named default)
        Field declaredField = HMSHandler.class.getDeclaredField("createDefaultDB");
        declaredField.setAccessible(true);
        declaredField.set(null, false);
    }

    public static void resetHiveConfStaticVariables() throws Exception {
        // HiveConf initializes location of hive-site.xml in static block.
        // So this is needed so that tests like TestHiveMain that create hive-site.xml don't fail.
        Field declaredField = HiveConf.class.getDeclaredField("hiveSiteURL");
        declaredField.setAccessible(true);
        declaredField.set(null, HiveConf.class.getClassLoader().getResource("hive-site.xml"));
    }

    private void setSystemProperty(String name, String value) {
        if (!sysProps.containsKey(name)) {
            String currentValue = System.getProperty(name);
            sysProps.put(name, currentValue);
        }
        if (value != null) {
            System.setProperty(name, value);
        }
        else {
            System.getProperties().remove(name);
        }
    }

    private void resetSystemProperties() {
        for (Map.Entry<String, String> entry : sysProps.entrySet()) {
            if (entry.getValue() != null) {
                System.setProperty(entry.getKey(), entry.getValue());
            }
            else {
                System.getProperties().remove(entry.getKey());
            }
        }
        sysProps.clear();
    }

    public Configuration getMetaStoreConf() {
        return hiveConf;
    }

    public String getMetastoreAuthority() {
        if (mode.equals(RUNMODE.SERVER)) {
            return "localhost:" + msPort;
        }
        else {
            return "unittest-local";
        }
    }

    public String getMetastoreURI() {
        return hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname);
    }

    public HCatClient getHCatClient() {
        return hcatClient;
    }

    public URI getHCatURI(String db, String table, String partitions) throws URISyntaxException {
        StringBuilder uri = new StringBuilder();
        uri.append("hcat://").append(getMetastoreAuthority()).append("/").append(db).append("/").append(table)
                .append("/").append(partitions);
        return new URI(uri.toString());
    }

    public void createDatabase(String db, String location) throws Exception {
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(db).ifNotExists(true).location(location).build();
        hcatClient.createDatabase(dbDesc);
        List<String> dbNames = hcatClient.listDatabaseNamesByPattern(db);
        Assert.assertTrue(dbNames.contains(db));
    }

    public void createTable(String db, String table, String partitionCols) throws Exception {
        List<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("userid", Type.INT, "userid"));
        cols.add(new HCatFieldSchema("viewtime", Type.BIGINT, "view time"));
        cols.add(new HCatFieldSchema("pageurl", Type.STRING, "page url visited"));
        cols.add(new HCatFieldSchema("ip", Type.STRING, "IP Address of the User"));
        ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
        for (String partitionCol : partitionCols.split(",")) {
            ptnCols.add(new HCatFieldSchema(partitionCol, Type.STRING, null));
        }
        // Remove this once NotificationListener is fixed and available in HCat snapshot
        Map<String, String> tblProps = new HashMap<String, String>();
        tblProps.put(HCatConstants.HCAT_MSGBUS_TOPIC_NAME, "hcat." + db + "." + table);
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(db, table, cols).fileFormat("textfile")
                .partCols(ptnCols).tblProps(tblProps ).build();
        hcatClient.createTable(tableDesc);
        List<String> tables = hcatClient.listTableNamesByPattern(db, "*");
        assertTrue(tables.contains(table));
    }

    public void dropDatabase(String db, boolean ifExists) throws Exception {
        hcatClient.dropDatabase(db, ifExists, DropDBMode.CASCADE);
        List<String> dbNames = hcatClient.listDatabaseNamesByPattern(db);
        assertFalse(dbNames.contains(db));
    }

    public void dropTable(String db, String table, boolean ifExists) throws Exception {
        hcatClient.dropTable(db, table, ifExists);
        List<String> tables = hcatClient.listTableNamesByPattern(db, "*");
        assertFalse(tables.contains(table));
    }

    public String getPartitionDir(String db, String table, String partitionSpec, String dbLocation) throws Exception {
        String dir = dbLocation + "/" + db + "/" + table + "/"
                + partitionSpec.replaceAll(HCatURI.PARTITION_SEPARATOR, "/");
        return dir;
    }

    public String createPartitionDir(String db, String table, String partitionSpec, String dbLocation) throws Exception {
        String dir = getPartitionDir(db, table, partitionSpec, dbLocation);
        FileSystem.get(hadoopConf).mkdirs(new Path(dir));
        return dir;
    }

    public void addPartition(String db, String table, String partitionSpec, String location) throws Exception {
        String[] parts = partitionSpec.split(HCatURI.PARTITION_SEPARATOR);
        Map<String, String> partitions = new HashMap<String, String>();
        for (String part : parts) {
            String[] split = part.split("=");
            partitions.put(split[0], split[1]);
        }
        HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(db, table, location, partitions).build();
        hcatClient.addPartition(addPtn);
        assertNotNull(hcatClient.getPartition(db, table, partitions));
    }

    public void dropPartition(String db, String table, String partitionSpec) throws Exception {
        String[] parts = partitionSpec.split(HCatURI.PARTITION_SEPARATOR);
        Map<String, String> partitions = new HashMap<String, String>();
        for (String part : parts) {
            String[] split = part.split("=");
            partitions.put(split[0], split[1]);
        }
        hcatClient.dropPartitions(db, table, partitions, false);
    }

    public List<HCatPartition> getPartitions(String db, String table, String partitionSpec) throws Exception {
        String[] parts = partitionSpec.split(HCatURI.PARTITION_SEPARATOR);
        Map<String, String> partitions = new HashMap<String, String>();
        for (String part : parts) {
            String[] split = part.split("=");
            partitions.put(split[0], split[1]);
        }
        return hcatClient.getPartitions(db, table, partitions);
    }

}
