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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatClient.DROP_DB_MODE;
import org.apache.hcatalog.api.HCatCreateDBDesc;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.XLog;

/**
 * Base JUnit <code>TestCase</code> subclass used by all Oozie testcases that
 * need Hadoop FS access and HCat access.
 */
public abstract class XHCatTestCase extends XFsTestCase {

    private static XLog LOG = XLog.getLog(XHCatTestCase.class);
    private static final Random RANDOM = new Random();
    private int msPort;
    private Services services;
    private Configuration hadoopConf;
    private HiveConf hiveConf;
    private HCatClient hcatClient;
    private Thread serverThread;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
        services.init();
        hadoopConf = createJobConf();
        LOG.info("Namenode started at " + getNameNodeUri());
        msPort = RANDOM.nextInt(100) + 30000;
        startMetastoreServer();
        initHiveConf();
        hcatClient = HCatClient.create(hiveConf);
    }

    private void initHiveConf() throws Exception {
        hiveConf = new HiveConf(hadoopConf, this.getClass());
        hiveConf.set("hive.metastore.local", "false");
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTRETRIES, 3);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    }

    private void startMetastoreServer() throws Exception {
        final HiveConf serverConf = new HiveConf(hadoopConf, this.getClass());
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
        Thread.sleep(15000L);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        hcatClient.close();
        serverThread.stop();
        super.tearDown();
    }

    protected Configuration getMetaStoreConf() {
        return hiveConf;
    }

    protected URI getHCatURI(String db, String table, String partitions) throws URISyntaxException {
        String[] parts = partitions.split(",");
        StringBuilder uri = new StringBuilder();
        uri.append("hcat://localhost:").append(msPort).append("/").append(db).append("/").append(table).append("/?");
        for (String partition : parts) {
            uri.append(partition).append("&");
        }
        uri.deleteCharAt(uri.length() - 1);
        return new URI(uri.toString());
    }

    protected void createDatabase(String db) throws Exception {
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(db).ifNotExists(true).location(getFsTestCaseDir().toString())
                .build();
        hcatClient.createDatabase(dbDesc);
        List<String> dbNames = hcatClient.listDatabaseNamesByPattern(db);
        assertTrue(dbNames.contains(db));
    }

    protected void createTable(String db, String table) throws Exception {
        List<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("userid", Type.INT, "userid"));
        cols.add(new HCatFieldSchema("viewtime", Type.BIGINT, "view time"));
        cols.add(new HCatFieldSchema("pageurl", Type.STRING, "page url visited"));
        cols.add(new HCatFieldSchema("ip", Type.STRING, "IP Address of the User"));
        ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
        ptnCols.add(new HCatFieldSchema("year", Type.STRING, "year column"));
        ptnCols.add(new HCatFieldSchema("month", Type.STRING, "month column"));
        ptnCols.add(new HCatFieldSchema("dt", Type.STRING, "date column"));
        ptnCols.add(new HCatFieldSchema("region", Type.STRING, "country column"));
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(db, table, cols).fileFormat("textfile")
                .partCols(ptnCols).build();
        hcatClient.createTable(tableDesc);
        List<String> tables = hcatClient.listTableNamesByPattern(db, "*");
        assertTrue(tables.contains(table));
    }

    protected void dropDatabase(String db, boolean ifExists) throws Exception {
        hcatClient.dropDatabase(db, ifExists, DROP_DB_MODE.CASCADE);
        List<String> dbNames = hcatClient.listDatabaseNamesByPattern(db);
        assertFalse(dbNames.contains(db));
    }

    protected void dropTable(String db, String table, boolean ifExists) throws Exception {
        hcatClient.dropTable(db, table, ifExists);
        List<String> tables = hcatClient.listTableNamesByPattern(db, "*");
        assertFalse(tables.contains(table));
    }

    protected String createPartitionDir(String db, String table, String partitionSpec) throws Exception {
        String dir = getPartitionDir(db, table, partitionSpec);
        getFileSystem().mkdirs(new Path(dir));
        return dir;
    }

    protected String getPartitionDir(String db, String table, String partitionSpec) throws Exception {
        String dir = getFsTestCaseDir() + "/" + db + "/" + table +  "/" + partitionSpec.replaceAll(",", "/");
        return dir;
    }

    protected void addPartition(String db, String table, String partitionSpec, String location) throws Exception {
        String[] parts = partitionSpec.split(",");
        Map<String, String> partitions = new HashMap<String, String>();
        for (String part : parts) {
            String[] split = part.split("=");
            partitions.put(split[0], split[1]);
        }
        HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(db, table, location, partitions).build();
        hcatClient.addPartition(addPtn);
    }

    protected void dropPartition(String db, String table, String partitionSpec) throws Exception {
        String[] parts = partitionSpec.split(",");
        Map<String, String> partitions = new HashMap<String, String>();
        for (String part : parts) {
            String[] split = part.split("=");
            partitions.put(split[0], split[1]);
        }
        hcatClient.dropPartition(db, table, partitions, false);
    }

}
