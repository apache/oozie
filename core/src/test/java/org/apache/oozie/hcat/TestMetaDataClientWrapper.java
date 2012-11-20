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
package org.apache.oozie.hcat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatCreateDBDesc;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.oozie.service.MetaDataAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;

public class TestMetaDataClientWrapper extends XDataTestCase {
    private Services services;
    private String server = "local";
    // private String server = "thrift://localhost:11002"; // to specify the
    // non-local endpoint.
    private String isLocal = "true"; // false for non-local instance

    @Before
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty("hive.metastore.local", isLocal);
        services = new Services();
        addServiceToRun(services.getConf(), MetaDataAccessorService.class.getName());
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    /**
     * Test if query of one specific partition works. Also check the -ve case,
     * if there is not such partition
     *
     * @throws Exception
     */
    public void testGetOnePartition() throws Exception {
        MetaDataClientWrapper clWr = new MetaDataClientWrapper();

        String db = "default";
        String table = "tablename";

        populateTable(server, db, table);
        // +ve test
        Map<String, String> existPartitions = new HashMap<String, String>();
        existPartitions.put("dt", "04/12/2012"); // The same value added in
                                                 // addRecords methods
        existPartitions.put("country", "brazil");

        HCatPartition part = clWr.getOnePartition(server, db, table, existPartitions, getTestUser());
        assertNotNull(part);
        assertEquals(part.getDatabaseName(), db);
        assertEquals(part.getTableName(), table);
        assertTrue(part.getValues().size() == 2);

        // -ve test
        Map<String, String> nonexistPartitions = new HashMap<String, String>();
        nonexistPartitions.put("dt", "04/01/2012"); // The same value added in
                                                    // addRecords methods
        nonexistPartitions.put("country", "brazil2");
        try {
            clWr.getOnePartition(server, db, table, nonexistPartitions, getTestUser());
            fail("Should throw exception earlier.");
        }
        catch (Exception ex) {
            // Expected
        }
    }

    /**
     * Test the query a list of partition based on specific filter.
     *
     * @throws Exception
     */
    public void testGetPartitionsByFilter() throws Exception {
        MetaDataClientWrapper clWr = new MetaDataClientWrapper();
        String db = "default";
        String table = "tablename";

        populateTable(server, db, table);

        String filter = "dt = '04/12/2012' AND country = 'brazil'";

        List<HCatPartition> partList = clWr.getPartitionsByFilter(server, db, table, filter, getTestUser());
        assertNotNull(partList);
        assertTrue(partList.size() == 1);

        filter = "country = 'brazil'";
        partList = clWr.getPartitionsByFilter(server, db, table, filter, getTestUser());
        assertNotNull(partList);
        assertTrue(partList.size() == 2);

        filter = "country = 'mexico'";
        partList = clWr.getPartitionsByFilter(server, db, table, filter, getTestUser());
        assertNotNull(partList);
        assertTrue(partList.size() == 0);
    }

    /**
     * Remove an existing filter.
     *
     * @throws Exception
     */
    public void testDropOnePartition() throws Exception {
        MetaDataClientWrapper clWr = new MetaDataClientWrapper();
        String db = "default";
        String table = "tablename";

        populateTable(server, db, table);
        // +ve test
        Map<String, String> existPartitions = new HashMap<String, String>();
        existPartitions.put("dt", "04/12/2012"); // The same value added in
                                                 // addRecords methods
        existPartitions.put("country", "brazil");

        HCatPartition part = clWr.getOnePartition(server, db, table, existPartitions, getTestUser());
        assertNotNull(part);
        assertEquals(part.getDatabaseName(), db);
        assertEquals(part.getTableName(), table);
        assertTrue(part.getValues().size() == 2);

        clWr.dropOnePartition(server, db, table, existPartitions, true, getTestUser());
        // -ve test
        try {
            HCatPartition part2 = clWr.getOnePartition(server, db, table, existPartitions, getTestUser());
            fail("Should throw exception earlier.");
        }
        catch (Exception ex) {
            // Expected
        }
    }

    private void populateTable(String server, String db, String table) throws Exception {
        createTable(server, db, table);
        addRecords(server, db, table);

    }

    private void createTable(String server, String db, String tableName) throws Exception {
        HCatClient client = services.get(MetaDataAccessorService.class).getHCatClient(server, getTestUser());
        assertNotNull(client);
        // Creating a table
        HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(db).ifNotExists(true).build();
        client.createDatabase(dbDesc);
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema("userid", Type.INT, "id columns"));
        cols.add(new HCatFieldSchema("viewtime", Type.BIGINT, "view time columns"));
        cols.add(new HCatFieldSchema("pageurl", Type.STRING, ""));
        cols.add(new HCatFieldSchema("ip", Type.STRING, "IP Address of the User"));
        ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
        ptnCols.add(new HCatFieldSchema("dt", Type.STRING, "date column"));
        ptnCols.add(new HCatFieldSchema("country", Type.STRING, "country column"));
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(db, tableName, cols).fileFormat("sequencefile")
                .partCols(ptnCols).build();
        client.dropTable(db, tableName, true);
        client.createTable(tableDesc);
        List<String> tables = client.listTableNamesByPattern(db, "*");
        assertTrue(tables.size() > 0);
        assertTrue(tables.contains(tableName));
        List<String> dbNames = client.listDatabaseNamesByPattern(db);
        assertTrue(dbNames.size() == 1);
        assertTrue(dbNames.contains(db));
    }

    private void addRecords(String server, String dbName, String tableName) throws Exception {
        HCatClient client = services.get(MetaDataAccessorService.class).getHCatClient(server, getTestUser());
        Map<String, String> firstPtn = new HashMap<String, String>();
        firstPtn.put("dt", "04/30/2012");
        firstPtn.put("country", "usa");
        HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName, tableName, null, firstPtn).build();
        client.addPartition(addPtn);

        Map<String, String> secondPtn = new HashMap<String, String>();
        secondPtn.put("dt", "04/12/2012");
        secondPtn.put("country", "brazil");
        HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(dbName, tableName, null, secondPtn).build();
        client.addPartition(addPtn2);

        Map<String, String> thirdPtn = new HashMap<String, String>();
        thirdPtn.put("dt", "04/13/2012");
        thirdPtn.put("country", "brazil");
        HCatAddPartitionDesc addPtn3 = HCatAddPartitionDesc.create(dbName, tableName, null, thirdPtn).build();
        client.addPartition(addPtn3);
    }
}
