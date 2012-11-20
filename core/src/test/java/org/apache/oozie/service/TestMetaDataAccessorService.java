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
package org.apache.oozie.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.oozie.test.XDataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMetaDataAccessorService extends XDataTestCase {
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        setSystemProperty("hive.metastore.local", "true");
        Services services = new Services();
        addServiceToRun(services.getConf(), MetaDataAccessorService.class.getName());
        services.init();
    }

    @After
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    /**
     * Test basic service startup
     *
     * @throws Exception
     */
    @Test
    public void testBasicService() throws Exception {
        Services services = Services.get();
        MetaDataAccessorService hcs = services.get(MetaDataAccessorService.class);
        assertNotNull(hcs);
    }

    /**
     * Test successful HCatClient connection and invocations
     *
     * @throws Exception
     */
    @Test
    public void testHCatClientSuccess() throws Exception {
        Services services = Services.get();
        MetaDataAccessorService hcs = services.get(MetaDataAccessorService.class);
        // String server = "thrift://localhost:11002";
        String server = "local";
        String db = "default";
        String tableName = "mytable";
        HCatClient client = null;
        try {
            client = hcs.getHCatClient(server, getTestUser());
            assertNotNull(client);
            // Creating a table
            ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
            cols.add(new HCatFieldSchema("id", Type.INT, "id columns"));
            cols.add(new HCatFieldSchema("value", Type.STRING, "id columns"));
            client.dropTable(db, tableName, true);
            HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(null, tableName, cols).fileFormat("rcfile")
                    .build();
            client.createTable(tableDesc);
            List<String> tables = client.listTableNamesByPattern(db, "*");
            assertTrue(tables.size() > 0);
            assertTrue(tables.contains(tableName));
            List<String> dbNames = client.listDatabaseNamesByPattern(db);
            assertTrue(dbNames.size() == 1);
            assertTrue(dbNames.get(0).equals(db));
            // Dropping the table
            client.dropTable(db, tableName, true);
        }
        finally {
            if (client != null) {
                client.close();
            }
        }
    }

}
