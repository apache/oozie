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
package org.apache.oozie.dependency;

import java.net.URI;

import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XHCatTestCase;
import org.junit.Test;

public class TestHCatURIHandler extends XHCatTestCase {

    private URIHandlerService service;
    private JobConf conf;
    private static final String db = "db1";
    private static final String table = "table1";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        service = Services.get().get(URIHandlerService.class);
        conf = createJobConf();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void createTestTable() throws Exception {
        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table);
    }

    private void dropTestTable() throws Exception {
        dropTable(db, table, false);
        dropDatabase(db, false);
    }

    @Test
    public void testExists() throws Exception {
        createTestTable();

        String location = createPartitionDir(db, table, "year=2012&month=12&dt=02&region=us");
        addPartition(db, table, "year=2012&month=12&dt=02&region=us", location);

        URI hcatURI = getHCatURI(db, table, "region=us&year=2012&month=12&dt=02");
        URIHandler handler = service.getURIHandler(hcatURI);
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "year=2012&month=12");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        location = createPartitionDir(db, table, "year=2012&month=12&dt=03&region=us");
        addPartition(db, table, "year=2012&month=12&dt=03&region=us", location);

        hcatURI = getHCatURI(db, table, "region=us&month=12");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "region=us");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "dt=02");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "dt=05");
        assertFalse(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "month=02&dt=02");
        assertFalse(handler.exists(hcatURI, conf, getTestUser()));

        dropTestTable();
    }

    /* Uncomment after bug in hcat fixed
    @Test
    public void testDelete() throws Exception {
        createTestTable();

        String location1 = getPartitionDir(db, table, "year=2012&month=12&dt=02&region=us");
        String location2 = getPartitionDir(db, table, "year=2012&month=12&dt=03&region=us");

        createPartitionForTestDelete(true, true);
        URI hcatURI = getHCatURI(db, table, "year=2012&month=12&dt=02&region=us");
        URIHandler handler = service.getURIHandler(hcatURI);
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));
        assertTrue(handler.delete(hcatURI, conf, getTestUser()));
        assertFalse(getFileSystem().exists(new Path(location1)));

        createPartitionForTestDelete(true, false);
        hcatURI = getHCatURI(db, table, "year=2012&month=12");
        assertTrue(handler.delete(hcatURI, conf, getTestUser()));
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));

        createPartitionForTestDelete(true, true);
        hcatURI = getHCatURI(db, table, "month=12&region=us");
        assertTrue(handler.delete(hcatURI, conf, getTestUser()));
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));

        createPartitionForTestDelete(true, true);
        hcatURI = getHCatURI(db, table, "region=us");
        assertTrue(handler.delete(hcatURI, conf, getTestUser()));
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));

        createPartitionForTestDelete(true, true);
        hcatURI = getHCatURI(db, table, "dt=03");
        assertTrue(handler.delete(hcatURI, conf, getTestUser()));
        assertFalse(getFileSystem().exists(new Path(location2)));

        createPartitionForTestDelete(true, false);
        hcatURI = getHCatURI(db, table, "dt=09");
        assertFalse(handler.delete(hcatURI, conf, getTestUser()));
        assertTrue(getFileSystem().exists(new Path(location1)));
        assertTrue(getFileSystem().exists(new Path(location2)));

        dropTestTable();
    }
    */

    private void createPartitionForTestDelete(boolean partition1, boolean partition2) throws Exception {
        if (partition1) {
            String location = createPartitionDir(db, table, "year=2012&month=12&dt=03&region=us");
            addPartition(db, table, "year=2012&month=12&dt=02&region=us", location);
        }
        if (partition2) {
            String location = createPartitionDir(db, table, "year=2012&month=12&dt=03&region=us");
            addPartition(db, table, "year=2012&month=12&dt=03&region=us", location);
        }
    }

}
