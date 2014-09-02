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

package org.apache.oozie.action.hadoop;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XHCatTestCase;
import org.junit.Test;

public class TestLauncherHCatURIHandler extends XHCatTestCase {

    private Services services;
    private URIHandlerService uriService;
    private JobConf conf;
    private static final String db = "db1";
    private static final String table = "table1";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.getConf().set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
        services.setService(HCatAccessorService.class);
        services.init();
        conf = createJobConf();
        uriService = Services.get().get(URIHandlerService.class);
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    private void createTestTable() throws Exception {
        dropTable(db, table, true);
        dropDatabase(db, true);
        createDatabase(db);
        createTable(db, table, "year,month,dt,country");
    }

    private void dropTestTable() throws Exception {
        dropTable(db, table, false);
        dropDatabase(db, false);
    }

    @Test
    public void testDelete() throws Exception {
        createTestTable();

        String location1 = getPartitionDir(db, table, "year=2012;month=12;dt=02;country=us");
        String location2 = getPartitionDir(db, table, "year=2012;month=12;dt=03;country=us");

        createPartitionForTestDelete(true, true);
        URI hcatURI = getHCatURI(db, table, "year=2012;month=12;dt=02;country=us");
        URIHandler uriHandler = uriService.getURIHandler(hcatURI);
        assertTrue(uriHandler.exists(hcatURI, conf, getTestUser()));
        LauncherURIHandlerFactory uriHandlerFactory = new LauncherURIHandlerFactory(uriService.getLauncherConfig());
        LauncherURIHandler handler = uriHandlerFactory.getURIHandler(hcatURI);
        handler.delete(hcatURI, conf);
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertTrue(getFileSystem().exists(new Path(location2)));
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=02;country=us").size());
        assertEquals(1, getPartitions(db, table, "year=2012;month=12;dt=03;country=us").size());

        createPartitionForTestDelete(true, false);
        hcatURI = getHCatURI(db, table, "year=2012;month=12");
        handler.delete(hcatURI, conf);
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=02;country=us").size());
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=03;country=us").size());

        createPartitionForTestDelete(true, true);
        hcatURI = getHCatURI(db, table, "month=12;country=us");
        handler.delete(hcatURI, conf);
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=02;country=us").size());
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=03;country=us").size());

        createPartitionForTestDelete(true, true);
        hcatURI = getHCatURI(db, table, "country=us");
        handler.delete(hcatURI, conf);
        assertFalse(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=02;country=us").size());
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=03;country=us").size());

        createPartitionForTestDelete(true, true);
        hcatURI = getHCatURI(db, table, "dt=03");
        handler.delete(hcatURI, conf);
        assertTrue(getFileSystem().exists(new Path(location1)));
        assertFalse(getFileSystem().exists(new Path(location2)));
        assertEquals(1, getPartitions(db, table, "year=2012;month=12;dt=02;country=us").size());
        assertEquals(0, getPartitions(db, table, "year=2012;month=12;dt=03;country=us").size());

        createPartitionForTestDelete(false, true);
        hcatURI = getHCatURI(db, table, "dt=09");
        handler.delete(hcatURI, conf);
        assertTrue(getFileSystem().exists(new Path(location1)));
        assertTrue(getFileSystem().exists(new Path(location2)));
        assertEquals(1, getPartitions(db, table, "year=2012;month=12;dt=02;country=us").size());
        assertEquals(1, getPartitions(db, table, "year=2012;month=12;dt=03;country=us").size());

        dropTestTable();
    }

    private void createPartitionForTestDelete(boolean partition1, boolean partition2) throws Exception {
        if (partition1) {
            String location = addPartition(db, table, "year=2012;month=12;dt=02;country=us");
            assertTrue(getFileSystem().exists(new Path(location)));
        }
        if (partition2) {
            String location = addPartition(db, table, "year=2012;month=12;dt=03;country=us");
            assertTrue(getFileSystem().exists(new Path(location)));
        }
    }

}
