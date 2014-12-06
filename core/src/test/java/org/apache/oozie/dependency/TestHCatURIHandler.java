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
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.test.XHCatTestCase;
import org.junit.Test;

public class TestHCatURIHandler extends XHCatTestCase {

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
    public void testExists() throws Exception {
        createTestTable();

        addPartition(db, table, "year=2012;month=12;dt=02;country=us");

        URI hcatURI = getHCatURI(db, table, "country=us;year=2012;month=12;dt=02");
        URIHandler handler = uriService.getURIHandler(hcatURI);
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "year=2012;month=12");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        addPartition(db, table, "year=2012;month=12;dt=03;country=us");

        hcatURI = getHCatURI(db, table, "country=us;month=12");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "country=us");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "dt=02");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "dt=05");
        assertFalse(handler.exists(hcatURI, conf, getTestUser()));

        hcatURI = getHCatURI(db, table, "month=02;dt=02");
        assertFalse(handler.exists(hcatURI, conf, getTestUser()));

        addPartition(db, table, "year=2012;month=12;dt=04;country=us");

        hcatURI = getHCatURI(db, table, "country=us;year=2012;month=12;dt=04");
        assertTrue(handler.exists(hcatURI, conf, getTestUser()));
        ((HCatURIHandler)handler).delete(hcatURI, conf, getTestUser());
        assertFalse(handler.exists(hcatURI, conf, getTestUser()));

        dropTestTable();
    }

}
