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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.test.XHCatTestCase;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.junit.Test;

public class TestHCatPrepareActions extends XHCatTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Services services = new Services();
        services.getConf().set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    @Test
    public void testDelete() throws Exception {
        dropTable("db1", "table1", true);
        dropDatabase("db1", true);
        createDatabase("db1");
        createTable("db1", "table1", "year,month,dt,country");
        String part1 = addPartition("db1", "table1", "year=2012;month=12;dt=02;country=us");
        String part2 = addPartition("db1", "table1", "year=2012;month=12;dt=03;country=us");
        String part3 = addPartition("db1", "table1", "year=2013;month=1;dt=01;country=us");

        String uri1 = "hcat://" + getMetastoreAuthority() + "/db1/table1/year=2012;month=12";
        String uri2 = "hcat://" + getMetastoreAuthority() + "/db1/table1/year=2013;dt=01";

        // Prepare block that contains delete action
        String prepareXML = "<prepare>"
                + "<delete path='" + uri1 + "'/>"
                + "<delete path='" + uri2 + "'/>"
                + "</prepare>";

        JobConf conf = createJobConf();
        LauncherMapperHelper.setupLauncherURIHandlerConf(conf);
        PrepareActionsDriver.doOperations(prepareXML, conf);
        FileSystem fs = getFileSystem();
        assertFalse(fs.exists(new Path(part1)));
        assertFalse(fs.exists(new Path(part2)));
        assertFalse(fs.exists(new Path(part3)));
        assertEquals(0, getPartitions("db1", "table1", "country=us").size());
        dropTable("db1", "table1", true);
        dropDatabase("db1", true);
    }

}
