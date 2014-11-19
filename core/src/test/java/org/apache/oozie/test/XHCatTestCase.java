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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.oozie.util.HCatURI;

/**
 * Base JUnit <code>TestCase</code> subclass used by all Oozie testcases that
 * need Hadoop FS access and HCat access.
 */
public abstract class XHCatTestCase extends XFsTestCase {

    private MiniHCatServer hcatServer;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        hcatServer = super.getHCatalogServer();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected Configuration getMetaStoreConf() {
        return hcatServer.getMetaStoreConf();
    }

    public String getMetastoreAuthority() {
        return hcatServer.getMetastoreAuthority();
    }

    protected URI getHCatURI(String db, String table, String partitions) throws URISyntaxException {
        return hcatServer.getHCatURI(db, table, partitions);
    }

    protected void createDatabase(String db) throws Exception {
        if (db.equals("default"))
            return;
        hcatServer.createDatabase(db, getSanitizedTestCaseDir());
    }

    protected void createTable(String db, String table, String partitionCols) throws Exception {
        hcatServer.createTable(db, table, partitionCols);
    }

    protected void dropDatabase(String db, boolean ifExists) throws Exception {
        if (db.equals("default"))
            return;
        hcatServer.dropDatabase(db, ifExists);
    }

    protected void dropTable(String db, String table, boolean ifExists) throws Exception {
        hcatServer.dropTable(db, table, ifExists);
    }

    protected String getPartitionDir(String db, String table, String partitionSpec) throws Exception {
        return hcatServer.getPartitionDir(db, table, partitionSpec, getSanitizedTestCaseDir()).toString();
    }

    /**
     * Add a partition to the table
     * @param db database name
     * @param table table name
     * @param partitionSpec partition key value pairs separated by ;. For eg: year=2011;country=usa
     * @return
     * @throws Exception
     */
    protected String addPartition(String db, String table, String partitionSpec) throws Exception {
        String location = hcatServer.createPartitionDir(db, table, partitionSpec, getSanitizedTestCaseDir());
        hcatServer.addPartition(db, table, partitionSpec, location);
        return location;
    }

    protected String getSanitizedTestCaseDir() {
        // On Windows, the working directory will have a colon from to the drive letter. Because colons
        // are not allowed in DFS paths, we remove it. Also, prepend a backslash to simulate an absolute path.
        if(Shell.WINDOWS) {
            return "\\" + getTestCaseDir().replaceAll(":", "");
        }
        else {
            return getTestCaseDir();
        }
    }

    protected void dropPartition(String db, String table, String partitionSpec) throws Exception {
        hcatServer.dropPartition(db, table, partitionSpec);
    }

    public List<HCatPartition> getPartitions(String db, String table, String partitionSpec) throws Exception {
        return hcatServer.getPartitions(db, table, partitionSpec);
    }

    protected Map<String, String> getPartitionMap(String partitionSpec) {
        String[] parts = partitionSpec.split(HCatURI.PARTITION_SEPARATOR);
        Map<String, String> partitions = new HashMap<String, String>();
        for (String part : parts) {
            String[] split = part.split("=");
            partitions.put(split[0], split[1]);
        }
        return partitions;
    }

}
