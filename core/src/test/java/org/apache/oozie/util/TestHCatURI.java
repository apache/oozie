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

import static org.junit.Assert.*;
import java.net.URISyntaxException;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.HCatURI;

public class TestHCatURI {

    @Test
    public void testHCatURIParseValidURI() {
        String input = "hcat://hcat.yahoo.com:5080/mydb/clicks/?datastamp=12;region=us";
        HCatURI uri = null;
        try{
            uri= new HCatURI(input);
        }catch (Exception ex){
            System.err.print(ex.getMessage());
        }
        assertEquals(uri.getServer(),"hcat.yahoo.com:5080");
        assertEquals(uri.getDb(),"mydb");
        assertEquals(uri.getTable(),"clicks");
        assertEquals(uri.getPartitionValue("datastamp"),"12");
        assertEquals(uri.getPartitionValue("region"),"us");

    }

    @Test
    public void testHCatURIParseWithDefaultServer() {

        String input = "hcat:///mydb/clicks/?datastamp=12;region=us";
        Configuration conf = new Configuration(false);
        conf.set("oozie.service.MetaAccessorService.hcat.server", "hcat.yahoo.com:5080");
        conf.set("oozie.service.MetaAccessorService.hcat.db", "mydb");
        conf.set("oozie.service.MetaAccessorService.hcat.table", "clicks");

        HCatURI uri = null;
        try{
            uri= new HCatURI(input,conf);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
        assertEquals(uri.getServer(),"hcat.yahoo.com:5080");
        assertEquals(uri.getDb(),"mydb");
        assertEquals(uri.getTable(),"clicks");
        assertEquals(uri.getPartitionValue("datastamp"),"12");
        assertEquals(uri.getPartitionValue("region"),"us");
    }

    @Test
    public void testHCatURIParseWithDefaultDB() {

        String input = "hcat://hcat.yahoo.com:5080//clicks/?datastamp=12;region=us";
        Configuration conf = new Configuration(false);
        conf.set("oozie.service.MetaAccessorService.hcat.server", "hcat.yahoo.com:5080");
        conf.set("oozie.service.MetaAccessorService.hcat.db", "mydb");
        conf.set("oozie.service.MetaAccessorService.hcat.table", "clicks");

        HCatURI uri = null;
        try{
            uri= new HCatURI(input,conf);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
        assertEquals(uri.getServer(),"hcat.yahoo.com:5080");
        assertEquals(uri.getDb(),"mydb");
        assertEquals(uri.getTable(),"clicks");
        assertEquals(uri.getPartitionValue("datastamp"),"12");
        assertEquals(uri.getPartitionValue("region"),"us");
    }

    @Test
    public void testHCatURIParseWithDefaultTable() {

        String input = "hcat://hcat.yahoo.com:5080/mydb//?datastamp=12;region=us";
        Configuration conf = new Configuration(false);
        conf.set("oozie.service.MetaAccessorService.hcat.server", "hcat.yahoo.com:5080");
        conf.set("oozie.service.MetaAccessorService.hcat.db", "mydb");
        conf.set("oozie.service.MetaAccessorService.hcat.table", "clicks");

        HCatURI uri = null;
        try{
            uri= new HCatURI(input,conf);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
        assertEquals(uri.getServer(),"hcat.yahoo.com:5080");
        assertEquals(uri.getDb(),"mydb");
        assertEquals(uri.getTable(),"clicks");
        assertEquals(uri.getPartitionValue("datastamp"),"12");
        assertEquals(uri.getPartitionValue("region"),"us");
    }


    @Test
    public void testHCatURIParseWithAllDefault() {

        String input = "hcat://///?datastamp=12;region=us";
        Configuration conf = new Configuration(false);
        conf.set("oozie.service.MetaAccessorService.hcat.server", "hcat.yahoo.com:5080");
        conf.set("oozie.service.MetaAccessorService.hcat.db", "mydb");
        conf.set("oozie.service.MetaAccessorService.hcat.table", "clicks");

        HCatURI uri = null;
        try{
            uri= new HCatURI(input,conf);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
        assertEquals(uri.getServer(),"hcat.yahoo.com:5080");
        assertEquals(uri.getDb(),"mydb");
        assertEquals(uri.getTable(),"clicks");
        assertEquals(uri.getPartitionValue("datastamp"),"12");
        assertEquals(uri.getPartitionValue("region"),"us");
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseInvalidURI() throws Exception{
        String input = "hcat://hcat.yahoo.com:5080/ mydb/clicks/?datastamp=12;region=us";
        HCatURI uri = new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseInvalidPartition() throws Exception{
        String input = "hcat://hcat.yahoo.com:5080/mydb/clicks/?datastamp";
        HCatURI uri = new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseServerMissing() throws Exception{
        String input = "hcat:///mydb/clicks/?datastamp=12;region=us";
        HCatURI uri = new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseDBMissing() throws Exception{
        String input = "hcat://hcat.yahoo.com:5080//clicks/?datastamp=12;region=us";
        HCatURI uri = new HCatURI(input);
    }

    @Test(expected = URISyntaxException.class)
    public void testHCatURIParseTableMissing() throws Exception{
        String input = "hcat://hcat.yahoo.com:5080/mydb//?datastamp=12;region=us";
        HCatURI uri = new HCatURI(input);
    }
}
