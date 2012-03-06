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

import org.apache.oozie.test.XTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class TestHadoopAccessorService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-hadoop-config.xml");
        OutputStream os = new FileOutputStream(new File(getTestCaseConfDir(), "test-hadoop-config.xml"));
        IOUtils.copyStream(is, os);
        setSystemProperty("oozie.service.HadoopAccessorService.hadoop.configurations",
                          "*=hadoop-config.xml,test=test-hadoop-config.xml");
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            setSystemProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "true");
            setSystemProperty("oozie.service.HadoopAccessorService.keytab.file", getKeytabFile());
            setSystemProperty("oozie.service.HadoopAccessorService.kerberos.principal", getOoziePrincipal());
        }
        Services services = new Services();
        services.init();
    }

    protected void tearDown() throws Exception {
        Services.get().destroy();
        super.tearDown();
    }

    public void testService() throws Exception {
        Services services = Services.get();
        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        assertNotNull(has);
        assertNotNull(has.getConfiguration("*"));
        assertNotNull(has.getConfiguration("test"));
        assertEquals("bar", has.getConfiguration("test").get("foo"));
    }
    public void testAccessor() throws Exception {
        Services services = Services.get();
        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        JobConf conf = new JobConf();
        conf.set("mapred.job.tracker", getJobTrackerUri());
        conf.set("fs.default.name", getNameNodeUri());
        injectKerberosInfo(conf);
        URI uri = new URI(getNameNodeUri());

        //valid user
        String user = getTestUser();
        String group = getTestGroup();

        JobClient jc = has.createJobClient(user, group, conf);
        assertNotNull(jc);
        FileSystem fs = has.createFileSystem(user, group, conf);
        assertNotNull(fs);
        fs = has.createFileSystem(user, group, uri, conf);
        assertNotNull(fs);

        //invalid user

        user = "invalid";

        try {
            has.createJobClient(user, group, conf);
            fail();
        }
        catch (Throwable ex) {
        }

        try {
            has.createFileSystem(user, group, conf);
            fail();
        }
        catch (Throwable ex) {
        }

        try {
            has.createFileSystem(user, group, uri, conf);
            fail();
        }
        catch (Throwable ex) {
        }
    }

}
