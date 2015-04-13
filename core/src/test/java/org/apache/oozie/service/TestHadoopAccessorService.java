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
import org.apache.hadoop.io.Text;
import org.apache.oozie.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.XConfiguration;

public class TestHadoopAccessorService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        new File(getTestCaseConfDir(), "hadoop-confx").mkdir();
        File actConfXDir = new File(getTestCaseConfDir(), "action-confx");
        actConfXDir.mkdir();
        new File(actConfXDir, "action").mkdir();
        // Create the default action dir
        new File(actConfXDir, "default").mkdir();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-hadoop-config.xml");
        OutputStream os = new FileOutputStream(new File(getTestCaseConfDir() + "/hadoop-confx", "core-site.xml"));
        IOUtils.copyStream(is, os);
        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config.xml");
        os = new FileOutputStream(new File(actConfXDir, "action.xml"));
        IOUtils.copyStream(is, os);

        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-default-config.xml");
        os = new FileOutputStream(new File(actConfXDir, "default.xml"));
        IOUtils.copyStream(is, os);

        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config-1.xml");
        os = new FileOutputStream(new File(actConfXDir + "/action", "a-conf.xml"));
        IOUtils.copyStream(is, os);

        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config-2.xml");
        os = new FileOutputStream(new File(actConfXDir + "/action", "b-conf.xml"));
        IOUtils.copyStream(is, os);

        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config-3.xml");
        os = new FileOutputStream(new File(actConfXDir + "/action", "c-conf-3.xml"));
        IOUtils.copyStream(is, os);

        is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-default-config-1.xml");
        os = new FileOutputStream(new File(actConfXDir + "/default", "z-conf.xml"));
        IOUtils.copyStream(is, os);

        setSystemProperty("oozie.service.HadoopAccessorService.hadoop.configurations",
                          "*=hadoop-conf,jt=hadoop-confx");
        setSystemProperty("oozie.service.HadoopAccessorService.action.configurations",
                          "*=hadoop-conf,jt=action-confx");
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
        assertNotNull(has.createJobConf("*"));
        assertNotNull(has.createJobConf("jt"));
        assertEquals("bar", has.createJobConf("jt").get("foo"));
        assertNotNull(has.createActionDefaultConf("*", "action"));
        assertNotNull(has.createActionDefaultConf("jt", "action"));
        assertNotNull(has.createActionDefaultConf("jt", "actionx"));
        assertNotNull(has.createActionDefaultConf("jtx", "action"));
        assertNull(has.createActionDefaultConf("*", "action").get("action.foo"));
    }

    public void testActionConfigurations() throws Exception {
        Services services = Services.get();
        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        assertNotNull(has);
        XConfiguration conf = has.createActionDefaultConf("jt", "action");
        assertNotNull(conf);

        // Check that the param only in default.xml is still present
        assertEquals("default.bar", conf.get("default.foo"));

        // And a property that is present in one of the conf files in default dir
        assertEquals("default.bus", conf.get("default.car"));

        // Check that a default param is overridden by one of the action config files
        assertEquals("action.bar", conf.get("action.foo"));

        // Check that params from <action-dir>/action/conf files is still present
        assertEquals("action.car", conf.get("action.boo"));
        assertEquals("action.carcar", conf.get("oozie.launcher.action.booboo"));

        /*
            Check precedence - Order of precedence - 0 is the lowest.   Parameters in files of
            lower precedence will be overridden by redefinitions in higher precedence files.

            0 - All files in defaultdir/*.xml (sorted by lexical name)
               Files with names lexically lower have lesser precedence than the following ones.
            1 - default.xml
            2 - All files in actiondir/*.xml (sort by lexical name)
               Files with names lexically lower have lesser precedence than the following ones
            3 - action.xml
         */
        assertEquals("100", conf.get("action.testprop"));
        assertEquals("1", conf.get("default.testprop"));

    }

    public void testAccessor() throws Exception {
        Services services = Services.get();
        HadoopAccessorService has = services.get(HadoopAccessorService.class);
        JobConf conf = has.createJobConf(getJobTrackerUri());
        conf.set("mapred.job.tracker", getJobTrackerUri());
        conf.set("fs.default.name", getNameNodeUri());

        URI uri = new URI(getNameNodeUri());

        //valid user
        String user = getTestUser();
        String group = getTestGroup();

        JobClient jc = has.createJobClient(user, conf);
        assertNotNull(jc);
        FileSystem fs = has.createFileSystem(user, new URI(getNameNodeUri()), conf);
        assertNotNull(fs);
        fs = has.createFileSystem(user, uri, conf);
        assertNotNull(fs);

        //invalid user

        user = "invalid";

        try {
            has.createJobClient(user, conf);
            fail();
        }
        catch (Throwable ex) {
        }

        try {
            has.createFileSystem(user, uri, conf);
            fail();
        }
        catch (Throwable ex) {
        }
    }

    public void testGetMRDelegationTokenRenewer() throws Exception {
        HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
        JobConf jobConf = new JobConf(false);
        assertEquals(new Text("oozie mr token"), has.getMRTokenRenewerInternal(jobConf));
        jobConf.set("mapred.job.tracker", "localhost:50300");
        jobConf.set("mapreduce.jobtracker.kerberos.principal", "mapred/_HOST@KDC.DOMAIN.COM");
        assertEquals(new Text("mapred/localhost@KDC.DOMAIN.COM"), has.getMRTokenRenewerInternal(jobConf));
        jobConf = new JobConf(false);
        jobConf.set("mapreduce.jobtracker.address", "127.0.0.1:50300");
        jobConf.set("mapreduce.jobtracker.kerberos.principal", "mapred/_HOST@KDC.DOMAIN.COM");
        assertEquals(new Text("mapred/localhost@KDC.DOMAIN.COM"), has.getMRTokenRenewerInternal(jobConf));
        jobConf = new JobConf(false);
        jobConf.set("yarn.resourcemanager.address", "localhost:8032");
        jobConf.set("yarn.resourcemanager.principal", "rm/server.com@KDC.DOMAIN.COM");
        assertEquals(new Text("rm/server.com@KDC.DOMAIN.COM"), has.getMRTokenRenewerInternal(jobConf));

        // Try the above with logical URIs (i.e. for HA)
        jobConf = new JobConf(false);
        jobConf.set("mapred.job.tracker", "jt-ha-uri");
        jobConf.set("mapreduce.jobtracker.kerberos.principal", "mapred/_HOST@KDC.DOMAIN.COM");
        assertEquals(new Text("mapred/localhost@KDC.DOMAIN.COM"), has.getMRTokenRenewerInternal(jobConf));
        jobConf = new JobConf(false);
        jobConf.set("mapreduce.jobtracker.address", "jt-ha-uri");
        jobConf.set("mapreduce.jobtracker.kerberos.principal", "mapred/_HOST@KDC.DOMAIN.COM");
        assertEquals(new Text("mapred/localhost@KDC.DOMAIN.COM"), has.getMRTokenRenewerInternal(jobConf));
        jobConf = new JobConf(false);
        jobConf.set("yarn.resourcemanager.address", "rm-ha-uri");
        jobConf.set("yarn.resourcemanager.principal", "rm/server.com@KDC.DOMAIN.COM");
        assertEquals(new Text("rm/server.com@KDC.DOMAIN.COM"), has.getMRTokenRenewerInternal(jobConf));
    }

    public void testCheckSupportedFilesystem() throws Exception {
        Configuration hConf = Services.get().getConf();

        // Only allow hdfs and foo schemes
        HadoopAccessorService has = new HadoopAccessorService();
        hConf.set("oozie.service.HadoopAccessorService.supported.filesystems", "hdfs,foo");
        has.init(hConf);
        has.checkSupportedFilesystem(new URI("hdfs://localhost:1234/blah"));
        has.checkSupportedFilesystem(new URI("foo://localhost:1234/blah"));
        try {
            has.checkSupportedFilesystem(new URI("file://localhost:1234/blah"));
            fail("Should have thrown an exception because 'file' scheme isn't allowed");
        }
        catch (HadoopAccessorException hae) {
            assertEquals(ErrorCode.E0904, hae.getErrorCode());
        }
        // giving no scheme should skip the check
        has.checkSupportedFilesystem(new URI("/blah"));

        // allow all schemes
        has = new HadoopAccessorService();
        hConf.set("oozie.service.HadoopAccessorService.supported.filesystems", "*");
        has.init(hConf);
        has.checkSupportedFilesystem(new URI("hdfs://localhost:1234/blah"));
        has.checkSupportedFilesystem(new URI("foo://localhost:1234/blah"));
        has.checkSupportedFilesystem(new URI("file://localhost:1234/blah"));
        // giving no scheme should skip the check
        has.checkSupportedFilesystem(new URI("/blah"));
    }

    public void testValidateJobTracker() throws Exception {
        HadoopAccessorService has = new HadoopAccessorService();
        Configuration conf = new Configuration(false);
        conf.set("oozie.service.HadoopAccessorService.jobTracker.whitelist", " ");
        has.init(conf);
        has.validateJobTracker("foo");
        has.validateJobTracker("bar");
        has.validateJobTracker("blah");
        conf.set("oozie.service.HadoopAccessorService.jobTracker.whitelist", "foo,bar");
        has.init(conf);
        has.validateJobTracker("foo");
        has.validateJobTracker("bar");
        try {
            has.validateJobTracker("blah");
            fail("Should have gotten an exception");
        } catch (HadoopAccessorException hae) {
            assertEquals(ErrorCode.E0900, hae.getErrorCode());
            // We have to check for either of these because Java 7 and 8 have a different order
            String s1 = "E0900: JobTracker [blah] not allowed, not in Oozie's whitelist. Allowed values are: [foo, bar]";
            String s2 = "E0900: JobTracker [blah] not allowed, not in Oozie's whitelist. Allowed values are: [bar, foo]";
            assertTrue("expected:<" + s1 + "> or <" + s2 + "> but was:<" + hae.getMessage() + ">",
                    s1.equals(hae.getMessage()) || s2.equals(hae.getMessage()));
        }
        has.destroy();
    }

    public void testValidateNameNode() throws Exception {
        HadoopAccessorService has = new HadoopAccessorService();
        Configuration conf = new Configuration(false);
        conf.set("oozie.service.HadoopAccessorService.nameNode.whitelist", " ");
        has.init(conf);
        has.validateNameNode("foo");
        has.validateNameNode("bar");
        has.validateNameNode("blah");
        conf.set("oozie.service.HadoopAccessorService.nameNode.whitelist", "foo,bar");
        has.init(conf);
        has.validateNameNode("foo");
        has.validateNameNode("bar");
        try {
            has.validateNameNode("blah");
            fail("Should have gotten an exception");
        } catch (HadoopAccessorException hae) {
            assertEquals(ErrorCode.E0901, hae.getErrorCode());
            // We have to check for either of these because Java 7 and 8 have a different order
            String s1 = "E0901: NameNode [blah] not allowed, not in Oozie's whitelist. Allowed values are: [foo, bar]";
            String s2 = "E0901: NameNode [blah] not allowed, not in Oozie's whitelist. Allowed values are: [bar, foo]";
            assertTrue("expected:<" + s1 + "> or <" + s2 + "> but was:<" + hae.getMessage() + ">",
                    s1.equals(hae.getMessage()) || s2.equals(hae.getMessage()));
        }
        has.destroy();
    }
}
