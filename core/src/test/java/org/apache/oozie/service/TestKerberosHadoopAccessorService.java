/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.service;

import org.apache.oozie.test.XTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * This testcase does a NOP when not testing with Kerberos ON
 */
public class TestKerberosHadoopAccessorService extends XTestCase {

    protected void setUp() throws Exception {
        super.setUp();
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            setSystemProperty(Services.CONF_SERVICE_EXT_CLASSES,
                              "org.apache.oozie.service.KerberosHadoopAccessorService");
            setSystemProperty("oozie.service.HadoopAccessorService.kerberos.enabled", "true");
            setSystemProperty("oozie.service.HadoopAccessorService.keytab.file", getKeytabFile());
            setSystemProperty("oozie.service.HadoopAccessorService.kerberos.principal", getOoziePrincipal());
            Services services = new Services();
            services.init();
        }
    }

    public void testAccessor() throws Exception {
        if (System.getProperty("oozie.test.hadoop.security", "simple").equals("kerberos")) {
            Services services = Services.get();
            HadoopAccessorService has = services.get(HadoopAccessorService.class);
            assertEquals("org.apache.oozie.service.KerberosHadoopAccessorService", has.getClass().getName());
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

}
