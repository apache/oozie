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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.oozie.service.HCatAccessorService;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HCatCredentialHelper.class, HCatCredentials.class })
public class TestHCatCredentials {
    private Services services;
    private static File OOZIE_HOME_DIR = null;
    private static final String TEST_HIVE_METASTORE_PRINCIPAL = "hcat/test-hcat1.com@OOZIE.EXAMPLE.COM";
    private static final String TEST_HIVE_METASTORE_URI = "thrift://test-hcat1.com:9898";
    private static final String TEST_HIVE_METASTORE_PRINCIPAL2 = "hcat/test-hcat2.com@OOZIE.EXAMPLE.COM";
    private static final String TEST_HIVE_METASTORE_URI2 = "thrift://test-hcat2.com:9898";
    final String HIVE_METASTORE_PRINCIPAL = "hive.principal";
    final String HIVE_METASTORE_URI = "hive.uri";
    final String HCAT_METASTORE_PRINCIPAL = "hcat.principal";
    final String HCAT_METASTORE_URI = "hcat.uri";
    private static File hiveSiteXml = null;
    private static ClassLoader prevClassloader = null;

    @BeforeClass
    public static void initialize() throws Exception {
        OOZIE_HOME_DIR = new File(new File("").getAbsolutePath(), "test-oozie-home");
        if (!OOZIE_HOME_DIR.exists()) {
            OOZIE_HOME_DIR.mkdirs();
        }
        System.setProperty(Services.OOZIE_HOME_DIR, OOZIE_HOME_DIR.getAbsolutePath());
        Services.setOozieHome();
        File oozieConfDir = new File(OOZIE_HOME_DIR.getAbsolutePath(), "conf");
        oozieConfDir.mkdir();
        File hadoopConfDir = new File(oozieConfDir, "hadoop-conf");
        hadoopConfDir.mkdir();
        File actionConfDir = new File(oozieConfDir, "action-conf");
        actionConfDir.mkdir();
        hiveSiteXml = new File(OOZIE_HOME_DIR, "hive-site.xml");
        FileWriter fw = new FileWriter(hiveSiteXml);
        fw.write(getHiveConfig(TEST_HIVE_METASTORE_PRINCIPAL, TEST_HIVE_METASTORE_URI));
        fw.flush();
        fw.close();
        prevClassloader = Thread.currentThread().getContextClassLoader();
    }

    @Before
    public void setUp() throws ServiceException, MalformedURLException {
        services = new Services();
        @SuppressWarnings("deprecation")
        Configuration conf = services.getConf();
        conf.set(Services.CONF_SERVICE_EXT_CLASSES, HCatAccessorService.class.getName());
        conf.set(Services.CONF_SERVICE_CLASSES, "");
        ContextClassLoader contextClassLoader = new ContextClassLoader(HCatCredentials.class.getClassLoader());
        contextClassLoader.addURL(hiveSiteXml.toURI().toURL());
        Thread.currentThread().setContextClassLoader(contextClassLoader);
    }

    @After
    public void tearDown(){
        if (services != null) {
            services.destroy();
        }
    }

    @AfterClass
    public static void terminate() throws IOException {
        FileUtils.deleteDirectory(OOZIE_HOME_DIR);
        Thread.currentThread().setContextClassLoader(prevClassloader);
    }

    @Test
    public void testAddToJobConfFromHCat() throws Exception {
        File hcatConfig = new File(OOZIE_HOME_DIR, "hcatConf.xml");
        FileWriter fw = new FileWriter(hcatConfig);
        fw.write(getHiveConfig(TEST_HIVE_METASTORE_PRINCIPAL2, TEST_HIVE_METASTORE_URI2));
        fw.flush();
        fw.close();
        @SuppressWarnings("deprecation")
        Configuration conf = services.getConf();
        conf.set(HCatAccessorService.HCAT_CONFIGURATION, OOZIE_HOME_DIR + "/hcatConf.xml");
        services.init();
        HCatCredentialHelper hcatCredHelper = Mockito.mock(HCatCredentialHelper.class);
        PowerMockito.whenNew(HCatCredentialHelper.class).withNoArguments().thenReturn(hcatCredHelper);
        CredentialsProperties credProps = new CredentialsProperties("", "");
        credProps.setProperties(new HashMap<String, String>());
        HCatCredentials hcatCred = new HCatCredentials();
        final JobConf jobConf = new JobConf(false);
        Credentials credentials = new Credentials();
        PowerMockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Configuration jConf = (Configuration) args[1];
                jConf.set(HCAT_METASTORE_PRINCIPAL, (String) args[2]);
                jConf.set(HCAT_METASTORE_URI, (String) args[3]);
                return null;
            }
        }).when(hcatCredHelper).set(credentials, jobConf, TEST_HIVE_METASTORE_PRINCIPAL2, TEST_HIVE_METASTORE_URI2);
        hcatCred.updateCredentials(credentials, jobConf, credProps, null);
        assertEquals(TEST_HIVE_METASTORE_PRINCIPAL2, jobConf.get(HCAT_METASTORE_PRINCIPAL));
        assertEquals(TEST_HIVE_METASTORE_URI2, jobConf.get(HCAT_METASTORE_URI));
        assertNull(jobConf.get(HIVE_METASTORE_PRINCIPAL));
        assertNull(jobConf.get(HIVE_METASTORE_URI));
        hcatConfig.delete();
    }

    @Test
    public void testAddToJobConfFromHiveConf() throws Exception {
        services.init();
        CredentialsProperties credProps = new CredentialsProperties("", "");
        credProps.setProperties(new HashMap<String, String>());
        HCatCredentials hcatCred = new HCatCredentials();
        final JobConf jobConf = new JobConf(false);
        Credentials credentials = new Credentials();
        HCatCredentialHelper hcatCredHelper = Mockito.mock(HCatCredentialHelper.class);
        PowerMockito.whenNew(HCatCredentialHelper.class).withNoArguments().thenReturn(hcatCredHelper);
        PowerMockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                Configuration jConf = (Configuration) args[1];
                jConf.set(HIVE_METASTORE_PRINCIPAL, (String) args[2]);
                jConf.set(HIVE_METASTORE_URI, (String) args[3]);
                return null;
            }
        }).when(hcatCredHelper).set(credentials, jobConf, TEST_HIVE_METASTORE_PRINCIPAL, TEST_HIVE_METASTORE_URI);
        hcatCred.updateCredentials(credentials, jobConf, credProps, null);
        assertEquals(TEST_HIVE_METASTORE_PRINCIPAL, jobConf.get(HIVE_METASTORE_PRINCIPAL));
        assertEquals(TEST_HIVE_METASTORE_URI, jobConf.get(HIVE_METASTORE_URI));
        assertNull(jobConf.get(HCAT_METASTORE_PRINCIPAL));
        assertNull(jobConf.get(HCAT_METASTORE_URI));
    }

    @Test
    public void testAddToJobConfFromOozieConfig() throws Exception {
        services.init();
        HCatCredentialHelper hcatCredHelper = Mockito.mock(HCatCredentialHelper.class);
        PowerMockito.whenNew(HCatCredentialHelper.class).withNoArguments().thenReturn(hcatCredHelper);
        CredentialsProperties credProps = new CredentialsProperties("", "");
        HashMap<String, String> prop = new HashMap<String, String>();
        prop.put("hcat.metastore.principal", TEST_HIVE_METASTORE_PRINCIPAL2);
        prop.put("hcat.metastore.uri", TEST_HIVE_METASTORE_URI2);
        credProps.setProperties(prop);
        HCatCredentials hcatCred = new HCatCredentials();
        final JobConf jobConf = new JobConf(false);
        Credentials credentials = new Credentials();
        PowerMockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                JobConf jConf = (JobConf) args[1];
                jConf.set(HCAT_METASTORE_PRINCIPAL, (String) args[2]);
                jConf.set(HCAT_METASTORE_URI, (String) args[3]);
                return null;
            }
        }).when(hcatCredHelper).set(credentials, jobConf, TEST_HIVE_METASTORE_PRINCIPAL2, TEST_HIVE_METASTORE_URI2);
        hcatCred.updateCredentials(credentials, jobConf, credProps, null);
        assertEquals(TEST_HIVE_METASTORE_PRINCIPAL2, jobConf.get(HCAT_METASTORE_PRINCIPAL));
        assertEquals(TEST_HIVE_METASTORE_URI2, jobConf.get(HCAT_METASTORE_URI));
        assertNull(jobConf.get(HIVE_METASTORE_PRINCIPAL));
        assertNull(jobConf.get(HIVE_METASTORE_URI));
    }

    private static String getHiveConfig(String hivePrincipal, String hiveUri) {
        return "<configuration>"
                + "<property>"
                    + "<name>hive.metastore.kerberos.principal</name>"
                    + "<value>"+ hivePrincipal + "</value>"
                + "</property>"
                + "<property>"
                    + "<name>hive.metastore.uris</name>"
                    + "<value>" + hiveUri + "</value>"
                + "</property>"
                + "</configuration>";
    }

    private static class ContextClassLoader extends URLClassLoader {
        // Map the resource name to its url
        private HashMap<String, URL> resources = new HashMap<String, URL>();

        @Override
        public URL findResource(String name) {
            if (resources.containsKey(name)) {
                return resources.get(name);
            }
            return super.findResource(name);
        }

        @Override
        public URL getResource(String name) {
            if (resources.containsKey(name)) {
                return resources.get(name);
            }
            return super.getResource(name);
        }

        public ContextClassLoader(ClassLoader classLoader) {
            this(new URL[0], classLoader);
        }

        public ContextClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

        @Override
        public void addURL(URL url) {
            super.addURL(url);
            try {
                resources.put(new Path(url.toURI()).getName(), url);
            }
            catch (URISyntaxException e) {
                e.printStackTrace(System.out);
            }
        }
    };
}