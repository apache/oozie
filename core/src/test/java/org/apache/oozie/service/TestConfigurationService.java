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

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.action.hadoop.CredentialsProviderFactory;
import org.apache.oozie.action.hadoop.DistcpActionExecutor;
import org.apache.oozie.action.hadoop.LauncherAMUtils;
import org.apache.oozie.command.coord.CoordActionInputCheckXCommand;
import org.apache.oozie.command.coord.CoordSubmitXCommand;
import org.apache.oozie.command.wf.JobXCommand;
import org.apache.oozie.compression.CodecFactory;
import org.apache.oozie.event.listener.ZKConnectionListener;
import org.apache.oozie.executor.jpa.CoordActionGetForInfoJPAExecutor;
import org.apache.oozie.servlet.AuthFilter;
import org.apache.oozie.servlet.V1JobServlet;
import org.apache.oozie.sla.service.SLAService;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.ConfigUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLogFilter;
import org.apache.oozie.util.XLogStreamer;
import org.apache.oozie.workflow.lite.LiteWorkflowAppParser;
import org.apache.xerces.jaxp.DocumentBuilderFactoryImpl;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileOutputStream;

public class TestConfigurationService extends XTestCase {

    Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
    }

    @Override
    protected void tearDown() throws Exception {
        services.destroy();
        super.tearDown();
    }

    private void prepareOozieConfDir(String oozieSite) throws Exception {
        prepareOozieConfDir(oozieSite, ConfigurationService.SITE_CONFIG_FILE);
    }

    private void prepareOozieConfDir(String oozieSite, String alternateSiteFile) throws Exception {
        if (!alternateSiteFile.equals(ConfigurationService.SITE_CONFIG_FILE)) {
            setSystemProperty(ConfigurationService.OOZIE_CONFIG_FILE, alternateSiteFile);
        }
        File siteFile = new File(getTestCaseConfDir(), alternateSiteFile);
        IOUtils.copyStream(IOUtils.getResourceAsStream(oozieSite, -1),
                           new FileOutputStream(siteFile));
    }

    public void testValueFromDefault() throws Exception {
        prepareOozieConfDir("oozie-site1.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("oozie-" + System.getProperty("user.name"), cl.getConf().get("oozie.system.id"));
        assertNull(cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testMissingSite() throws Exception {
        prepareOozieConfDir("oozie-site2.xml");
        setSystemProperty(ConfigurationService.OOZIE_CONFIG_FILE, "oozie-site-missing.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("oozie-" + System.getProperty("user.name"), cl.getConf().get("oozie.system.id"));
        assertNull(cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testValueFromSite() throws Exception {
        prepareOozieConfDir("oozie-site2.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("SITE1", cl.getConf().get("oozie.system.id"));
        assertEquals("SITE2", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testValueFromSiteAlternate() throws Exception {
        prepareOozieConfDir("oozie-sitealternate.xml", "oozie-alternate.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("ALTERNATE1", cl.getConf().get("oozie.system.id"));
        assertEquals("ALTERNATE2", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testSysPropOverride() throws Exception {
        prepareOozieConfDir("oozie-site2.xml");
        setSystemProperty("oozie.dummy", "OVERRIDE");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("OVERRIDE", cl.getConf().get("oozie.dummy"));
        cl.destroy();
    }

    public void testAlternateConfDir() throws Exception {
        String customConfDir = createTestCaseSubDir("xconf");
        setSystemProperty(ConfigurationService.OOZIE_CONFIG_DIR, customConfDir);

        IOUtils.copyStream(IOUtils.getResourceAsStream("oozie-site1.xml", -1),
                           new FileOutputStream(new File(customConfDir, "oozie-site.xml")));

        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("oozie-" + System.getProperty("user.name"), cl.getConf().get("oozie.system.id"));
        assertNull(cl.getConf().get("oozie.dummy"));
        cl.destroy();

    }

    public void testMaskProperties() throws Exception {
        prepareOozieConfDir("oozie-site-mask.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        Configuration conf = cl.getConf();
        assertEquals("my-secret", conf.get("oozie.authentication.signature.secret"));
        assertEquals("my-password", conf.get("oozie.service.JPAService.jdbc.password"));
        assertEquals("true", conf.get("oozie.is.awesome"));
        conf = cl.getMaskedConfiguration();
        assertEquals("**MASKED**", conf.get("oozie.authentication.signature.secret"));
        assertEquals("**MASKED**", conf.get("oozie.service.JPAService.jdbc.password"));
        assertEquals("true", conf.get("oozie.is.awesome"));
        cl.destroy();
    }

    public void testOozieConfig() throws Exception{
        prepareOozieConfDir("oozie-site2.xml");
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals("SITE1", cl.getConf().get("oozie.system.id"));
        assertEquals("SITE2", cl.getConf().get("oozie.dummy"));
        assertEquals("SITE1", ConfigurationService.get(cl.getConf(), "oozie.system.id"));
        assertEquals("SITE2", ConfigurationService.get(cl.getConf(), "oozie.dummy"));

        assertNull(cl.getConf().get("oozie.test.nonexist"));
        assertEquals(ConfigUtils.STRING_DEFAULT, ConfigurationService.get(cl.getConf(), "oozie.test.nonexist"));
        assertEquals(ConfigUtils.BOOLEAN_DEFAULT, ConfigurationService.getBoolean(cl.getConf(), "oozie.test.nonexist"));

        Configuration testConf = new Configuration(false);
        assertEquals(ConfigUtils.STRING_DEFAULT, ConfigurationService.get("test.nonexist"));
        assertEquals(ConfigUtils.STRING_DEFAULT, ConfigurationService.get(testConf, "test.nonexist"));
        testConf.set("test.nonexist", "another-conf");
        assertEquals(ConfigUtils.STRING_DEFAULT, ConfigurationService.get("test.nonexist"));
        assertEquals("another-conf", ConfigurationService.get(testConf, "test.nonexist"));
        Services.get().getConf().set("test.nonexist", "oozie-conf");
        assertEquals("oozie-conf", ConfigurationService.get("test.nonexist"));
        assertEquals("another-conf", ConfigurationService.get(testConf, "test.nonexist"));
        testConf.clear();
        assertEquals("oozie-conf", ConfigurationService.get("test.nonexist"));
        assertEquals(ConfigUtils.STRING_DEFAULT, ConfigurationService.get(testConf, "test.nonexist"));

        assertEquals("http://0.0.0.0:11000/oozie/callback", ConfigurationService.get(CallbackService.CONF_BASE_URL));
        assertEquals(5, ConfigurationService.getInt(CallbackService.CONF_EARLY_REQUEUE_MAX_RETRIES));
        assertEquals("gz", ConfigurationService.get(CodecFactory.COMPRESSION_OUTPUT_CODEC));
        assertEquals(4096, ConfigurationService.getInt(XLogStreamer.STREAM_BUFFER_LEN));
        assertEquals(10000,  ConfigurationService.getLong(JvmPauseMonitorService.WARN_THRESHOLD_KEY));
        assertEquals(60, ConfigurationService.getInt(InstrumentationService.CONF_LOGGING_INTERVAL));
        assertEquals(30, ConfigurationService.getInt(PurgeService.CONF_OLDER_THAN));
        assertEquals(7, ConfigurationService.getInt(PurgeService.COORD_CONF_OLDER_THAN));
        assertEquals(7, ConfigurationService.getInt(PurgeService.BUNDLE_CONF_OLDER_THAN));
        assertEquals(100, ConfigurationService.getInt(PurgeService.PURGE_LIMIT));
        assertEquals(3600, ConfigurationService.getInt(PurgeService.CONF_PURGE_INTERVAL));
        assertEquals(300, ConfigurationService.getInt(CoordMaterializeTriggerService.CONF_LOOKUP_INTERVAL));
        assertEquals(0, ConfigurationService.getInt(CoordMaterializeTriggerService.CONF_SCHEDULING_INTERVAL));
        assertEquals(300, cl.getConf().getInt(
                CoordMaterializeTriggerService.CONF_SCHEDULING_INTERVAL,
                ConfigurationService.getInt(CoordMaterializeTriggerService.CONF_LOOKUP_INTERVAL)));
        assertEquals(3600, ConfigurationService.getInt(CoordMaterializeTriggerService.CONF_MATERIALIZATION_WINDOW));
        assertEquals(10, ConfigurationService.getInt(CoordMaterializeTriggerService.CONF_CALLABLE_BATCH_SIZE));
        assertEquals(50, ConfigurationService.getInt(CoordMaterializeTriggerService
                .CONF_MATERIALIZATION_SYSTEM_LIMIT));
        assertEquals(0.05f, ConfigurationService.getFloat(CoordSubmitXCommand.CONF_MAT_THROTTLING_FACTOR));

        assertEquals("oozie", ConfigurationService.get(JPAService.CONF_DB_SCHEMA));
        assertEquals("jdbc:hsqldb:mem:oozie-db;create=true", ConfigurationService.get(JPAService.CONF_URL));
        assertEquals("org.hsqldb.jdbcDriver", ConfigurationService.get(JPAService.CONF_DRIVER));
        assertEquals("sa", ConfigurationService.get(JPAService.CONF_USERNAME));
        assertEquals("", ConfigurationService.get(JPAService.CONF_PASSWORD).trim());
        assertEquals("10", ConfigurationService.get(JPAService.CONF_MAX_ACTIVE_CONN).trim());
        assertEquals("org.apache.oozie.util.db.BasicDataSourceWrapper",
                ConfigurationService.get(JPAService.CONF_CONN_DATA_SOURCE));
        assertEquals("", ConfigurationService.get(JPAService.CONF_CONN_PROPERTIES).trim());
        assertEquals("300000", ConfigurationService.get(JPAService.CONF_VALIDATE_DB_CONN_EVICTION_INTERVAL).trim());
        assertEquals("10", ConfigurationService.get(JPAService.CONF_VALIDATE_DB_CONN_EVICTION_NUM).trim());

        assertEquals(2048, ConfigurationService.getInt(LauncherAMUtils.CONF_OOZIE_ACTION_MAX_OUTPUT_DATA));
        assertEquals("http://0.0.0.0:11000/oozie?job=", ConfigurationService.get(JobXCommand.CONF_CONSOLE_URL));
        assertEquals(false, ConfigurationService.getBoolean(HadoopAccessorService.KERBEROS_AUTH_ENABLED));

        assertEquals(0, ConfigurationService.getStrings("no.defined").length);
        assertEquals(0, ConfigurationService.getStrings(CredentialsProviderFactory.CRED_KEY).length);
        assertEquals(1, ConfigurationService.getStrings(DistcpActionExecutor.CLASS_NAMES).length);
        assertEquals("distcp=org.apache.hadoop.tools.DistCp",
                ConfigurationService.getStrings(DistcpActionExecutor.CLASS_NAMES)[0]);
        assertEquals(1, ConfigurationService.getInt(CoordActionInputCheckXCommand.COORD_EXECUTION_NONE_TOLERANCE));
        assertEquals(1000, ConfigurationService.getInt(V1JobServlet.COORD_ACTIONS_DEFAULT_LENGTH));

        assertEquals(cl.getConf().get(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE), ConfigurationService.get
                (LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE));
        assertEquals(cl.getConf().get(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT),
                ConfigurationService.get(LiteWorkflowStoreService.CONF_USER_RETRY_ERROR_CODE_EXT));

        assertEquals("simple", cl.getConf().get(AuthFilter.OOZIE_PREFIX + AuthFilter.AUTH_TYPE));
        assertEquals("36000", cl.getConf().get(AuthFilter.OOZIE_PREFIX + AuthFilter.AUTH_TOKEN_VALIDITY));
        // The cookie.domain config is in oozie-default.xml mostly for documentation purposes, but it needs to have an empty string
        // value by default, which Configuration parses as null
        assertNull(cl.getConf().get(AuthFilter.OOZIE_PREFIX + AuthFilter.COOKIE_DOMAIN));
        assertEquals("true", cl.getConf().get(AuthFilter.OOZIE_PREFIX + "simple.anonymous.allowed"));
        assertEquals("HTTP/localhost@LOCALHOST", cl.getConf().get(AuthFilter.OOZIE_PREFIX + "kerberos.principal"));
        assertEquals(cl.getConf().get(HadoopAccessorService.KERBEROS_KEYTAB),
                cl.getConf().get(AuthFilter.OOZIE_PREFIX + "kerberos.keytab"));
        assertEquals("DEFAULT", cl.getConf().get(AuthFilter.OOZIE_PREFIX + "kerberos.name.rules"));

        assertEquals(true, ConfigurationService.getBoolean(LiteWorkflowAppParser.VALIDATE_FORK_JOIN));
        assertEquals(false,
                ConfigurationService.getBoolean(CoordActionGetForInfoJPAExecutor.COORD_GET_ALL_COLS_FOR_ACTION));
        assertEquals(1, ConfigurationService.getStrings(URIHandlerService.URI_HANDLERS).length);
        assertEquals("org.apache.oozie.dependency.FSURIHandler",
                ConfigurationService.getStrings(URIHandlerService.URI_HANDLERS)[0]);
        assertEquals(cl.getConf().getBoolean("oozie.hadoop-2.0.2-alpha.workaround.for.distributed.cache", false),
                ConfigurationService.getBoolean(LauncherAMUtils.HADOOP2_WORKAROUND_DISTRIBUTED_CACHE));

        assertEquals("org.apache.oozie.event.MemoryEventQueue",
                (ConfigurationService.getClass(cl.getConf(), EventHandlerService.CONF_EVENT_QUEUE).getName()));
        assertEquals(-1, ConfigurationService.getInt(XLogFilter.MAX_SCAN_DURATION));
        assertEquals(-1, ConfigurationService.getInt(XLogFilter.MAX_ACTIONLIST_SCAN_DURATION));
        assertEquals(10000, ConfigurationService.getLong(JvmPauseMonitorService.WARN_THRESHOLD_KEY));
        assertEquals(1000, ConfigurationService.getLong(JvmPauseMonitorService.INFO_THRESHOLD_KEY));

        assertEquals(10000, ConfigurationService.getInt(CallableQueueService.CONF_QUEUE_SIZE));
        assertEquals(10, ConfigurationService.getInt(CallableQueueService.CONF_THREADS));
        assertEquals(3, ConfigurationService.getInt(CallableQueueService.CONF_CALLABLE_CONCURRENCY));
        assertEquals(120, ConfigurationService.getInt(CoordSubmitXCommand.CONF_DEFAULT_TIMEOUT_NORMAL));

        assertEquals(300, ConfigurationService.getInt(ZKLocksService.REAPING_THRESHOLD));
        assertEquals(2, ConfigurationService.getInt(ZKLocksService.REAPING_THREADS));
        assertEquals(10000, ConfigurationService.getInt(JobXCommand.DEFAULT_REQUEUE_DELAY));

        assertEquals(0, ConfigurationService.getStrings(AbandonedCoordCheckerService.TO_ADDRESS).length);
        assertEquals(25, ConfigurationService.getInt(AbandonedCoordCheckerService.CONF_FAILURE_LEN));
        assertEquals(false, ConfigurationService.getBoolean(AbandonedCoordCheckerService.CONF_JOB_KILL));
        assertEquals(60, ConfigurationService.getInt(AbandonedCoordCheckerService.CONF_CHECK_DELAY));
        assertEquals(1440, ConfigurationService.getInt(AbandonedCoordCheckerService.CONF_CHECK_INTERVAL));
        assertEquals(2880, ConfigurationService.getInt(AbandonedCoordCheckerService.CONF_JOB_OLDER_THAN));

        assertEquals(true, ConfigurationService.getBoolean(ZKConnectionListener.CONF_SHUTDOWN_ON_TIMEOUT));

        assertEquals(7, ConfigurationService.getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION));
        assertEquals(5000, ConfigurationService.getInt(SLAService.CONF_CAPACITY));
        assertEquals(11000, ConfigurationService.getInt("oozie.http.port"));
        assertEquals(11443, ConfigurationService.getInt("oozie.https.port"));
        assertFalse(ConfigurationService.getBoolean("oozie.https.enabled"));
        assertEquals(65536, ConfigurationService.getInt("oozie.http.response.header.size"));
        assertEquals(65536, ConfigurationService.getInt("oozie.http.request.header.size"));
        assertEquals("TLSv1,SSLv2Hello,TLSv1.1,TLSv1.2", ConfigurationService.get("oozie.https.include.protocols"));
        assertEquals("", ConfigurationService.get("oozie.https.exclude.protocols"));
        assertEquals("", ConfigurationService.get("oozie.https.include.cipher.suites"));
        assertEquals("TLS_ECDHE_RSA_WITH_RC4_128_SHA,SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA,SSL_RSA_WITH_DES_CBC_SHA," +
                "SSL_DHE_RSA_WITH_DES_CBC_SHA,SSL_RSA_EXPORT_WITH_RC4_40_MD5,SSL_RSA_EXPORT_WITH_DES40_CBC_SHA," +
                "SSL_RSA_WITH_RC4_128_MD5", ConfigurationService.get("oozie.https.exclude.cipher.suites"));
        assertEquals(150, ConfigurationService.getInt("oozie.server.threadpool.max.threads"));

        cl.destroy();
    }

    public void testDocumentBuilderFactorySystemPropertyDefault() throws Exception {
        verifyDocumentBuilderFactoryClass(DocumentBuilderFactoryImpl.class.getName(), DocumentBuilderFactoryImpl.class);
    }

    public void testDocumentBuilderFactorySystemPropertyCustom() throws Exception {
        prepareOozieConfDir("oozie-site-documentbuilderfactory.xml");
        verifyDocumentBuilderFactoryClass(DummyDocumentBuilderFactoryImpl.class.getName(), DummyDocumentBuilderFactoryImpl.class);
    }

    public void testDocumentBuilderFactorySystemPropertyEmpty() throws Exception {
        // Determine the class that the JVM would provide if we don't set the property
        setSystemProperty("javax.xml.parsers.DocumentBuilderFactory", null);
        assertNull(System.getProperty("javax.xml.parsers.DocumentBuilderFactory"));
        Class<?> dbfClass = DocumentBuilderFactory.newInstance().getClass();

        prepareOozieConfDir("oozie-site-documentbuilderfactory-empty.xml");
        verifyDocumentBuilderFactoryClass(null, dbfClass);
    }

    private void verifyDocumentBuilderFactoryClass(String expectedPropertyValue, Class<?> expectedClass) throws Exception {
        setSystemProperty("javax.xml.parsers.DocumentBuilderFactory", null);
        assertNull(System.getProperty("javax.xml.parsers.DocumentBuilderFactory"));
        ConfigurationService cl = new ConfigurationService();
        cl.init(null);
        assertEquals(expectedPropertyValue, System.getProperty("javax.xml.parsers.DocumentBuilderFactory"));
        DocumentBuilderFactory docFac = DocumentBuilderFactory.newInstance();
        assertTrue("Expected DocumentBuilderFactory to be of class [" + expectedClass.getName() + "] but was ["
                + docFac.getClass().getName() + "]", expectedClass.isInstance(docFac));
        cl.destroy();
    }

}

