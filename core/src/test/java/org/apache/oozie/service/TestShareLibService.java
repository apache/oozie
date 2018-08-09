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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.HiveActionExecutor;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.PigActionExecutor;
import org.apache.oozie.action.hadoop.TestJavaActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.FSUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.Assert;
import org.junit.Test;

import org.mockito.Mockito;

public class TestShareLibService extends XFsTestCase {
    private static final String HDFS_SCHEME_PREFIX = "hdfs";
    private static final String TEST_HDFS_HOME = "/user/test/";
    private static final String TEST_MAPPING_FILENAME = "config.properties";
    private static final String TEST_HDFS_MAPPING_FILE_PATH = TEST_HDFS_HOME + TEST_MAPPING_FILENAME;
    private static final String SHARELIB_PATH = "shareLibPath/";

    Services services;
    private static String testCaseDirPath;
    SimpleDateFormat dt = new SimpleDateFormat("yyyyMMddHHmmss");

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        testCaseDirPath = getTestCaseDir();
        services = new Services();
        setSystemProps();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        services.destroy();
    }

    private void setSystemProps() throws IOException {
        IOUtils.createJar(new File(getTestCaseDir()), MyOozie.class.getName() + ".jar", MyOozie.class);
        IOUtils.createJar(new File(getTestCaseDir()), MyPig.class.getName() + ".jar", MyPig.class);
        IOUtils.createJar(new File(getTestCaseDir()), TestHive.class.getName() + ".jar", TestHive.class);

        Configuration conf = getOozieConfig();
        conf.set(WorkflowAppService.SYSTEM_LIB_PATH, getFsTestCaseDir() + "/share/lib");
        conf.set(Services.CONF_SERVICE_CLASSES, conf.get(Services.CONF_SERVICE_CLASSES) + ","
                + DummyShareLibService.class.getName());
        conf.setStrings(ActionService.CONF_ACTION_EXECUTOR_CLASSES, DummyPigActionExecutor.class.getName(),
                DummyHiveActionExecutor.class.getName());
    }

    public static class DummyShareLibService extends ShareLibService {
        @Override
        public String findContainingJar(Class<?> clazz) {
            if (JavaActionExecutor.getCommonLauncherClasses().contains(clazz)) {
                return testCaseDirPath + Path.SEPARATOR + MyOozie.class.getName() + ".jar";
            }
            return testCaseDirPath + Path.SEPARATOR + clazz.getName() + ".jar";
        }
    }

    public static class DummyPigActionExecutor extends PigActionExecutor {
        public DummyPigActionExecutor() {
        }

        @Override
        public List<Class<?>> getLauncherClasses() {
            return Lists.<Class<?>>newArrayList(MyPig.class);
        }
    }

    public static class DummyHiveActionExecutor extends HiveActionExecutor {
        public DummyHiveActionExecutor() {
        }

        @Override
        public List<Class<?>> getLauncherClasses() {
            return Lists.<Class<?>>newArrayList(TestHive.class);
        }
    }

    static class MyOozie {
    }

    static class MyPig {
    }

    static class TestHive {
    }

    @Test
    public void testfailFast() throws Exception {
        Configuration conf = getOozieConfig();
        conf.set(ShareLibService.FAIL_FAST_ON_STARTUP, "true");
        // Set dummyfile as metafile which doesn't exist.
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, String.valueOf(new Date().getTime()));
        try {
            services.init();
            fail("Should throw exception");
        }
        catch (Throwable e) {
            assertTrue(e.getMessage().contains("E0104: Could not fully initialize service"));
        }
    }

    @Test
    public void testCreateLauncherLibPath() throws Exception {
        setShipLauncherInOozieConfig();
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        List<Path> launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        assertNotNull(launcherPath);
        assertTrue(getFileSystem().exists(launcherPath.get(0)));
        List<Path> pigLauncherPath = shareLibService.getSystemLibJars("pig");
        assertTrue(getFileSystem().exists(pigLauncherPath.get(0)));
    }

    @Test
    public void testAddShareLibDistributedCache() throws Exception {
        setShipLauncherInOozieConfig();

        services.init();
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "</java>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        WorkflowJobBean wfj = new WorkflowJobBean();
        wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
        wfj.setConf(XmlUtils.prettyPrint(new XConfiguration()).toString());
        Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());
        PigActionExecutor ae = new PigActionExecutor();
        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
        verifyFilesInDistributedCache(DistributedCache.getCacheFiles(jobConf), MyPig.class.getName() + ".jar",
                MyOozie.class.getName() + ".jar");
    }

    @Test
    public void testAddShareLib_pig() throws Exception {
        setShipLauncherInOozieConfig();
        services.init();
        String actionXml = "<pig>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>" + "</pig>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        WorkflowJobBean wfj = new WorkflowJobBean();
        wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
        wfj.setConf(XmlUtils.prettyPrint(new XConfiguration()).toString());
        Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());
        PigActionExecutor ae = new PigActionExecutor();
        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
        verifyFilesInDistributedCache(DistributedCache.getCacheFiles(jobConf), "MyPig.jar", "MyOozie.jar");
    }

    @Test
    public void testAddShareLib_pig_withVersion() throws Exception {
        setShipLauncherInOozieConfig();

        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dt.get().format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);

        createFiles(libpath.toString() + Path.SEPARATOR + "pig_10" + Path.SEPARATOR + "pig-10.jar");

        services.init();
        String actionXml = "<pig>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<property><name>oozie.action.sharelib.for.pig</name><value>pig_10</value></property>" + "</pig>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        WorkflowJobBean wfj = new WorkflowJobBean();
        protoConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
        wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
        wfj.setConf(XmlUtils.prettyPrint(protoConf).toString());

        Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());
        PigActionExecutor ae = new PigActionExecutor();
        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        jobConf.set("oozie.action.sharelib.for.pig", "pig_10");
        ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);

        verifyFilesInDistributedCache(DistributedCache.getCacheFiles(jobConf), "MyPig.jar", "MyOozie.jar",
                "pig-10.jar");
    }

    // retentionTime overflows to negative before OOZIE-3142
    @Test
    public void testRetentionOverflow() throws Exception {
        getOozieConfig().set(ShareLibService.LAUNCHERJAR_LIB_RETENTION, "25");
        services.init();
        ShareLibService shareLibService = services.get(ShareLibService.class);
        assertTrue(shareLibService.retentionTime > 0);
    }

    @Test
    public void testPurgeShareLib() throws Exception {
        setShipLauncherInOozieConfig();
        FileSystem fs = getFileSystem();
        long expiryTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.convert(
                        getOozieConfig()
                                .getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7), TimeUnit.DAYS);

        // for directory created 8 days back to be deleted
        String expireTs = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 6 days back NOT to be deleted
        String noexpireTs = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 5 days back NOT to be deleted
        String noexpireTs1 = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));

        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path expirePath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + expireTs);
        Path noexpirePath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + noexpireTs);
        Path noexpirePath1 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + noexpireTs1);

        createDirs(fs, expirePath, noexpirePath, noexpirePath1);

        services.init();
        assertEquals(4, fs.listStatus(basePath).length);
        assertTrue(fs.exists(noexpirePath));
        assertTrue(fs.exists(noexpirePath1));
        assertTrue(fs.exists(expirePath));
    }

    @Test
    public void testPurgeLauncherJar() throws Exception {
        setShipLauncherInOozieConfig();
        FileSystem fs = getFileSystem();

        long expiryTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.convert(
                        getOozieConfig()
                                .getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7), TimeUnit.DAYS);

        // for directory created 8 days back to be deleted
        String expireTs = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 6 days back NOT to be deleted
        String noexpireTs = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 5 days back NOT to be deleted
        String noexpireTs1 = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));

        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path expirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + expireTs);
        Path noexpirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs);

        Path noexpirePath1 = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs1);
        createDirs(fs, expirePath, noexpirePath, noexpirePath1);

        services.init();
        assertEquals(4, fs.listStatus(basePath).length);
        assertTrue(fs.exists(noexpirePath));
        assertTrue(fs.exists(noexpirePath1));
        assertTrue(fs.exists(expirePath));
    }

    // Logic is to keep all share-lib between current timestamp and 7days old + 1 latest sharelib older than 7 days.
    // refer OOZIE-1761
    @Test
    public void testPurgeJar() throws Exception {
        setShipLauncherInOozieConfig();
        final FileSystem fs = getFileSystem();
        // for directory created 8 days back to be deleted
        long expiryTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.convert(
                        getOozieConfig()
                                .getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7), TimeUnit.DAYS);

        String expireTs = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        String expireTs1 = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));
        String noexpireTs = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        String noexpireTs1 = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));
        final Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));

        Path expirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + expireTs);
        Path expirePath1 = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + expireTs1);
        Path noexpirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs);
        Path noexpirePath1 = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs1);

        createDirs(fs, expirePath, expirePath1, noexpirePath, noexpirePath1);

        services.init();
        // Wait for the scheduled purge runnable to complete
        waitFor(20 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return (fs.listStatus(basePath).length == 4);
            }
        });
        assertEquals(4, fs.listStatus(basePath).length);
        assertTrue(fs.exists(noexpirePath));
        assertTrue(fs.exists(noexpirePath1));
        assertTrue(fs.exists(expirePath));
        assertFalse(fs.exists(expirePath1));
    }

    @Test
    public void testGetShareLibCompatible() throws Exception {
        FileSystem fs = getFileSystem();
        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));

        // Use basepath if there is no timestamped directory
        fs.mkdirs(basePath);
        Path pigPath = new Path(basePath.toString() + Path.SEPARATOR + "pig");
        fs.mkdirs(pigPath);

        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertNotNull(shareLibService.getShareLibJars("pig"));
    }

    @Test
    public void testGetShareLibPath() throws Exception {
        FileSystem fs = getFileSystem();
        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));

        // Use timedstamped directory if available
        Date time = new Date(System.currentTimeMillis());
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dt.get().format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);

        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertNotNull(shareLibService.getShareLibJars("pig"));
        assertNotNull(shareLibService.getShareLibJars("pig_9"));
        assertNotNull(shareLibService.getShareLibJars("pig_10"));
        assertNull(shareLibService.getShareLibJars("pig_11"));
    }

    @Test
    public void testShareLib() throws Exception {
        setShipLauncherInOozieConfig();
        FileSystem fs = getFileSystem();
        String dir1 = dt.format(new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        String dir2 = dt.format(new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));
        String dir3 = dt.format(new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS)));
        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path path1 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + dir1);
        Path path2 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + dir2);
        Path path3 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + dir3);
        createDirs(fs, path1, path2, path3);
        createFiles(path1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");

        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertTrue(shareLibService.getShareLibJars("pig").get(0).getName().endsWith("pig.jar"));
    }

    private void validateShareLibLoadFiles(final FileSystem fs, final String schema) throws Exception {
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);

            verifyShareLibFromMappingFileContent(schema, shareLibService);
        }
        finally {
            if (schema.startsWith(HDFS_SCHEME_PREFIX)) {
                fs.delete(new Path(SHARELIB_PATH), true);
                fs.delete(new Path("linkFile.xml"), true);
            }
        }
    }

    private void verifyShareLibFromMappingFileContent(String schema, ShareLibService shareLibService) throws IOException
    {
        assertTrue(shareLibService.getShareLibJars("something_new").get(0).getName().endsWith("somethingNew.jar"));
        assertTrue(shareLibService.getShareLibJars("pig").get(0).getName().endsWith("pig.jar"));
        assertTrue(shareLibService.getShareLibJars("directjar").get(0).getName().endsWith("direct.jar"));
        assertTrue(shareLibService.getShareLibJars("linkFile").get(0).getName().endsWith("targetOfLinkFile.xml"));

        List<Path> listOfPaths = shareLibService.getShareLibJars("directjar");
        for (Path p : listOfPaths) {
            assertTrue(p.toString().startsWith(schema));
        }
    }

    private void setupShareLibLoadFiles(FileSystem fs, String testUserHome) throws ServiceException, IOException {
        createShareLibMetaFileTestResources(fs, testUserHome);
        setShipLauncherInOozieConfig();
        Configuration conf = getOozieConfig();
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + testUserHome + TEST_MAPPING_FILENAME);
    }

    @Test
    public void testShareLibLoadFilesFromLocalFs() throws Exception {
        final FileSystem localFs = newLocalFileSystem();
        final String testUserHome = Files.createTempDir().toString() + Path.SEPARATOR;
        try {
            setupShareLibLoadFiles(localFs, testUserHome);
            validateShareLibLoadFiles(localFs, FSUtils.FILE_SCHEME_PREFIX);
        }
        finally {
            localFs.delete(new Path(testUserHome), true);
        }
    }

    @Test
    public void testShareLibLoadFilesFromHDFS() throws Exception {
        FileSystem fs = getFileSystem();
        setupShareLibLoadFiles(fs, TEST_HDFS_HOME);
        validateShareLibLoadFiles(fs, HDFS_SCHEME_PREFIX);
    }

    @Test
    public void testLoadfromDFS() throws Exception {
        services.init();
        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX
                + ShareLibService.dt.get().format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path ooziePath = new Path(libpath.toString() + Path.SEPARATOR + "oozie");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(ooziePath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);

        createFiles(libpath.toString() + Path.SEPARATOR + "pig_10" + Path.SEPARATOR + "pig-10.jar");
        createFiles(libpath.toString() + Path.SEPARATOR + "oozie" + Path.SEPARATOR + "oozie_luncher.jar");

        String actionXml = "<pig>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node>"
                + "<property><name>oozie.action.sharelib.for.pig</name><value>pig_10</value></property>" + "</pig>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        WorkflowJobBean wfj = new WorkflowJobBean();
        protoConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
        wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
        wfj.setConf(XmlUtils.prettyPrint(protoConf).toString());

        Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());
        PigActionExecutor ae = new PigActionExecutor();
        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        jobConf.set("oozie.action.sharelib.for.pig", "pig_10");
        ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);

        verifyFilesInDistributedCache(DistributedCache.getCacheFiles(jobConf), "pig-10.jar", "oozie_luncher.jar");
    }

    @Test
    public void testShareLibLoadFileMultipleFile() throws Exception {
        FileSystem fs = getFileSystem();
        createTestShareLibMetaFile_multipleFile(fs);
        Configuration conf = getOozieConfig();
        setShipLauncherInOozieConfig();
        setShareLibMappingFileInOozieConfig(fs, conf);

        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertNull(shareLibService.getShareLibJars("something_new"));
        assertEquals(shareLibService.getShareLibJars("pig").size(), 2);
        fs.delete(new Path(SHARELIB_PATH), true);
    }

    private void setShareLibMappingFileInOozieConfig(FileSystem fs, Configuration conf) {
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + TEST_HDFS_HOME + TEST_MAPPING_FILENAME);
    }

    @Test
    public void testMultipleLauncherCall() throws Exception {
        setShipLauncherInOozieConfig();
        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());
        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX
                + ShareLibService.dt.get().format(time));
        fs.mkdirs(libpath);
        Path ooziePath = new Path(libpath.toString() + Path.SEPARATOR + "oozie");
        fs.mkdirs(ooziePath);
        createFiles(libpath.toString() + Path.SEPARATOR + "oozie" + Path.SEPARATOR + "oozie_luncher.jar");
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        List<Path> launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        assertEquals(launcherPath.size(), 2);
        launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        assertEquals(launcherPath.size(), 2);
    }

    @Test
    public void testMetafileSymlink() throws ServiceException, IOException {
        Configuration conf = getOozieConfig();
        setShipLauncherInOozieConfig();
        services.init();
        FileSystem fs = getFileSystem();
        Properties prop = new Properties();
        try {
            Path basePath = new Path(SHARELIB_PATH + "testPath");
            Path basePath1 = new Path(SHARELIB_PATH + "testPath1");
            Path hive_site = new Path(basePath.toString() + Path.SEPARATOR + "hive_conf" + Path.SEPARATOR
                    + "hive-site.xml");
            Path hive_site1 = new Path(basePath.toString() + Path.SEPARATOR + "hive_conf" + Path.SEPARATOR
                    + "hive-site1.xml");
            Path symlink = new Path("symlink/");
            Path symlink_hive_site = new Path("symlink/hive_conf" + Path.SEPARATOR + "hive-site.xml");

            fs.mkdirs(basePath);

            createFiles(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFiles(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_1.jar");

            createFiles(basePath1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_2.jar");
            createFiles(basePath1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_3.jar");
            createFiles(basePath1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_4.jar");

            createFiles(hive_site.toString());

            FSUtils.createSymlink(fs, basePath, symlink, true);
            FSUtils.createSymlink(fs, hive_site, symlink_hive_site, true);

            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", TEST_HDFS_HOME + symlink.toString());
            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".hive_conf", TEST_HDFS_HOME + symlink_hive_site.toString()
                    + "#hive-site.xml");
            createTestShareLibMappingFile(fs, prop);
            assertEquals(FSUtils.isSymlink(fs, symlink), true);

            setShareLibMappingFileInOozieConfig(fs, conf);
            setShipLauncherInOozieConfig();
            try {
                ShareLibService shareLibService = Services.get().get(ShareLibService.class);
                assertEquals(shareLibService.getShareLibJars("pig").size(), 2);
                assertEquals(shareLibService.getShareLibJars("hive_conf").size(), 1);
                FSUtils.createSymlink(fs, basePath1, symlink, true);
                FSUtils.createSymlink(fs, hive_site1, symlink_hive_site, true);
                assertEquals(FSUtils.getSymLinkTarget(fs, shareLibService.getShareLibJars("hive_conf").get(0)),
                        hive_site1);
                assertEquals(shareLibService.getShareLibJars("pig").size(), 3);
            }
            finally {
                fs.delete(symlink, true);
            }
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        finally {
            fs.delete(new Path(SHARELIB_PATH), true);
            fs.delete(new Path(TEST_HDFS_MAPPING_FILE_PATH), true);
        }
    }

    @Test
    public void testDuplicateJarsInDistributedCache() throws Exception {

        FileSystem fs = getFileSystem();
        Path basePath = new Path(getOozieConfig()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        setShipLauncherInOozieConfig();

        // Use timedstamped directory if available
        Date time = new Date(System.currentTimeMillis());
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dt.get().format(time));

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        createDirs(fs, pigPath, new Path(pigPath, "temp"));
        createFiles(new Path(pigPath, "pig.jar"));
        createFiles(new Path(pigPath, "hive.jar"));
        createFiles(new Path(new Path(pigPath, "temp"), "pig.jar#pig.jar"));

        // DistributedCache should have only one pig jar
        verifyFilesInDistributedCache(setUpPigJob(true), "pig.jar", "hive.jar", "MyOozie.jar", "MyPig.jar");
        ShareLibService shareLibService = services.get(ShareLibService.class);
        // sharelib service should have two jars
        List<Path> shareLib = shareLibService.getShareLibJars("pig");
        assertEquals(shareLib.size(), 3);
        assertTrue(shareLib.toString().contains("pig.jar#pig.jar"));
        assertTrue(shareLib.toString().contains("hive.jar"));
    }

    private URI[] setUpPigJob(boolean useSystemSharelib) throws Exception {
        services.init();
        String actionXml = "<pig>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                + getNameNodeUri() + "</name-node></pig>";
        Element eActionXml = XmlUtils.parseXml(actionXml);
        XConfiguration protoConf = new XConfiguration();
        protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        WorkflowJobBean wfj = new WorkflowJobBean();
        protoConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, useSystemSharelib);
        wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
        wfj.setConf(XmlUtils.prettyPrint(protoConf).toString());

        Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());
        PigActionExecutor ae = new PigActionExecutor();
        Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        jobConf.set("oozie.action.sharelib.for.pig", "pig");
        ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
        return DistributedCache.getCacheFiles(jobConf);
    }

    private static void createFiles(FileSystem fs, Path... paths) throws IOException {
        for (Path path : paths) {
            FSDataOutputStream out = fs.create(path);
            out.close();
        }
    }

    private static void createFiles(FileSystem fs, String... filenames) throws IOException {
        Path[] paths = new Path[filenames.length];
        for (int i = 0; i != filenames.length; ++i) {
            paths[i] = new Path(filenames[i]);
        }
        createFiles(fs, paths);
    }

    private void createFiles(String... filenames) throws IOException {
        createFiles(getFileSystem(), filenames);
    }

    private void createFiles(Path... paths) throws IOException {
        createFiles(getFileSystem(), paths);
    }

    private void createShareLibMetaFileTestResources(final FileSystem fs, final String testUserHome)
            throws IOException {
        final String testPath = testUserHome + SHARELIB_PATH;

        final Path basePath = new Path(testPath  + "testPath");
        final Path somethingNew = new Path(testPath + "something_new");
        final Path directJarDir = new Path(testPath + "directJarDir");
        final String directJarPath = directJarDir.toString() + Path.SEPARATOR + "direct.jar";

        final String[] testFiles = {
                basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar" ,
                somethingNew.toString() + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar",
                directJarDir.toString() + Path.SEPARATOR + "direct.jar"};

        final Map<String, String> symlinks = new HashMap<>();
        symlinks.put(testPath + Path.SEPARATOR + "linkDir" + Path.SEPARATOR + "targetOfLinkFile.xml",
                testUserHome + "linkFile.xml");

        final Properties mappingFileConfig = new Properties();
        mappingFileConfig.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig",
                TestShareLibMappingFileInput.getLocalizedShareLibPath(fs, basePath.toString()));
        mappingFileConfig.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".something_new",
                TestShareLibMappingFileInput.getLocalizedShareLibPath(fs, somethingNew.toString()));
        mappingFileConfig.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".directjar",
                TestShareLibMappingFileInput.getLocalizedShareLibPath(fs, directJarPath.toString()));
        String symlink = testUserHome + "linkFile.xml";
        mappingFileConfig.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".linkFile",
                TestShareLibMappingFileInput.getLocalizedShareLibPath(fs, symlink + "#targetOfLinkFile.xml"));

        createTestShareLibMappingFile(fs, testUserHome, Arrays.asList(testFiles), symlinks, mappingFileConfig);
    }

    @Test
    public void testLoadMappingFilesFromDFSandLocalFs() throws IOException, ServiceException {
        final String testUserHome = Files.createTempDir().toString() + Path.SEPARATOR;
        final String testPath = testUserHome + SHARELIB_PATH;
        final Path basePath = new Path(testPath + "testPath");
        final Path somethingNew = new Path(testPath + "something_new");
        final Path directHdfsJarDir = new Path(TEST_HDFS_HOME + SHARELIB_PATH +  "directJarDir");
        final String directHdfsJarPath = directHdfsJarDir.toString() + Path.SEPARATOR + "direct.jar";
        final String directjarShareLibName = "directjar";

        final FileSystem localFs = newLocalFileSystem();
        final FileSystem hdFs = getFileSystem();
        final TestShareLibMappingFileInput[] testShareLibMappingFileInputs = {
                new TestShareLibMappingFileInput(localFs, "pig", basePath.toString() + Path.SEPARATOR + "pig"
                        + Path.SEPARATOR + "pig.jar"),
                new TestShareLibMappingFileInput(localFs, "something_new", somethingNew.toString()
                        + Path.SEPARATOR + "something_new.jar"),
                new TestShareLibMappingFileInput(hdFs, directjarShareLibName, directHdfsJarPath),
                new TestShareLibMappingSymlinkInput(hdFs, "linkFile",
                        TEST_HDFS_HOME + "symlinkTargetDir" + Path.SEPARATOR + "targetOfLinkFile.xml",
                        TEST_HDFS_HOME + "linkFile.xml")
        };

        final Properties mappingFileConfig = new Properties();
        for (final TestShareLibMappingFileInput shmfInput : testShareLibMappingFileInputs) {
            shmfInput.materialize();
            mappingFileConfig.put(shmfInput.sharelibNameWithMappingFilePrefix, shmfInput.getFullShareLibPathDir());
        }

        createTestShareLibMappingFile(testUserHome + TEST_MAPPING_FILENAME, localFs, mappingFileConfig);

        setShipLauncherInOozieConfig();
        final Configuration oozieConfig = getOozieConfig();
        oozieConfig.set(ShareLibService.SHARELIB_MAPPING_FILE, localFs.getUri() + testUserHome + TEST_MAPPING_FILENAME);

        try {
            services.init();
            final ShareLibService shareLibService = Services.get().get(ShareLibService.class);

            for (final TestShareLibMappingFileInput sh : testShareLibMappingFileInputs) {
                final String firstShareLibPath = shareLibService.getShareLibJars(sh.sharelibName).get(0).toString();
                assertTrue(firstShareLibPath.endsWith(sh.baseName));
            }

            final List<Path> listOfPaths = shareLibService.getShareLibJars(directjarShareLibName);
            for (final Path p : listOfPaths) {
                assertTrue(p.toString().startsWith(HDFS_SCHEME_PREFIX));
            }

        }
        finally {
            hdFs.delete(new Path(SHARELIB_PATH), true);
            hdFs.delete(new Path("linkFile.xml"), true);
            localFs.delete(new Path(testUserHome), true);
        }
    }

    private FileSystem newLocalFileSystem() throws IOException {
        final Configuration emptyConfig = new Configuration(false);
        return LocalFileSystem.get(emptyConfig);
    }

    private void setShipLauncherInOozieConfig() {
        Configuration oozieConfig = getOozieConfig();
        oozieConfig.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
    }

    private Configuration getOozieConfig() {
        return services.get(ConfigurationService.class).getConf();
    }

    private void createTestShareLibMappingFile(final FileSystem fs,
                                               final String testUserHome,
                                               final Iterable<String> testFiles,
                                               final Map<String, String> testSymlinks,
                                               final Properties mappingConf) throws IOException {
        createTestShareLibDirsAndFiles(fs, testFiles);
        createTestShareLibDirsAndSymlinks(fs, testSymlinks);
        createTestShareLibMappingFile(testUserHome + TEST_MAPPING_FILENAME, fs, mappingConf);
    }

    private void createTestShareLibDirsAndFiles(final FileSystem fs, final Iterable<String> testFiles)
            throws IOException {
        for (final String f : testFiles) {
            createFileWithDirectoryPath(fs, f);
        }
    }

    static void createFileWithDirectoryPath(final FileSystem fs, final String f) throws IOException {
        if (!fs.exists(new Path(f))) {
            String dirName = f.substring(0, f.toString().lastIndexOf(Path.SEPARATOR));
            fs.mkdirs(new Path(dirName));
        }
        createFiles(fs, f);
    }

    void createTestShareLibDirsAndSymlinks(final FileSystem fs, final Map<String, String> symlinks) throws IOException {
        createTestShareLibDirsAndFiles(fs, symlinks.keySet()); // create symlink targets
        for (Map.Entry<String, String> symlinkEntry : symlinks.entrySet())  {
            String symlinkTarget = symlinkEntry.getKey();
            String symlink = symlinkEntry.getValue();
            FSUtils.createSymlink(fs, new Path(symlinkTarget), new Path(symlink), true);
        }
    }

    private void createTestShareLibMappingFile(final FileSystem fs, final Properties prop) {
        createTestShareLibMappingFile(TEST_HDFS_MAPPING_FILE_PATH, fs, prop);
    }

    private void createTestShareLibMappingFile(final String metaFile, final FileSystem fs, final Properties prop) {
        try (final FSDataOutputStream out = fs.create(new Path(metaFile))) {
            prop.store(out, null);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void createTestShareLibMetaFile_multipleFile(FileSystem fs) {
        Properties prop = new Properties();

        try {
            Path basePath = new Path(SHARELIB_PATH + "testPath");
            Path somethingNew = new Path(SHARELIB_PATH + "something_new");
            fs.mkdirs(basePath);
            fs.mkdirs(somethingNew);

            createFiles(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFiles(somethingNew.toString() + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", TEST_HDFS_HOME + basePath.toString()
                    + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar," + TEST_HDFS_HOME + somethingNew.toString()
                    + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            FSDataOutputStream out = fs.create(new Path(TEST_HDFS_MAPPING_FILE_PATH));

            prop.store(out, null);
            out.close();

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void createDirs(FileSystem fs, Path... paths) throws IOException {
        for (Path path : paths) {
            fs.mkdirs(path);
        }
    }

    @Test
    public void testConfFileAddedToActionConf() throws Exception {
        try {
            XConfiguration protoConf = new XConfiguration();
            protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
            WorkflowJobBean wfj = new WorkflowJobBean();
            protoConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
            wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
            wfj.setConf(XmlUtils.prettyPrint(protoConf).toString());

            Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());

            // Test hive-site.xml in sharelib cache
            setupSharelibConf("hive-site.xml", "oozie.hive_conf");
            ShareLibService shareLibService = services.get(ShareLibService.class);
            assertEquals(shareLibService.getShareLibConfigMap().get("hive_conf").values().size(), 1);
            assertEquals(
                    shareLibService.getShareLibConfigMap().get("hive_conf").keySet().toArray(new Path[] {})[0]
                            .getName(),
                    "hive-site.xml");

            // Test hive-site.xml not in distributed cache
            setupSharelibConf("hive-site.xml", "oozie.hive_conf");
            String actionXml = "<hive>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                    + getNameNodeUri() + "</name-node>" + "<script>test</script>" + "</hive>";
            Element eActionXml = XmlUtils.parseXml(actionXml);

            HiveActionExecutor ae = new HiveActionExecutor();
            Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);

            Configuration actionConf = ae.createBaseHadoopConf(context, eActionXml);
            jobConf.set("oozie.action.sharelib.for.hive", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            URI[] cacheFiles = DistributedCache.getCacheFiles(actionConf);
            String cacheFilesStr = Arrays.toString(cacheFiles);
            assertFalse(cacheFilesStr.contains("hive-site.xml"));

            // Test hive-site.xml property in jobconf with linkname
            jobConf = ae.createBaseHadoopConf(context, eActionXml);
            Properties prop = new Properties();
            actionConf = ae.createBaseHadoopConf(context, eActionXml);
            prop.put("oozie.hive_conf", TEST_HDFS_HOME + SHARELIB_PATH + "hive-site.xml#hive-site.xml");
            setupSharelibConf("hive-site.xml", "oozie.hive_conf", prop);
            jobConf.set("oozie.action.sharelib.for.hive", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            assertEquals(jobConf.get("oozie.hive_conf-sharelib-test"), "test");

            // Test hive-site.xml property in jobconf with linkname
            // and with hdfs path
            prop = new Properties();
            jobConf = ae.createBaseHadoopConf(context, eActionXml);
            actionConf = ae.createBaseHadoopConf(context, eActionXml);
            prop.put("oozie.hive_conf", "hdfs://" + TEST_HDFS_HOME + SHARELIB_PATH + "hive-site.xml#hive-site.xml");
            setupSharelibConf("hive-site.xml", "oozie.hive_conf", prop);
            jobConf.set("oozie.action.sharelib.for.hive", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            assertEquals(jobConf.get("oozie.hive_conf-sharelib-test"), "test");
            cacheFiles = DistributedCache.getCacheFiles(actionConf);
            cacheFilesStr = Arrays.toString(cacheFiles);
            assertFalse(cacheFilesStr.contains("hive-site.xml"));

            // Test hive-site.xml property in jobconf with non hdfs path
            prop = new Properties();
            jobConf = ae.createBaseHadoopConf(context, eActionXml);
            actionConf = ae.createBaseHadoopConf(context, eActionXml);
            prop.put("oozie.hive_conf", TEST_HDFS_HOME + SHARELIB_PATH + "hive-site.xml");
            setupSharelibConf("hive-site.xml", "oozie.hive_conf", prop);
            jobConf.set("oozie.action.sharelib.for.hive", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            assertEquals(jobConf.get("oozie.hive_conf-sharelib-test"), "test");
            cacheFiles = DistributedCache.getCacheFiles(actionConf);
            cacheFilesStr = Arrays.toString(cacheFiles);
            assertFalse(cacheFilesStr.contains("hive-site.xml"));

            // Test hive-site.xml property in jobconf with non hdfs path with
            // link name
            prop = new Properties();
            jobConf = ae.createBaseHadoopConf(context, eActionXml);
            actionConf = ae.createBaseHadoopConf(context, eActionXml);
            prop.put("oozie.hive_conf", TEST_HDFS_HOME + SHARELIB_PATH + "hive-site.xml#hive-site.xml");
            setupSharelibConf("hive-site.xml", "oozie.hive_conf", prop);
            jobConf.set("oozie.action.sharelib.for.hive", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            assertEquals(jobConf.get("oozie.hive_conf-sharelib-test"), "test");
            cacheFiles = DistributedCache.getCacheFiles(actionConf);
            cacheFilesStr = Arrays.toString(cacheFiles);
            assertFalse(cacheFilesStr.contains("hive-site.xml"));
        }
        finally {
            getFileSystem().delete(new Path(SHARELIB_PATH), true);
        }
    }

    @Test
    public void testConfFileAddedToDistributedCache() throws Exception {
        try {

            Properties prop = new Properties();
            prop.put("oozie.hive_conf", TEST_HDFS_HOME + SHARELIB_PATH + "hive-site.xml#hive-site.xml");
            setupSharelibConf("hive-site.xml", "oozie.hive_conf", prop);

            String actionXml = "<pig>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" + "<name-node>"
                    + getNameNodeUri() + "</name-node>" + "<script>test</script>" + "</pig>";
            Element eActionXml = XmlUtils.parseXml(actionXml);
            XConfiguration protoConf = new XConfiguration();
            protoConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
            WorkflowJobBean wfj = new WorkflowJobBean();
            protoConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
            wfj.setProtoActionConf(XmlUtils.prettyPrint(protoConf).toString());
            wfj.setConf(XmlUtils.prettyPrint(protoConf).toString());

            Context context = new TestJavaActionExecutor().new Context(wfj, new WorkflowActionBean());

            PigActionExecutor ae = new PigActionExecutor();
            Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
            jobConf.set("oozie.action.sharelib.for.pig", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
            String cacheFilesStr = Arrays.toString(cacheFiles);
            assertEquals(jobConf.get("oozie.hive_conf-sharelib-test"), null);
            assertTrue(URLDecoder.decode(cacheFilesStr).contains("hive-site.xml#hive-site.xml"));

            setupSharelibConf("hbase-site.xml", "oozie.hbase_conf");
            jobConf = ae.createBaseHadoopConf(context, eActionXml);
            jobConf.set("oozie.action.sharelib.for.pig", "hbase_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            cacheFiles = DistributedCache.getCacheFiles(jobConf);
            cacheFilesStr = Arrays.toString(cacheFiles);
            assertTrue(cacheFilesStr.contains("hbase-site.xml"));

        }
        finally {
            getFileSystem().delete(new Path(SHARELIB_PATH), true);
        }
    }

    @Test
    public void testParsingALotOfShareLibsParallel() throws ServiceException, IOException {
        setShipLauncherInOozieConfig();
        services.init();
        // destroying, as we dont want the sharelib dirs purge to be scheduled
        services.get(SchedulerService.class).destroy();

        final List<FileStatus> fileStatuses = new ArrayList<>();

        final Path rootDir = Mockito.mock(Path.class);
        final FileSystem fs = Mockito.mock(FileSystem.class);

        final int NUMBER_OF_FILESTATUSES = 100;

        for (int i = 0; i < NUMBER_OF_FILESTATUSES; ++i) {
            createAndAddMockedFileStatus(fileStatuses, 2018, 8, 1, 0, 0, 1);
        }

        final FileStatus[] statuses = fileStatuses.toArray(new FileStatus[1]);
        Mockito.when(fs.listStatus(Mockito.any(Path.class), Mockito.any(PathFilter.class))).thenReturn(statuses);

        final ShareLibService shareLibService = services.get(ShareLibService.class);
        shareLibService.fs = fs;

        runGivenCallableOnThreads(() -> {
            try {
                shareLibService.getLatestLibPath(rootDir, "lib_");
            } catch (final IOException | NumberFormatException e) {
                log.error(e.getMessage());
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }, 10, 10);
    }

    @Test
    public void testDeterminingLatestSharelibPathOn1Thread() throws IOException, ServiceException {
        testDeterminingLatestSharelibPath(1);
    }

    @Test
    public void testDeterminingLatestSharelibPathOn5Threads()  throws IOException, ServiceException {
        testDeterminingLatestSharelibPath(5);
    }

    @Test
    public void testDeterminingLatestSharelibPathOn10Threads() throws IOException, ServiceException {
        testDeterminingLatestSharelibPath(10);
    }

    private void testDeterminingLatestSharelibPath(final int numberOfThreads) throws ServiceException, IOException {
        setShipLauncherInOozieConfig();
        services.init();
        // destroying, as we dont want the sharelib dirs purge to be scheduled
        services.get(SchedulerService.class).destroy();

        final List<FileStatus> fileStatuses = new ArrayList<>();
        createAndAddMockedFileStatus(fileStatuses, 2018, 8, 1, 0, 0, 1);
        createAndAddMockedFileStatus(fileStatuses, 2018, 8, 1, 0, 1, 1);
        final Path filePath3 = createAndAddMockedFileStatus(fileStatuses, 2018, 8, 1, 1, 0, 1);

        final Path rootDir = Mockito.mock(Path.class);
        final FileSystem fs = Mockito.mock(FileSystem.class);

        final FileStatus[] statuses = fileStatuses.toArray(new FileStatus[1]);
        Mockito.when(fs.listStatus(Mockito.any(Path.class), Mockito.any(PathFilter.class))).thenReturn(statuses);

        final ShareLibService shareLibService = services.get(ShareLibService.class);
        shareLibService.fs = fs;

        runGivenCallableOnThreads(() -> {
            try {
                final Path path = shareLibService.getLatestLibPath(rootDir, "lib_");
                Assert.assertEquals(filePath3, path);
            } catch (final IOException | NumberFormatException e) {
                log.error(e.getMessage());
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }, 100, numberOfThreads);
    }

    private void runGivenCallableOnThreads(
            final Callable<Boolean> callable, final int numberOfCallables, final int numberOfThreads) {
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final List<Callable<Boolean>> callableTasks = new ArrayList<>();

        for (int i = 0; i < numberOfCallables; ++i) {
            callableTasks.add(callable);
        }

        // Start 10 thread to do parallel time parsing. Issue is experienced with old code.
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            futures = executor.invokeAll(callableTasks);
        } catch (final InterruptedException e) {
            log.error(e.getMessage());
            Assert.fail("Determining timestamp of a share lib name is failed with: " + e.getMessage());
        }

        // Shut down the executor service to have all tasks finished, then collect the results
        awaitTerminationAfterShutdown(executor);

        Assert.assertFalse(futures.isEmpty());
        for (final Future<Boolean> f : futures) {
            try {
                final Boolean result = f.get(5, TimeUnit.SECONDS);
                Assert.assertTrue("Parsed share lib name shall be a valid timestamp", result);
            } catch (final InterruptedException | ExecutionException | TimeoutException e) {
                log.error(e.getMessage());
                Assert.fail("Determining timestamp of a share lib name is failed with: " + e.getMessage());
            }
        }
    }

    private Path createAndAddMockedFileStatus(final List<FileStatus> fileStatuses, int y, int m, int d, int h, int min, int s) {
        final String date = new SimpleDateFormat("yyyyMMddHHmmss").format(
                new Calendar.Builder().setDate(y, m, d).setTimeOfDay(h, min, s).build().getTime());
        final Path filePath = Mockito.mock(Path.class);
        final String libName = ShareLibService.SHARE_LIB_PREFIX + date;
        Mockito.when(filePath.getName()).thenReturn(libName);
        final FileStatus fileStatus = Mockito.mock(FileStatus.class);
        Mockito.when(fileStatus.getPath()).thenReturn(filePath);
        fileStatuses.add(fileStatus);
        return filePath;
    }

    private void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void setupSharelibConf(final String file, final String tag) throws ServiceException, IOException {
        Properties prop = new Properties();
        prop.put(tag, TEST_HDFS_HOME + SHARELIB_PATH);
        setupSharelibConf(file, tag, prop);
    }

    private void setupSharelibConf(final String file, final String tag, Properties prop) throws IOException,
            ServiceException {
        setupSharelibConf(getFileSystem(), file, tag, prop);
    }

    private void setupSharelibConf(final FileSystem fs, final String file, final String tag, Properties prop)
            throws IOException, ServiceException {
        Configuration conf = getOozieConfig();
        setShipLauncherInOozieConfig();
        setShareLibMappingFileInOozieConfig(fs, conf);

        XConfiguration hiveConf = new XConfiguration();
        hiveConf.set(tag + "-sharelib-test", "test");
        createDirs(getFileSystem(), new Path(SHARELIB_PATH));
        FSDataOutputStream out = getFileSystem().create(new Path(SHARELIB_PATH, file), true);
        PrintWriter bufOut = new PrintWriter(out);
        bufOut.write(hiveConf.toXmlString(false));
        bufOut.close();
        createTestShareLibMappingFile(getFileSystem(), prop);

        services.init();
    }

    private void verifyFilesInDistributedCache(URI[] cacheFiles, String... files) {
        String cacheFilesStr = Arrays.toString(cacheFiles);
        // Hadoop 2 has the following jars too: MRAppJar.jar and hadoop-mapreduce-client-jobclient-
        assertEquals(cacheFiles.length, files.length + 2);
        assertTrue(cacheFilesStr.contains("MRAppJar.jar"));
        assertTrue(cacheFilesStr.contains("hadoop-mapreduce-client-jobclient-"));

        for (String file : files) {
            assertTrue(cacheFilesStr.contains(file));
        }
    }
}
