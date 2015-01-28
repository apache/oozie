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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.HiveActionExecutor;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.PigActionExecutor;
import org.apache.oozie.action.hadoop.TestJavaActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.hadoop.utils.HadoopShims;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.Test;

public class TestShareLibService extends XFsTestCase {

    Services services;
    private static String testCaseDirPath;
    String shareLibPath = "shareLibPath";
    SimpleDateFormat dt = new SimpleDateFormat("yyyyMMddHHmmss");
    final String sharelibPath = "sharelib";
    final String metaFile = "/user/test/config.properties";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        testCaseDirPath = getTestCaseDir();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void setSystemProps() throws IOException {
        IOUtils.createJar(new File(getTestCaseDir()), MyOozie.class.getName() + ".jar", MyOozie.class);
        IOUtils.createJar(new File(getTestCaseDir()), MyPig.class.getName() + ".jar", MyPig.class);
        IOUtils.createJar(new File(getTestCaseDir()), TestHive.class.getName() + ".jar", TestHive.class);

        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(WorkflowAppService.SYSTEM_LIB_PATH, getFsTestCaseDir() + "/share/lib");
        conf.set(Services.CONF_SERVICE_CLASSES, conf.get(Services.CONF_SERVICE_CLASSES) + ","
                + DummyShareLibService.class.getName());
        conf.setStrings(ActionService.CONF_ACTION_EXECUTOR_CLASSES, DummyPigActionExecutor.class.getName(),
                DummyHiveActionExecutor.class.getName());

    }

    public static class DummyShareLibService extends ShareLibService {
        @Override
        public String findContainingJar(Class clazz) {
            if (JavaActionExecutor.getCommonLauncherClasses().contains(clazz)) {
                return testCaseDirPath + "/" + MyOozie.class.getName() + ".jar";
            }
            return testCaseDirPath + "/" + clazz.getName() + ".jar";
        }
    }

    public static class DummyPigActionExecutor extends PigActionExecutor {
        public DummyPigActionExecutor() {
        }

        @Override
        public List<Class> getLauncherClasses() {
            return Arrays.asList((Class) MyPig.class);
        }
    }

    public static class DummyHiveActionExecutor extends HiveActionExecutor {
        public DummyHiveActionExecutor() {
        }

        @Override
        public List<Class> getLauncherClasses() {
            return Arrays.asList((Class) TestHive.class);
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
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
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
        finally {
            services.destroy();
        }
    }

    @Test
    public void testCreateLauncherLibPath() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            List<Path> launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
            assertNotNull(launcherPath);
            assertTrue(getFileSystem().exists(launcherPath.get(0)));
            List<Path> pigLauncherPath = shareLibService.getSystemLibJars("pig");
            assertTrue(getFileSystem().exists(pigLauncherPath.get(0)));
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testAddShareLibDistributedCache() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");

        try {
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
        finally {
            services.destroy();
        }
    }

    @Test
    public void testAddShareLib_pig() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        try {
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
        finally {
            services.destroy();
        }
    }

    @Test
    public void testAddShareLib_pig_withVersion() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");

        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dateFormat.format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);

        createFile(libpath.toString() + Path.SEPARATOR + "pig_10" + Path.SEPARATOR + "pig-10.jar");

        try {
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
        finally {
            services.destroy();
        }
    }

    @Test
    public void testPurgeShareLib() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        FileSystem fs = getFileSystem();
        long expiryTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.convert(
                        services.get(ConfigurationService.class).getConf()
                                .getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7), TimeUnit.DAYS);

        // for directory created 8 days back to be deleted
        String expireTs = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 6 days back NOT to be deleted
        String noexpireTs = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 5 days back NOT to be deleted
        String noexpireTs1 = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));

        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path expirePath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + expireTs);
        Path noexpirePath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + noexpireTs);
        Path noexpirePath1 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + noexpireTs1);

        createDirs(fs, expirePath, noexpirePath, noexpirePath1);
        try {
            services.init();
            assertEquals(4, fs.listStatus(basePath).length);
            assertTrue(fs.exists(noexpirePath));
            assertTrue(fs.exists(noexpirePath1));
            assertTrue(fs.exists(expirePath));
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testPurgeLauncherJar() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        FileSystem fs = getFileSystem();

        long expiryTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.convert(
                        services.get(ConfigurationService.class).getConf()
                                .getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7), TimeUnit.DAYS);

        // for directory created 8 days back to be deleted
        String expireTs = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 6 days back NOT to be deleted
        String noexpireTs = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        // for directory created 5 days back NOT to be deleted
        String noexpireTs1 = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));

        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path expirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + expireTs);
        Path noexpirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs);

        Path noexpirePath1 = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs1);
        createDirs(fs, expirePath, noexpirePath, noexpirePath1);

        try {
            services.init();
            assertEquals(4, fs.listStatus(basePath).length);
            assertTrue(fs.exists(noexpirePath));
            assertTrue(fs.exists(noexpirePath1));
            assertTrue(fs.exists(expirePath));
        }
        finally {
            services.destroy();
        }
    }

    // Logic is to keep all share-lib between current timestamp and 7days old + 1 latest sharelib older than 7 days.
    // refer OOZIE-1761
    @Test
    public void testPurgeJar() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        final FileSystem fs = getFileSystem();
        // for directory created 8 days back to be deleted
        long expiryTime = System.currentTimeMillis()
                - TimeUnit.MILLISECONDS.convert(
                        services.get(ConfigurationService.class).getConf()
                                .getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7), TimeUnit.DAYS);

        String expireTs = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        String expireTs1 = dt.format(new Date(expiryTime - TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));
        String noexpireTs = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        String noexpireTs1 = dt.format(new Date(expiryTime + TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));
        final Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));

        Path expirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + expireTs);
        Path expirePath1 = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + expireTs1);
        Path noexpirePath = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs);
        Path noexpirePath1 = new Path(basePath, ShareLibService.LAUNCHER_LIB_PREFIX + noexpireTs1);

        createDirs(fs, expirePath, expirePath1, noexpirePath, noexpirePath1);
        try {
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
        finally {
            services.destroy();
        }
    }

    @Test
    public void testGetShareLibCompatible() throws Exception {
        services = new Services();
        setSystemProps();
        FileSystem fs = getFileSystem();
        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));

        // Use basepath if there is no timestamped directory
        fs.mkdirs(basePath);
        Path pigPath = new Path(basePath.toString() + Path.SEPARATOR + "pig");
        fs.mkdirs(pigPath);
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            assertNotNull(shareLibService.getShareLibJars("pig"));
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testGetShareLibPath() throws Exception {
        services = new Services();
        setSystemProps();
        FileSystem fs = getFileSystem();
        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));

        // Use timedstamped directory if available
        Date time = new Date(System.currentTimeMillis());
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dateFormat.format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            assertNotNull(shareLibService.getShareLibJars("pig"));
            assertNotNull(shareLibService.getShareLibJars("pig_9"));
            assertNotNull(shareLibService.getShareLibJars("pig_10"));
            assertNull(shareLibService.getShareLibJars("pig_11"));
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testShareLib() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        FileSystem fs = getFileSystem();
        String dir1 = dt.format(new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS)));
        String dir2 = dt.format(new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(2, TimeUnit.DAYS)));
        String dir3 = dt.format(new Date(System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(3, TimeUnit.DAYS)));
        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path path1 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + dir1);
        Path path2 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + dir2);
        Path path3 = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + dir3);
        createDirs(fs, path1, path2, path3);
        createFile(path1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            assertTrue(shareLibService.getShareLibJars("pig").get(0).getName().endsWith("pig.jar"));
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testShareLibLoadFile() throws Exception {
        services = new Services();
        FileSystem fs = getFileSystem();
        setSystemProps();
        createTestShareLibMetaFile(fs);
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + "/user/test/config.properties");
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            assertTrue(shareLibService.getShareLibJars("something_new").get(0).getName().endsWith("somethingNew.jar"));
            assertTrue(shareLibService.getShareLibJars("pig").get(0).getName().endsWith("pig.jar"));
            fs.delete(new Path("shareLibPath/"), true);
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testLoadfromDFS() throws Exception {
        services = new Services();
        setSystemProps();
        try {
            services.init();
            FileSystem fs = getFileSystem();
            Date time = new Date(System.currentTimeMillis());

            Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                    .get(WorkflowAppService.SYSTEM_LIB_PATH));
            Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX
                    + ShareLibService.dateFormat.format(time));
            fs.mkdirs(libpath);

            Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
            Path ooziePath = new Path(libpath.toString() + Path.SEPARATOR + "oozie");
            Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
            Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
            fs.mkdirs(pigPath);
            fs.mkdirs(ooziePath);
            fs.mkdirs(pigPath1);
            fs.mkdirs(pigPath2);

            createFile(libpath.toString() + Path.SEPARATOR + "pig_10" + Path.SEPARATOR + "pig-10.jar");
            createFile(libpath.toString() + Path.SEPARATOR + "oozie" + Path.SEPARATOR + "oozie_luncher.jar");

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
        finally {
            services.destroy();
        }
    }

    @Test
    public void testShareLibLoadFileMultipleFile() throws Exception {
        FileSystem fs = getFileSystem();
        services = new Services();
        createTestShareLibMetaFile_multipleFile(fs);
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + "/user/test/config.properties");
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        try {
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            assertNull(shareLibService.getShareLibJars("something_new"));
            assertEquals(shareLibService.getShareLibJars("pig").size(), 2);
            fs.delete(new Path("shareLibPath/"), true);
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testMultipleLauncherCall() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        try {
            FileSystem fs = getFileSystem();
            Date time = new Date(System.currentTimeMillis());
            Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                    .get(WorkflowAppService.SYSTEM_LIB_PATH));
            Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX
                    + ShareLibService.dateFormat.format(time));
            fs.mkdirs(libpath);
            Path ooziePath = new Path(libpath.toString() + Path.SEPARATOR + "oozie");
            fs.mkdirs(ooziePath);
            createFile(libpath.toString() + Path.SEPARATOR + "oozie" + Path.SEPARATOR + "oozie_luncher.jar");
            services.init();
            ShareLibService shareLibService = Services.get().get(ShareLibService.class);
            List<Path> launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
            assertEquals(launcherPath.size(), 2);
            launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
            assertEquals(launcherPath.size(), 2);
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testMetafileSymlink() throws ServiceException, IOException {
        // Assume.assumeTrue("Skipping for hadoop - 1.x",HadoopFileSystem.isSymlinkSupported());
        if (!HadoopShims.isSymlinkSupported()) {
            return;
        }

        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        services.init();
        FileSystem fs = getFileSystem();
        Properties prop = new Properties();
        try {

            String testPath = "shareLibPath/";

            Path basePath = new Path(testPath + Path.SEPARATOR + "testPath");
            Path basePath1 = new Path(testPath + Path.SEPARATOR + "testPath1");
            Path hive_site = new Path(basePath.toString() + Path.SEPARATOR + "hive_conf" + Path.SEPARATOR
                    + "hive-site.xml");
            Path hive_site1 = new Path(basePath.toString() + Path.SEPARATOR + "hive_conf" + Path.SEPARATOR
                    + "hive-site1.xml");
            Path symlink = new Path("symlink/");
            Path symlink_hive_site = new Path("symlink/hive_conf" + Path.SEPARATOR + "hive-site.xml");

            fs.mkdirs(basePath);

            createFile(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFile(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_1.jar");

            createFile(basePath1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_2.jar");
            createFile(basePath1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_3.jar");
            createFile(basePath1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig_4.jar");

            createFile(hive_site.toString());

            HadoopShims fileSystem = new HadoopShims(fs);
            fileSystem.createSymlink(basePath, symlink, true);
            fileSystem.createSymlink(hive_site, symlink_hive_site, true);

            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", "/user/test/" + symlink.toString());
            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".hive_conf", "/user/test/" + symlink_hive_site.toString()
                    + "#hive-site.xml");
            createTestShareLibMetaFile(fs, prop);
            assertEquals(fileSystem.isSymlink(symlink), true);

            conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + "/user/test/config.properties");
            conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
            try {
                ShareLibService shareLibService = Services.get().get(ShareLibService.class);
                assertEquals(shareLibService.getShareLibJars("pig").size(), 2);
                assertEquals(shareLibService.getShareLibJars("hive_conf").size(), 1);
                new HadoopShims(fs).createSymlink(basePath1, symlink, true);
                new HadoopShims(fs).createSymlink(hive_site1, symlink_hive_site, true);
                assertEquals(new HadoopShims(fs).getSymLinkTarget(shareLibService.getShareLibJars("hive_conf").get(0)),
                        hive_site1);
                assertEquals(shareLibService.getShareLibJars("pig").size(), 3);
            }
            finally {
                fs.delete(new Path("shareLibPath/"), true);
                fs.delete(new Path(metaFile), true);
                fs.delete(new Path("/user/test/config.properties"), true);

                fs.delete(symlink, true);
                services.destroy();
            }
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testDuplicateJarsInDistributedCache() throws Exception {
        services = new Services();
        setSystemProps();
        FileSystem fs = getFileSystem();
        Path basePath = new Path(services.get(ConfigurationService.class).getConf()
                .get(WorkflowAppService.SYSTEM_LIB_PATH));
        Configuration conf = services.get(ConfigurationService.class).getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");

        // Use timedstamped directory if available
        Date time = new Date(System.currentTimeMillis());
        Path libpath = new Path(basePath, ShareLibService.SHARE_LIB_PREFIX + ShareLibService.dateFormat.format(time));

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        createDirs(fs, pigPath, new Path(pigPath, "temp"));
        createFile(new Path(pigPath, "pig.jar"));
        createFile(new Path(pigPath, "hive.jar"));
        createFile(new Path(new Path(pigPath, "temp"), "pig.jar#pig.jar"));

        try {

            // DistributedCache should have only one pig jar
            verifyFilesInDistributedCache(setUpPigJob(true), "pig.jar", "hive.jar", "MyOozie.jar", "MyPig.jar");
            ShareLibService shareLibService = services.get(ShareLibService.class);
            // sharelib service should have two jars
            List<Path> shareLib = shareLibService.getShareLibJars("pig");
            assertEquals(shareLib.size(), 3);
            assertTrue(shareLib.toString().contains("pig.jar#pig.jar"));
            assertTrue(shareLib.toString().contains("hive.jar"));
        }
        finally {
            services.destroy();
        }
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

    private void createFile(String... filenames) throws IOException {
        for (String filename : filenames) {
            Path path = new Path(filename);
            createFile(path);
        }
    }

    private void createFile(Path... paths) throws IOException {
        for (Path path : paths) {
            FSDataOutputStream out = getFileSystem().create(path);
            out.close();
        }
    }

    private void createTestShareLibMetaFile(FileSystem fs) {
        Properties prop = new Properties();

        try {

            String testPath = "shareLibPath/";

            Path basePath = new Path(testPath + Path.SEPARATOR + "testPath");

            Path somethingNew = new Path(testPath + Path.SEPARATOR + "something_new");
            fs.mkdirs(basePath);
            fs.mkdirs(somethingNew);

            createFile(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFile(somethingNew.toString() + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", "/user/test/" + basePath.toString());
            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".something_new", "/user/test/" + somethingNew.toString());
            createTestShareLibMetaFile(fs, prop);

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void createTestShareLibMetaFile(FileSystem fs, Properties prop) {
        try {
            FSDataOutputStream out = fs.create(new Path(metaFile));
            prop.store(out, null);
            out.close();

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void createTestShareLibMetaFile_multipleFile(FileSystem fs) {

        Properties prop = new Properties();
        try {

            String testPath = "shareLibPath/";

            Path basePath = new Path(testPath + Path.SEPARATOR + "testPath");
            Path somethingNew = new Path(testPath + Path.SEPARATOR + "something_new");
            fs.mkdirs(basePath);
            fs.mkdirs(somethingNew);

            createFile(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFile(somethingNew.toString() + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            prop.put(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", "/user/test/" + basePath.toString()
                    + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar," + "/user/test/" + somethingNew.toString()
                    + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            FSDataOutputStream out = fs.create(new Path(metaFile));

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
            prop.put("oozie.hive_conf", "/user/test/" + sharelibPath + "/hive-site.xml#hive-site.xml");
            setupSharelibConf("hive-site.xml", "oozie.hive_conf", prop);
            jobConf.set("oozie.action.sharelib.for.hive", "hive_conf");
            ae.setLibFilesArchives(context, eActionXml, new Path("hdfs://dummyAppPath"), jobConf);
            assertEquals(jobConf.get("oozie.hive_conf-sharelib-test"), "test");

        }
        finally {
            getFileSystem().delete(new Path(sharelibPath), true);
            services.destroy();
        }
    }

    @Test
    public void testConfFileAddedToDistributedCache() throws Exception {
        try {

            Properties prop = new Properties();
            prop.put("oozie.hive_conf", "/user/test/" + sharelibPath + "/hive-site.xml#hive-site.xml");
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
            getFileSystem().delete(new Path(sharelibPath), true);
            services.destroy();
        }
    }

    private void setupSharelibConf(final String file, final String tag) throws ServiceException, IOException {
        Properties prop = new Properties();
        prop.put(tag, "/user/test/" + sharelibPath);
        setupSharelibConf(file, tag, prop);

    }

    private void setupSharelibConf(final String file, final String tag, Properties prop) throws IOException,
            ServiceException {
        services = new Services();
        setSystemProps();
        Configuration conf = services.get(ConfigurationService.class).getConf();

        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, getFileSystem().getUri() + "/user/test/config.properties");

        XConfiguration hiveConf = new XConfiguration();
        hiveConf.set(tag + "-sharelib-test", "test");
        createDirs(getFileSystem(), new Path(sharelibPath));
        FSDataOutputStream out = getFileSystem().create(new Path(sharelibPath, file));
        PrintWriter bufOut = new PrintWriter(out);
        bufOut.write(hiveConf.toXmlString(false));
        bufOut.close();
        createTestShareLibMetaFile(getFileSystem(), prop);

        services.init();
    }

    private void verifyFilesInDistributedCache(URI[] cacheFiles, String... files) {

        String cacheFilesStr = Arrays.toString(cacheFiles);
        if (new HadoopShims(getFileSystem()).isYARN()) {
            // Hadoop 2 has two extra jars
            assertEquals(cacheFiles.length, files.length + 2);
            assertTrue(cacheFilesStr.contains("MRAppJar.jar"));
            assertTrue(cacheFilesStr.contains("hadoop-mapreduce-client-jobclient-"));

        }
        else {
            assertEquals(cacheFiles.length, files.length);
        }
        for (String file : files) {
            assertTrue(cacheFilesStr.contains(file));

        }

    }

}
