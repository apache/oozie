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
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.PigActionExecutor;
import org.apache.oozie.action.hadoop.TestJavaActionExecutor;
import org.apache.oozie.client.OozieClient;
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
        Configuration conf = services.getConf();
        conf.set(WorkflowAppService.SYSTEM_LIB_PATH, getFsTestCaseDir() + "/share/lib");
        conf.set(Services.CONF_SERVICE_CLASSES, conf.get(Services.CONF_SERVICE_CLASSES) + ","
                + DummyShareLibService.class.getName());
        conf.set(ActionService.CONF_ACTION_EXECUTOR_CLASSES, DummyPigActionExecutor.class.getName());
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

    static class MyOozie {
    }

    static class MyPig {
    }

    @Test
    public void testCreateLauncherLibPath() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        List<Path> launcherPath = shareLibService.getSystemLibJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        assertNotNull(launcherPath);
        assertTrue(getFileSystem().exists(launcherPath.get(0)));
        List<Path> pigLauncherPath = shareLibService.getSystemLibJars("pig");
        assertTrue(getFileSystem().exists(pigLauncherPath.get(0)));

        services.destroy();

    }

    @Test
    public void testAddShareLibDistributedCache() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");

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

        URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        String cacheFilesStr = Arrays.toString(cacheFiles);
        assertTrue(cacheFilesStr.contains(MyPig.class.getName() + ".jar"));
        assertTrue(cacheFilesStr.contains(MyOozie.class.getName() + ".jar"));
        // Hadoop 2 has two extra jars
        if (cacheFiles.length == 4) {
            assertTrue(cacheFilesStr.contains("MRAppJar.jar"));
            assertTrue(cacheFilesStr.contains("hadoop-mapreduce-client-jobclient-"));
        }
        else {
            assertEquals(2, cacheFiles.length);
        }
        services.destroy();
    }

    @Test
    public void testAddShareLib_pig() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
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

        URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        assertEquals(2, cacheFiles.length);
        assertTrue(cacheFiles[0].getPath().endsWith("MyPig.jar") || cacheFiles[1].getPath().endsWith("MyPig.jar"));
        services.destroy();

    }

    @Test
    public void testAddShareLib_pig_withVersion() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");

        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(services.getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + ShareLibService.dateFormat.format(time));
        fs.mkdirs(libpath);

        Path pigPath = new Path(libpath.toString() + Path.SEPARATOR + "pig");
        Path pigPath1 = new Path(libpath.toString() + Path.SEPARATOR + "pig_9");
        Path pigPath2 = new Path(libpath.toString() + Path.SEPARATOR + "pig_10");
        fs.mkdirs(pigPath);
        fs.mkdirs(pigPath1);
        fs.mkdirs(pigPath2);

        createFile(libpath.toString() + Path.SEPARATOR + "pig_10" + Path.SEPARATOR + "pig-10.jar");

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

        URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        assertEquals(3, cacheFiles.length);
        assertTrue(cacheFiles[0].getPath().endsWith("pig-10.jar") || cacheFiles[1].getPath().endsWith("pig-10.jar")
                || cacheFiles[2].getPath().endsWith("pig-10.jar"));
        services.destroy();

    }

    @Test
    public void testPurgeShareLib() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        FileSystem fs = getFileSystem();
        // for directory created 8 days back to be deleted
        int expire1 = services.getConf().getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7) + 1;
        // for directory created 6 days back NOT to be deleted
        int noexpire = services.getConf().getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7) - 1;
        // for directory created 5 days back NOT to be deleted
        int noexpire1 = services.getConf().getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7) - 2;

        Date expireDate = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * expire1));
        Date noexpireDate = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * noexpire));
        Date noexpireDate1 = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * noexpire1));
        String expireTs = new SimpleDateFormat("yyyyMMddHHmmss").format(expireDate);
        String noexpireTs = new SimpleDateFormat("yyyyMMddHHmmss").format(noexpireDate);
        String noexpireTs1 = new SimpleDateFormat("yyyyMMddHHmmss").format(noexpireDate1);
        Path basePath = new Path(services.getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path expirePath = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + expireTs);
        Path noexpirePath = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + noexpireTs);
        Path noexpirePath1 = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + noexpireTs1);
        fs.mkdirs(expirePath);
        fs.mkdirs(noexpirePath);
        fs.mkdirs(noexpirePath1);
        try {
            services.init();
            assertEquals(3, fs.listStatus(basePath).length);
            assertTrue(fs.exists(noexpirePath));
            assertTrue(fs.exists(noexpirePath1));
            assertTrue(!fs.exists(expirePath));
        }
        finally {
            services.destroy();
        }
    }

    @Test
    public void testPurgeLauncherJar() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        FileSystem fs = getFileSystem();
        // for directory created 8 days back to be deleted
        int expire1 = services.getConf().getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7) + 1;
        // for directory created 6 days back NOT to be deleted
        int noexpire = services.getConf().getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7) - 1;
        // for directory created 5 days back NOT to be deleted
        int noexpire1 = services.getConf().getInt(ShareLibService.LAUNCHERJAR_LIB_RETENTION, 7) - 2;

        Date expireDate = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * expire1));
        Date noexpireDate = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * noexpire));
        Date noexpireDate1 = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * noexpire1));
        String expireTs = new SimpleDateFormat("yyyyMMddHHmmss").format(expireDate);
        String noexpireTs = new SimpleDateFormat("yyyyMMddHHmmss").format(noexpireDate);
        String noexpireTs1 = new SimpleDateFormat("yyyyMMddHHmmss").format(noexpireDate1);
        Path basePath = new Path(services.getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path expirePath = new Path(basePath, ShareLibService.LAUNCHER_PREFIX + expireTs);
        Path noexpirePath = new Path(basePath, ShareLibService.LAUNCHER_PREFIX + noexpireTs);

        Path noexpirePath1 = new Path(basePath, ShareLibService.LAUNCHER_PREFIX + noexpireTs1);

        fs.mkdirs(expirePath);
        fs.mkdirs(noexpirePath);
        fs.mkdirs(noexpirePath1);
        services.init();
        assertEquals(3, fs.listStatus(basePath).length);
        assertTrue(fs.exists(noexpirePath));
        assertTrue(fs.exists(noexpirePath1));
        assertTrue(!fs.exists(expirePath));
        services.destroy();
    }

    @Test
    public void testGetShareLibPath() throws Exception {
        services = new Services();
        setSystemProps();
        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(services.getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + ShareLibService.dateFormat.format(time));
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
        services.destroy();
    }

    @Test
    public void testShareLib() throws Exception {
        services = new Services();
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        FileSystem fs = getFileSystem();

        Date day1 = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 1));
        Date day2 = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 2));
        Date day3 = new Date(System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 3));
        String dir1 = new SimpleDateFormat("yyyyMMddHHmmss").format(day1);
        String dir2 = new SimpleDateFormat("yyyyMMddHHmmss").format(day2);
        String dir3 = new SimpleDateFormat("yyyyMMddHHmmss").format(day3);
        Path basePath = new Path(services.getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path path1 = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + dir1);
        Path path2 = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + dir2);

        Path path3 = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + dir3);
        fs.mkdirs(path1);
        fs.mkdirs(path2);
        fs.mkdirs(path3);
        createFile(path1.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertTrue(shareLibService.getShareLibJars("pig").get(0).getName().endsWith("pig.jar"));
        services.destroy();
    }

    @Test
    public void testShareLibLoadFile() throws Exception {

        services = new Services();
        FileSystem fs = getFileSystem();
        setSystemProps();
        creatTempshareLibKeymap(fs);
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + "/user/test/config.properties");
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertTrue(shareLibService.getShareLibJars("something_new").get(0).getName().endsWith("somethingNew.jar"));
        assertTrue(shareLibService.getShareLibJars("pig").get(0).getName().endsWith("pig.jar"));
        fs.delete(new Path("shareLibPath/"), true);
        services.destroy();
    }

    @Test
    public void testLoadfromDFS() throws Exception {
        services = new Services();
        setSystemProps();
        services.init();
        FileSystem fs = getFileSystem();
        Date time = new Date(System.currentTimeMillis());

        Path basePath = new Path(services.getConf().get(WorkflowAppService.SYSTEM_LIB_PATH));
        Path libpath = new Path(basePath, ShareLibService.SHARED_LIB_PREFIX + ShareLibService.dateFormat.format(time));
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
        URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        assertEquals(2, cacheFiles.length);
        assertTrue(cacheFiles[0].getPath().endsWith("pig-10.jar") || cacheFiles[1].getPath().endsWith("pig-10.jar"));
        services.destroy();

    }

    @Test
    public void testShareLibLoadFileMultipleFile() throws Exception {
        FileSystem fs = getFileSystem();
        services = new Services();
        creatTempshareLibKeymap_multipleFile(fs);
        setSystemProps();
        Configuration conf = services.getConf();
        conf.set(ShareLibService.SHARELIB_MAPPING_FILE, fs.getUri() + "/user/test/config.properties");
        conf.set(ShareLibService.SHIP_LAUNCHER_JAR, "true");
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        assertNull(shareLibService.getShareLibJars("something_new"));
        assertEquals(shareLibService.getShareLibJars("pig").size(), 2);
        fs.delete(new Path("shareLibPath/"), true);
        services.destroy();
    }

    public void createFile(String filename) throws IOException {
        Path path = new Path(filename);
        FSDataOutputStream out = getFileSystem().create(path);
        out.close();
    }

    public void creatTempshareLibKeymap(FileSystem fs) {
        XConfiguration prop = new XConfiguration();

        try {

            String testPath = "shareLibPath/";

            Path basePath = new Path(testPath + Path.SEPARATOR + "testPath");

            Path somethingNew = new Path(testPath + Path.SEPARATOR + "something_new");
            fs.mkdirs(basePath);
            fs.mkdirs(somethingNew);

            createFile(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFile(somethingNew.toString() + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            prop.set(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", "/user/test/" + basePath.toString());
            prop.set(ShareLibService.SHARE_LIB_CONF_PREFIX + ".something_new", "/user/test/" + somethingNew.toString());

            FSDataOutputStream out = fs.create(new Path("/user/test/config.properties"));

            prop.writeXml(out);
            out.close();

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void creatTempshareLibKeymap_multipleFile(FileSystem fs) {

        XConfiguration prop = new XConfiguration();
        try {

            String testPath = "shareLibPath/";

            Path basePath = new Path(testPath + Path.SEPARATOR + "testPath");
            Path somethingNew = new Path(testPath + Path.SEPARATOR + "something_new");
            fs.mkdirs(basePath);
            fs.mkdirs(somethingNew);

            createFile(basePath.toString() + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar");
            createFile(somethingNew.toString() + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            prop.set(ShareLibService.SHARE_LIB_CONF_PREFIX + ".pig", "/user/test/" + basePath.toString()
                    + Path.SEPARATOR + "pig" + Path.SEPARATOR + "pig.jar," + "/user/test/" + somethingNew.toString()
                    + Path.SEPARATOR + "somethingNew" + Path.SEPARATOR + "somethingNew.jar");

            FSDataOutputStream out = fs.create(new Path("/user/test/config.properties"));

            prop.writeXml(out);
            out.close();

        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
