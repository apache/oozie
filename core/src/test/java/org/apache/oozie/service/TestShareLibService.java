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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.action.hadoop.PigActionExecutor;
import org.apache.oozie.action.hadoop.ActionExecutorTestCase.Context;
import org.apache.oozie.action.hadoop.TestJavaActionExecutor;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.junit.Test;

public class TestShareLibService extends XFsTestCase {

    Services services;
    private static String testCaseDirPath;

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
            if (JavaActionExecutor.getCommonLauncherClasses().contains(clazz) ){
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
        services.init();
        ShareLibService shareLibService = Services.get().get(ShareLibService.class);
        List<Path> launcherPath = shareLibService.getActionSystemLibCommonJars(JavaActionExecutor.OOZIE_COMMON_LIBDIR);
        assertNotNull(launcherPath);
        assertTrue(getFileSystem().exists(launcherPath.get(0)));
        List<Path> pigLauncherPath = shareLibService.getActionSystemLibCommonJars("pig");
        assertTrue(getFileSystem().exists(pigLauncherPath.get(0)));
        services.destroy();
    }

    @Test
    public void testAddShareLibDistributedCache() throws Exception {
        services = new Services();
        setSystemProps();
        services.init();
        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" +
                "</java>";
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
        assertEquals(2, cacheFiles.length);
        assertTrue(cacheFilesStr.contains(MyPig.class.getName() + ".jar"));
        assertTrue(cacheFilesStr.contains(MyOozie.class.getName() + ".jar"));
        services.destroy();

    }

    @Test
    public void testPurgeShareLib() throws Exception {
        services = new Services();
        setSystemProps();
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
        Path expirePath = new Path(basePath, "tmp-" + expireTs);
        Path noexpirePath = new Path(basePath, "tmp-" + noexpireTs);
        Path noexpirePath1 = new Path(basePath, "tmp-" + noexpireTs1);
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

}
