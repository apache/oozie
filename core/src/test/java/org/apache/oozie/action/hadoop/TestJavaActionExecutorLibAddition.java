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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.ShareLibService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


import static org.apache.oozie.action.hadoop.JavaActionExecutor.ACTION_SHARELIB_FOR;
import static org.apache.oozie.action.hadoop.JavaActionExecutor.SHARELIB_EXCLUDE_SUFFIX;

public class TestJavaActionExecutorLibAddition extends ActionExecutorTestCase {

    private static final String[] TEST_SHARELIB_OOZIE_FILES = {
            "jackson-core-2.3.jar",
            "jackson-databind-2.3.jar",
            "other-lib.jar",
            "oozie-library.jar",
    };

    private static final String[] TEST_SHARELIB_JAVA_FILES = {
            "jackson-core-2.6.5.jar",
            "jackson-databind-2.6.5.jar",
            "some-lib.jar",
            "another-lib.jar",
    };

    private static final String[] TEST_SHARELIB_PIG_FILES = {
            "jackson-pig-0.3.3.jar",
            "jackson-datapig-0.3.5.jar",
            "pig_data.txt",
    };

    private static final String[] TEST_SHARELIB_USER_FILES = {
            "jackson-user-3.3.jar",
            "jackson-workflow-app.jar",
            "user-job-utils.jar",
            "jackson-files.zip"
    };

    private static final String[] TEST_SHARELIB_FILES = {
            "soFile.so",
            "soFile.so.1",
            "file",
            "jar.jar"
    };

    private static final String[] TEST_SHARELIB_ROOT_FILES = {
            "rootSoFile.so",
            "rootSoFile.so.1",
            "rootFile",
            "rootJar.jar"
    };

    private static final String[] TEST_SHARELIB_ARCHIVES = {
            "archive.tar",
            "rootArchive.tar",
    };

    @Override
    protected void setSystemProps() throws Exception {
        super.setSystemProps();
        setHadoopSystemProps();
        createActionConfDirFiles();
    }

    private void createActionConfDirFiles() throws IOException {
        new File(getTestCaseConfDir(), "action-conf").mkdir();
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("test-action-config.xml");
        OutputStream os = new FileOutputStream(new File(getTestCaseConfDir() + "/action-conf", "java.xml"));
        IOUtils.copyStream(is, os);
    }

    private void setHadoopSystemProps() {
        setSystemProperty("oozie.service.ActionService.executor.classes", JavaActionExecutor.class.getName());
        setSystemProperty("oozie.service.HadoopAccessorService.action.configurations",
                "*=hadoop-conf," + getJobTrackerUri() + "=action-conf");
        setSystemProperty(WorkflowAppService.SYSTEM_LIB_PATH, getFsTestCaseDir().toUri().getPath() + "/systemlib");
    }

    private Map<String, Path> setupTestShareLibExcludeTestJars(final Path systemLibPath) throws Exception {
        final Path oozieShareLibPath = new Path(systemLibPath, "oozie");
        final Path javaShareLibPath = new Path(systemLibPath, "java");
        final Path pigShareLibPath = new Path(systemLibPath, new Path("pig", "lib"));
        final Path actionLibPath = new Path(getAppPath(), "lib");
        makeDirs(oozieShareLibPath, javaShareLibPath, pigShareLibPath, actionLibPath);

        final Map<String, Path> libs = new LinkedHashMap<>();
        loadLibPathsToMap(libs, oozieShareLibPath, TEST_SHARELIB_OOZIE_FILES);
        loadLibPathsToMap(libs, javaShareLibPath, TEST_SHARELIB_JAVA_FILES);
        loadLibPathsToMap(libs, pigShareLibPath, TEST_SHARELIB_PIG_FILES);
        loadLibPathsToMap(libs, actionLibPath, TEST_SHARELIB_USER_FILES);
        createFiles(libs.values());

        return libs;
    }

    private void loadLibPathsToMap(Map<String, Path> libMap, Path root, String... files) {
        for (String f : files) {
            libMap.put(f, new Path(root, f));
        }
    }

    private void checkLibExclude(final String... libsToBeExcluded) throws Exception {
        final int LIBS_ADDED_BY_TESTS = 6;
        final Map<String, Path> libs = setupTestShareLibExcludeTestJars(getNewSystemLibPath());
        final Path actionLibPath = new Path(getAppPath(), "lib");
        final Context context = createContextUsingSharelib(actionLibPath);

        createWorkflowJobUsingSharelib(context);
        setSharelibForActionInConfiguration("java,pig");

        final Configuration jobConf = createActionExecutorAndSetupServices(context);
        final URI[] cacheFiles = DistributedCache.getCacheFiles(jobConf);
        final String cacheFilesStr = getDistributedCacheFilesStr(jobConf);

        for (final String lib : libsToBeExcluded) {
            assertFalse(lib + " should have been excluded from distributed cache",
                    cacheFilesStr.contains(libs.get(lib).toString()));
        }
        assertEquals("The number of files on distributed cache is not what expected.",
                libs.size() - libsToBeExcluded.length + LIBS_ADDED_BY_TESTS, cacheFiles.length);
    }

    private Context createContextUsingSharelib(final Path actionLibPath) throws Exception {
        final String actionXml = String.format("<java>" +
                "<job-tracker>%s</job-tracker>" +
                "<name-node>%s</name-node>" +
                "<configuration>" +
                "<property>" +
                "<name>oozie.launcher.oozie.libpath</name>" +
                "<value>%s</value>" +
                "</property>" +
                "</configuration>" +
                "<main-class>MAIN-CLASS</main-class>" +
                "</java>", getJobTrackerUri(), getNameNodeUri(), actionLibPath);

        return createContext(actionXml, null);
    }

    private void createWorkflowJobUsingSharelib(final Context context, final XConfiguration wfConf) {
        final WorkflowJobBean workflow = (WorkflowJobBean) context.getWorkflow();
        wfConf.set(WorkflowAppService.HADOOP_USER, getTestUser());
        wfConf.set(OozieClient.APP_PATH, new Path(getAppPath(), "workflow.xml").toString());
        wfConf.setBoolean(OozieClient.USE_SYSTEM_LIBPATH, true);
        workflow.setConf(XmlUtils.prettyPrint(wfConf).toString());
    }

    private void createWorkflowJobUsingSharelib(final Context context) {
        createWorkflowJobUsingSharelib(context, new XConfiguration());
    }

    private Configuration createActionExecutorAndSetupServices(final Context context) throws Exception {
        Services.get().setService(ShareLibService.class);
        final Element eActionXml = XmlUtils.parseXml(context.getAction().getConf());
        final JavaActionExecutor ae = new JavaActionExecutor();
        final Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        Services.get().get(ShareLibService.class).updateShareLib();
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);
        return jobConf;
    }

    private Configuration createActionExecutorAndSetLibFilesArchives(final Context context) throws Exception {
        final Element eActionXml = XmlUtils.parseXml(context.getAction().getConf());
        final JavaActionExecutor ae = new JavaActionExecutor();
        final Configuration jobConf = ae.createBaseHadoopConf(context, eActionXml);
        ae.setupLauncherConf(jobConf, eActionXml, getAppPath(), context);
        ae.setLibFilesArchives(context, eActionXml, getAppPath(), jobConf);
        return jobConf;
    }

    private void setExcludePatternInConfiguration(String pattern) {
        ConfigurationService.set(ACTION_SHARELIB_FOR + "java" + SHARELIB_EXCLUDE_SUFFIX, pattern);
    }

    private void setSharelibForActionInConfiguration(String libs) {
        ConfigurationService.set(ACTION_SHARELIB_FOR + "java", libs);
    }

    public void testExcludeFilesFromAllSharelibLocation() throws Exception {
        setExcludePatternInConfiguration(".*jackson.*");
        checkLibExclude(TEST_SHARELIB_OOZIE_FILES[0], TEST_SHARELIB_OOZIE_FILES[1],
                TEST_SHARELIB_JAVA_FILES[0], TEST_SHARELIB_JAVA_FILES[1],
                TEST_SHARELIB_PIG_FILES[0], TEST_SHARELIB_PIG_FILES[1],
                TEST_SHARELIB_USER_FILES[0], TEST_SHARELIB_USER_FILES[1], TEST_SHARELIB_USER_FILES[3]);
    }

    public void testExcludeFilesFromAllOozieSharelibFolder() throws Exception {
        setExcludePatternInConfiguration("oozie/jackson.*");
        checkLibExclude(TEST_SHARELIB_OOZIE_FILES[0], TEST_SHARELIB_OOZIE_FILES[1]);
    }

    public void testExcludeFilesFromMultipleLocations() throws Exception {
        setExcludePatternInConfiguration("pig/lib/jackson.*|java/jackson.*|oozie/jackson.*");
        checkLibExclude(TEST_SHARELIB_OOZIE_FILES[0], TEST_SHARELIB_OOZIE_FILES[1],
                TEST_SHARELIB_JAVA_FILES[0], TEST_SHARELIB_JAVA_FILES[1],
                TEST_SHARELIB_PIG_FILES[0], TEST_SHARELIB_PIG_FILES[1]);
    }

    public void testExcludeUserProvidedFiles() throws Exception {
        setExcludePatternInConfiguration(".*/app/lib/jackson.*");
        checkLibExclude(TEST_SHARELIB_USER_FILES[0], TEST_SHARELIB_USER_FILES[1], TEST_SHARELIB_USER_FILES[3]);
    }

    public void testAddActionShareLib() throws Exception {
        final Path systemLibPath = getNewSystemLibPath();
        final Path actionLibPath = new Path(getAppPath(), "lib");

        final Path javaShareLibPath = new Path(systemLibPath, "java");
        final Path jar1Path = new Path(javaShareLibPath, "jar1.jar");
        final Path jar2Path = new Path(javaShareLibPath, "jar2.jar");

        final Path hcatShareLibPath = new Path(systemLibPath, "hcat");
        final Path jar3Path = new Path(hcatShareLibPath, "jar3.jar");
        final Path jar4Path = new Path(hcatShareLibPath, "jar4.jar");

        final Path otherShareLibPath = new Path(systemLibPath, "other");
        final Path jar5Path = new Path(otherShareLibPath, "jar5.jar");

        makeDirs(javaShareLibPath, hcatShareLibPath, otherShareLibPath);
        createFiles(Arrays.asList(jar1Path, jar2Path, jar3Path, jar4Path, jar5Path));

        final Context context = createContextUsingSharelib(actionLibPath);
        createWorkflowJobUsingSharelib(context);
        setSharelibForActionInConfiguration("java,hcat");

        try {
            createActionExecutorAndSetupServices(context);
            fail("Expected ActionExecutorException to be thrown, but got nothing.");
        }
        catch (ActionExecutorException aee) {
            assertEquals("Unexpected error code. Message: " + aee.getMessage(), "EJ001", aee.getErrorCode());
            assertEquals("Unexpected error message","Could not locate Oozie sharelib", aee.getMessage());
        }

        final Path launcherPath = new Path(systemLibPath, "oozie");
        final Path jar6Path = new Path(launcherPath, "jar6.jar");
        makeDirs(launcherPath);
        getFileSystem().create(jar6Path).close();

        Configuration jobConf = createActionExecutorAndSetupServices(context);
        String cacheFilesStr = getDistributedCacheFilesStr(jobConf);

        assertContainsJars( cacheFilesStr, Arrays.asList(jar1Path, jar2Path, jar3Path, jar4Path, jar6Path));
        assertNotContainsJars( cacheFilesStr, Collections.singletonList(jar5Path));

        final XConfiguration wfConf = new XConfiguration();
        wfConf.set(ACTION_SHARELIB_FOR + "java", "other,hcat");
        createWorkflowJobUsingSharelib(context, wfConf);
        setSharelibForActionInConfiguration("java");
        jobConf = createActionExecutorAndSetupServices(context);

        // The oozie server setting should have been overridden by workflow setting
        cacheFilesStr = getDistributedCacheFilesStr(jobConf);
        assertContainsJars(cacheFilesStr, Arrays.asList(jar3Path, jar4Path, jar5Path, jar6Path));
        assertNotContainsJars(cacheFilesStr, Arrays.asList(jar1Path, jar2Path));
    }

    private Path getActionLibPath() throws Exception {
        Path actionLibPath = new Path(getFsTestCaseDir(), "actionlibs");
        makeDirs(actionLibPath);
        return actionLibPath;
    }

    private List<Path> createTestActionLibPaths(Path... paths) throws Exception{
        final Path actionLibPath = new Path(getFsTestCaseDir(), "actionlibs");
        makeDirs(actionLibPath);
        createFiles(Arrays.asList(paths));
        return Arrays.asList(paths);
    }

    public void testAddingActionLibDir() throws Exception{
        makeDirs(getActionLibPath());
        List<Path> expectedJars = createTestActionLibPaths(
                new Path(getActionLibPath(), "jar1.jar"),
                new Path(getActionLibPath(), "jar2.jar"));

        Context context = createContextUsingSharelib(getActionLibPath());
        Configuration jobConf = createActionExecutorAndSetLibFilesArchives(context);
        assertContainsJars(getDistributedCacheFilesStr(jobConf), expectedJars);
    }

    public void testAddingActionLibFile() throws Exception{
        List<Path> expectedJars = createTestActionLibPaths(new Path(getFsTestCaseDir(), "jar3.jar"));

        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" + "<configuration>" +
                "<property><name>oozie.launcher.oozie.libpath</name><value>" + expectedJars.get(0) + "</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        Configuration jobConf = createActionExecutorAndSetLibFilesArchives(context);
        assertContainsJars(getDistributedCacheFilesStr(jobConf), expectedJars);
    }

    public void testActionLibFileAndDir() throws Exception {
        makeDirs(getActionLibPath());
        List<Path> expectedJars = createTestActionLibPaths(
                new Path(getActionLibPath(), "jar1.jar"),
                new Path(getActionLibPath(), "jar2.jar"),
                new Path(getFsTestCaseDir(), "jar3.jar"));

        String actionXml = "<java>" + "<job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "<name-node>" + getNameNodeUri() + "</name-node>" + "<configuration>" +
                "<property><name>oozie.launcher.oozie.libpath</name><value>" + getActionLibPath() + "," + expectedJars.get(2) +
                "</value></property>" +
                "</configuration>" + "<main-class>MAIN-CLASS</main-class>" +
                "</java>";
        Context context = createContext(actionXml, null);
        Configuration jobConf = createActionExecutorAndSetLibFilesArchives(context);
        assertContainsJars(getDistributedCacheFilesStr(jobConf), expectedJars);
    }

    private String getDistributedCacheFilesStr(final Configuration jobConf) throws IOException {
        return Arrays.toString(DistributedCache.getCacheFiles(jobConf));
    }

    private Map<String, Path> createAndGetSharelibTestFiles() throws Exception {
        Path rootPath = new Path(getFsTestCaseDir(), "root");
        Map<String, Path> libs = new LinkedHashMap<>();
        loadLibPathsToMap(libs, getAppPath(), TEST_SHARELIB_FILES);
        loadLibPathsToMap(libs, rootPath, TEST_SHARELIB_ROOT_FILES);
        loadLibPathsToMap(libs, getAppPath(), TEST_SHARELIB_ARCHIVES[0]);
        loadLibPathsToMap(libs, rootPath, TEST_SHARELIB_ARCHIVES[1]);
        createFiles(libs.values());
        return libs;
    }

    public void testLibFileArchives() throws Exception {
        Map<String, Path> libs = createAndGetSharelibTestFiles();

        final String actionXml = "<java>" +
                "      <job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "      <name-node>" + getNameNodeUri() + "</name-node>" +
                "      <main-class>CLASS</main-class>" +
                "      <file>" + libs.get(TEST_SHARELIB_FILES[0]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_FILES[1]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_FILES[2]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_FILES[3]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_ROOT_FILES[0]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_ROOT_FILES[1]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_ROOT_FILES[2]).toString() + "</file>\n" +
                "      <file>" + libs.get(TEST_SHARELIB_ROOT_FILES[3]).toString() + "</file>\n" +
                "      <archive>" + libs.get(TEST_SHARELIB_ARCHIVES[0]).toString() + "</archive>\n" +
                "      <archive>" + libs.get(TEST_SHARELIB_ARCHIVES[1]).toString() + "</archive>\n" +
                "</java>";

        final Configuration jobConf = createActionExecutorAndSetLibFilesArchives(createContext(actionXml, null));
        verifyFilesInDistributedCache(libs, jobConf);
    }

    public void testCommaSeparatedFilesAndArchives() throws Exception {
        Map<String, Path> libs = createAndGetSharelibTestFiles();

        final String actionXml = "<java>" +
                "      <job-tracker>" + getJobTrackerUri() + "</job-tracker>" +
                "      <name-node>" + getNameNodeUri() + "</name-node>" +
                "      <main-class>CLASS</main-class>" +
                "      <file>" + libs.get(TEST_SHARELIB_FILES[3]).toString() +
                "," + libs.get(TEST_SHARELIB_ROOT_FILES[3]).toString() +
                "," + libs.get(TEST_SHARELIB_FILES[2]).toString() +
                ", " + libs.get(TEST_SHARELIB_ROOT_FILES[2]).toString() + // with leading and trailing spaces
                "  ," + libs.get(TEST_SHARELIB_FILES[0]).toString() +
                "," + libs.get(TEST_SHARELIB_ROOT_FILES[0]).toString() +
                "," + libs.get(TEST_SHARELIB_FILES[1]).toString() +
                "," + libs.get(TEST_SHARELIB_ROOT_FILES[1]).toString() + "</file>\n" +
                "      <archive>" + libs.get(TEST_SHARELIB_ARCHIVES[0]).toString() + ", "
                + libs.get(TEST_SHARELIB_ARCHIVES[1]).toString() + " </archive>\n" + // with leading and trailing spaces
                "</java>";

        final Configuration jobConf = createActionExecutorAndSetLibFilesArchives(createContext(actionXml, null));
        verifyFilesInDistributedCache(libs, jobConf);
    }

    private void verifyFilesInDistributedCache(Map<String, Path> libs, final Configuration jobConf) throws Exception {
        assertTrue(DistributedCache.getSymlink(jobConf));

        String filesInClassPathString = Arrays.toString(DistributedCache.getFileClassPaths(jobConf));
        assertContainsJars(filesInClassPathString, Arrays.asList(
                libs.get(TEST_SHARELIB_FILES[3]),
                libs.get(TEST_SHARELIB_ROOT_FILES[3])));

        assertNotContainsJars(filesInClassPathString, Arrays.asList(
                libs.get(TEST_SHARELIB_FILES[0]),
                libs.get(TEST_SHARELIB_FILES[1]),
                libs.get(TEST_SHARELIB_FILES[2]),
                libs.get(TEST_SHARELIB_ROOT_FILES[0]),
                libs.get(TEST_SHARELIB_ROOT_FILES[1]),
                libs.get(TEST_SHARELIB_ROOT_FILES[2])));

        filesInClassPathString = Arrays.toString(DistributedCache.getCacheFiles(jobConf));
        assertContainsJars(filesInClassPathString, Arrays.asList(
                libs.get(TEST_SHARELIB_FILES[0]),
                libs.get(TEST_SHARELIB_FILES[1]),
                libs.get(TEST_SHARELIB_FILES[2]),
                libs.get(TEST_SHARELIB_FILES[3]),
                libs.get(TEST_SHARELIB_ROOT_FILES[0]),
                libs.get(TEST_SHARELIB_ROOT_FILES[1]),
                libs.get(TEST_SHARELIB_ROOT_FILES[2]),
                libs.get(TEST_SHARELIB_ROOT_FILES[3])));

        filesInClassPathString = Arrays.toString(DistributedCache.getCacheArchives(jobConf));
        assertContainsJars(filesInClassPathString,  Arrays.asList(
                libs.get(TEST_SHARELIB_ARCHIVES[0]),
                libs.get(TEST_SHARELIB_ARCHIVES[1])));
    }
}
