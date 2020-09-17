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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;


public class TestSparkMain extends MainTestCase {

    private static final String INPUT = "input.txt";
    private static final String OUTPUT = "output";

    @Override
    public Void call() throws Exception {
        XConfiguration jobConf = new XConfiguration();
        XConfiguration.copy(createJobConf(), jobConf);

        FileSystem fs = getFileSystem();
        Path file = new Path(getFsTestCaseDir(), "input.txt");
        Writer scriptWriter = new OutputStreamWriter(fs.create(file), StandardCharsets.UTF_8);
        scriptWriter.write("1,2,3");
        scriptWriter.write("\n");
        scriptWriter.write("2,3,4");
        scriptWriter.close();

        jobConf.set(JavaMain.JAVA_MAIN_CLASS, "org.apache.spark.deploy.SparkSubmit");

        jobConf.set("mapreduce.job.tags", "" + System.currentTimeMillis());
        setSystemProperty("oozie.job.launch.time", "" + System.currentTimeMillis());
        File statsDataFile = new File(getTestCaseDir(), "statsdata.properties");
        File hadoopIdsFile = new File(getTestCaseDir(), "hadoopIds");
        File outputDataFile = new File(getTestCaseDir(), "outputdata.properties");

        jobConf.set(SparkActionExecutor.SPARK_MASTER, "local[*]");
        jobConf.set(SparkActionExecutor.SPARK_MODE, "client");
        jobConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        jobConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        jobConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-memory  1042M " +
                "--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"");

        jobConf.set(SparkActionExecutor.SPARK_JAR, getFsTestCaseDir() + "/lib/test.jar");


        File actionXml = new File(getTestCaseDir(), "action.xml");
        OutputStream os = new FileOutputStream(actionXml);
        jobConf.writeXml(os);
        os.close();

        System.setProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
        setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
        setSystemProperty("oozie.action.stats.properties", statsDataFile.getAbsolutePath());
        setSystemProperty("oozie.action.externalChildIDs", hadoopIdsFile.getAbsolutePath());
        setSystemProperty("oozie.action.output.properties", outputDataFile.getAbsolutePath());

        File jarFile = IOUtils.createJar(new File(getTestCaseDir()), "test.jar", LauncherMainTester.class);
        InputStream is = new FileInputStream(jarFile);
        os = getFileSystem().create(new Path(getFsTestCaseDir(), "lib/test.jar"));
        IOUtils.copyStream(is, os);

        String input  = getFsTestCaseDir() + "/" + INPUT;
        String output = getFsTestCaseDir() + "/" + OUTPUT;
        String[] args = {input, output};
        SparkMain.main(args);
        assertTrue(getFileSystem().exists(new Path(getFsTestCaseDir() + "/" + OUTPUT)));
        return null;
    }

    public void testPatterns() {
        patternHelper("spark-yarn", SparkMain.SPARK_YARN_JAR_PATTERN);
        patternHelper("spark-assembly", SparkMain.SPARK_ASSEMBLY_JAR_PATTERN);
    }

    private void patternHelper(String jarName, Pattern pattern) {
        ArrayList<String> jarList = new ArrayList<String>();
        jarList.add(jarName + "-1.2.jar");
        jarList.add(jarName + "-1.2.4.jar");
        jarList.add(jarName + "1.2.4.jar");
        jarList.add(jarName + "-1.2.4_1.2.3.4.jar");
        jarList.add(jarName + ".jar");

        // all should pass
        for (String s : jarList) {
            assertTrue(pattern.matcher(s).find());
        }

        jarList.clear();
        jarList.add(jarName + "-1.2.3-sources.jar");
        jarList.add(jarName + "-sources-1.2.3.jar");
        jarList.add(jarName + "-sources.jar");
        // all should not pass
        for (String s : jarList) {
            assertFalse(pattern.matcher(s).find());
        }
    }

    public void testJobIDPattern() {
        List<String> lines = new ArrayList<String>();
        lines.add("Submitted application application_001");
        // Non-matching ones
        lines.add("Submitted application job_002");
        lines.add("HadoopJobId: application_003");
        lines.add("Submitted application = application_004");
        Set<String> jobIds = new LinkedHashSet<String>();
        for (String line : lines) {
            LauncherMain.extractJobIDs(line, SparkMain.SPARK_JOB_IDS_PATTERNS, jobIds);
        }
        Set<String> expected = new LinkedHashSet<String>();
        expected.add("job_001");
        assertEquals(expected, jobIds);
    }

    @Override
    protected List<File> getFilesToDelete() {
        List<File> filesToDelete = super.getFilesToDelete();
        filesToDelete.add(new File(SparkMain.HIVE_SITE_CONF));
        return filesToDelete;
    }

    public void testFixFsDefaultUrisAndFilterDuplicates() throws URISyntaxException, IOException {
        URI[] uris = new URI[2];
        URI uri1 = new URI("/foo/bar.keytab#foo.bar");
        URI uri2 = new URI("/foo/bar.keytab#bar.foo");

        uris[0] = uri1;
        uris[1] = uri2;

        Map<String, URI> result1 = SparkMain.fixFsDefaultUrisAndFilterDuplicates(uris);
        assertEquals("Duplication elimination was not successful. " +
                "Reason: Keytab added twice, but the result map contained more or less.",
                1, result1.size());
    }

    public void testFixFsDefaultUrisAndFilterDuplicatesNoDuplication() throws URISyntaxException, IOException {
        URI[] uris = new URI[2];
        URI uri1 = new URI("/bar/foo.keytab#foo.bar");
        URI uri2 = new URI("/foo/bar.keytab#bar.foo");

        uris[0] = uri1;
        uris[1] = uri2;

        Map<String, URI> result = SparkMain.fixFsDefaultUrisAndFilterDuplicates(uris);
        assertEquals("Duplication elimination was not successful. " +
                "Reason: Two different keytabs were added, but the result map contained more or less.",
                2,  result.size());
    }

    public void testFixFsDefaultUrisAndFilterDuplicatesWithKeytabSymNotToAdd() throws URISyntaxException, IOException {
        URI[] uris = new URI[2];
        URI uri1 = new URI("/bar/foo.keytab#foo.bar");
        URI uri2 = new URI("/foo/bar.keytab#bar.foo");

        uris[0] = uri1;
        uris[1] = uri2;

        Map<String, URI> result = SparkMain.fixFsDefaultUrisAndFilterDuplicates(uris, "foo.bar");
        assertEquals("Duplication elimination was not successful. " +
                "Reason: foo.bar exists in the URI array, but the deletion did not happen.",
                1, result.size());
    }

    public void testFixFsDefaultUrisAndFilterDuplicatesWithKeytabNameNotToAdd() throws URISyntaxException, IOException {
        URI[] uris = new URI[2];

        URI uri1 = new URI("/bar/foo.keytab#foo.bar");
        URI uri2 = new URI("/foo/bar.keytab#bar.foo");

        uris[0] = uri1;
        uris[1] = uri2;

        Map<String, URI> result = SparkMain.fixFsDefaultUrisAndFilterDuplicates(uris, "foo.keytab");
        assertEquals("Duplication elimination was not successful. " +
                "Reason: foo.keytab exists in the URI array, but the deletion did not happen.",
                1, result.size());
    }
}