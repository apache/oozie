/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import com.facebook.fb303.FacebookBase;
import org.antlr.runtime.ANTLRFileStream;
import org.apache.commons.collections.Bag;
import org.apache.commons.lang.text.StrMatcher;
import org.apache.derby.iapi.services.io.DerbyIOException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.contrib.serde2.RegexSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.hive.shims.Hadoop20Shims;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.thrift.TBase;
import org.datanucleus.FetchGroup;
import org.datanucleus.store.rdbms.RDBMSStoreManager;
import org.json.JSONML;
import org.objectweb.asm.ByteVector;

import javax.jdo.Query;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TestHiveMain extends MainTestCase {
    private SecurityManager SECURITY_MANAGER;

    protected void setUp() throws Exception {
        super.setUp();
        SECURITY_MANAGER = System.getSecurityManager();
    }

    protected void tearDown() throws Exception {
        System.setSecurityManager(SECURITY_MANAGER);
        super.tearDown();
    }

    private static final String NEW_LINE =
        System.getProperty("line.separator", "\n");

    private String getHiveScript(String inputPath, String outputPath) {
        StringBuilder buffer = new StringBuilder(NEW_LINE);
        buffer.append("set -v;").append(NEW_LINE);
        buffer.append("CREATE EXTERNAL TABLE test (a INT) STORED AS");
        buffer.append(NEW_LINE).append("TEXTFILE LOCATION '");
        buffer.append(inputPath).append("';").append(NEW_LINE);
        buffer.append("INSERT OVERWRITE DIRECTORY '");
        buffer.append(outputPath).append("'").append(NEW_LINE);
        buffer.append("SELECT (a-1) FROM test;").append(NEW_LINE);

        return buffer.toString();
    }

    private String copyJar(String targetFile, Class<?> anyContainedClass)
            throws Exception {
        String file = ClassUtils.findContainingJar(anyContainedClass);
        System.out.println("[copy-jar] class: " + anyContainedClass
                + ", local jar ==> " + file);
        Path targetPath = new Path(getFsTestCaseDir(), targetFile);
        FileSystem fs = getFileSystem();
        InputStream is = new FileInputStream(file);
        OutputStream os = fs.create(targetPath);
        IOUtils.copyStream(is, os);
        return targetPath.toString();
    }

    public Void call() throws Exception {
        if (System.getenv("HADOOP_HOME") == null) {
            System.out.println("WARNING: 'HADOOP_HOME' env var not defined, TestHiveMain test is not running");
        }
        else {
            FileSystem fs = getFileSystem();

            List<String> jars = new ArrayList<String>();
            jars.add(copyJar("lib/libfb303-x.jar", FacebookBase.class));
            jars.add(copyJar("lib/libthrift-x.jar", TBase.class));
            jars.add(copyJar("lib/libjson-x.jar", JSONML.class));
            jars.add(copyJar("lib/jdo2-api-x.jar", Query.class));
            jars.add(copyJar("lib/antlr-runtime-x.jar", ANTLRFileStream.class));
            jars.add(copyJar("lib/asm-x.jar", ByteVector.class));
            jars.add(copyJar("lib/commons-collection-x.jar", Bag.class));
            jars.add(copyJar("lib/commons-lang-x.jar", StrMatcher.class));
            jars.add(copyJar("lib/datanucleus-core-x.jar", FetchGroup.class));
            jars.add(copyJar("lib/datanucleus-rdbms-x.jar", RDBMSStoreManager.class));
            jars.add(copyJar("lib/jdo-x.jar", javax.jdo.metadata.TypeMetadata.class));
            jars.add(copyJar("lib/derby.jar", DerbyIOException.class));
            jars.add(copyJar("lib/jline.jar", jline.FileNameCompletor.class));

            jars.add(copyJar("lib/hive-cli-x.jar", CliSessionState.class));
            jars.add(copyJar("lib/hive-common-x.jar", HiveConf.class));
            jars.add(copyJar("lib/hive-exec-x.jar", ExecDriver.class));
            jars.add(copyJar("lib/hive-metastore-x.jar", HiveMetaStore.class));
            jars.add(copyJar("lib/hive-serde-x.jar", SerDe.class));
            jars.add(copyJar("lib/hive-service-x.jar", HiveServer.class));
            jars.add(copyJar("lib/hive-shims-x.jar", Hadoop20Shims.class));
            jars.add(copyJar("lib/hive-contrib-x.jar", RegexSerDe.class));

            Path inputDir = new Path(getFsTestCaseDir(), "input");
            fs.mkdirs(inputDir);
            Writer writer = new OutputStreamWriter(fs.create(new Path(inputDir, "data.txt")));
            writer.write("3\n4\n6\n1\n2\n7\n9\n0\n8\n");
            writer.close();

            Path outputDir = new Path(getFsTestCaseDir(), "output");

            Path script = new Path(getTestCaseDir(), "script.q");
            Writer w = new FileWriter(script.toString());
            w.write(getHiveScript("${IN}", "${OUT}"));
            w.close();

            XConfiguration jobConf = new XConfiguration();

            jobConf.set("user.name", getTestUser());
            jobConf.set("group.name", getTestGroup());
            jobConf.setInt("mapred.map.tasks", 1);
            jobConf.setInt("mapred.map.max.attempts", 1);
            jobConf.setInt("mapred.reduce.max.attempts", 1);
            jobConf.set("mapred.job.tracker", getJobTrackerUri());
            jobConf.set("fs.default.name", getNameNodeUri());
            jobConf.set("javax.jdo.option.ConnectionURL", "jdbc:hsqldb:mem:hive-main;create=true");
            jobConf.set("javax.jdo.option.ConnectionDriverName", "org.hsqldb.jdbcDriver");
            jobConf.set("javax.jdo.option.ConnectionUserName", "sa");
            injectKerberosInfo(jobConf);

            for (String jar : jars) {
                DistributedCache.addFileToClassPath(new Path(new Path(jar).toUri().getPath()),
                                                    getFileSystem().getConf());
            }

            HiveMain.setHiveScript(jobConf, script.toString(), new String[]{"IN=" + inputDir.toUri().getPath(),
                    "OUT=" + outputDir.toUri().getPath()});

            File actionXml = new File(getTestCaseDir(), "action.xml");
            OutputStream os = new FileOutputStream(actionXml);
            jobConf.writeXml(os);
            os.close();

            //needed in the testcase classpath
            URL url = Thread.currentThread().getContextClassLoader().getResource("PigMain.txt");
            File classPathDir = new File(url.getPath()).getParentFile();
            assertTrue(classPathDir.exists());
            File hiveSite = new File(classPathDir, "hive-site.xml");

            InputStream is = IOUtils.getResourceAsStream("user-hive-default.xml", -1);
            os = new FileOutputStream(new File(classPathDir, "hive-default.xml"));
            IOUtils.copyStream(is, os);

            File outputDataFile = new File(getTestCaseDir(), "outputdata.properties");

            setSystemProperty("oozie.launcher.job.id", "" + System.currentTimeMillis());
            setSystemProperty("oozie.action.conf.xml", actionXml.getAbsolutePath());
            setSystemProperty("oozie.action.output.properties", outputDataFile.getAbsolutePath());

            new LauncherSecurityManager();
            String user = System.getProperty("user.name");
            try {
                os = new FileOutputStream(hiveSite);
                jobConf.writeXml(os);
                os.close();
                HiveMain.main(null);
            }
            catch (SecurityException ex) {
                if (LauncherSecurityManager.getExitInvoked()) {
                    System.out.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                    System.err.println("Intercepting System.exit(" + LauncherSecurityManager.getExitCode() + ")");
                    if (LauncherSecurityManager.getExitCode() != 0) {
                        fail();
                    }
                }
                else {
                    throw ex;
                }
            }
            finally {
                System.setProperty("user.name", user);
                hiveSite.delete();
            }

            assertTrue(outputDataFile.exists());

//TODO: I cannot figure out why when log file is not created in this testcase, it works when running in Launcher
//            Properties props = new Properties();
//            props.load(new FileReader(outputDataFile));
//            assertTrue(props.containsKey("hadoopJobs"));
//            assertTrue(props.getProperty("hadoopJobs").trim().length() > 0);
        }
        return null;
    }

}
