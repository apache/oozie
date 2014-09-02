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
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Callable;

import org.antlr.runtime.ANTLRReaderStream;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.test.XFsTestCase;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.python.util.jython;

import com.google.common.primitives.Booleans;

public abstract class PigTestCase extends XFsTestCase implements Callable<Void> {
    protected static String pigScript;
    protected static boolean writeStats;
    protected static boolean failOnException = true;
    private static String commonPigScript = "set job.name 'test'\n" +
             "set debug on\n" +
             "A = load '$IN' using PigStorage(':');\n" +
             "B = foreach A generate $0 as id;\n" +
             "store B into '$OUT' USING PigStorage();";



    public void testPigScript() throws Exception {
        pigScript = commonPigScript;
        writeStats = true;
        failOnException = true;
        MainTestCase.execute(getTestUser(), this);
        String hadoopIdsFile = System.getProperty("oozie.action.externalChildIDs");
        assertTrue(new File(hadoopIdsFile).exists());
        String externalChildIds = IOUtils.getReaderAsString(new FileReader(hadoopIdsFile), -1);
        assertTrue(externalChildIds.contains("job_"));

    }
    // testing embedded Pig feature of Pig 0.9
    public void testEmbeddedPigWithinPython() throws Exception {
        failOnException = true;
        FileSystem fs = getFileSystem();
        Path jythonJar = new Path(getFsTestCaseDir(), "jython.jar");
        InputStream is = new FileInputStream(ClassUtils.findContainingJar(jython.class));
        OutputStream os = fs.create(jythonJar);
        IOUtils.copyStream(is, os);

        Path antlrRuntimeJar = new Path(getFsTestCaseDir(), "antlr_runtime.jar");
        is = new FileInputStream(ClassUtils.findContainingJar(ANTLRReaderStream.class));
        os = fs.create(antlrRuntimeJar);
        IOUtils.copyStream(is, os);

        Path guavaJar = new Path(getFsTestCaseDir(), "guava.jar");
        is = new FileInputStream(ClassUtils.findContainingJar(Booleans.class));
        os = fs.create(guavaJar);
        IOUtils.copyStream(is, os);

        DistributedCache.addFileToClassPath(new Path(jythonJar.toUri().getPath()), getFileSystem().getConf());
        DistributedCache.addFileToClassPath(new Path(antlrRuntimeJar.toUri().getPath()), getFileSystem().getConf());
        DistributedCache.addFileToClassPath(new Path(guavaJar.toUri().getPath()), getFileSystem().getConf());

        Path inputDir = new Path(getFsTestCaseDir(), "input");
        Path outputDir = new Path(getFsTestCaseDir(), "output");
        pigScript = "#!/usr/bin/python"
                + "\nfrom org.apache.pig.scripting import Pig"
                + "\nP = Pig.compile(\"\"\"" + "\n" + commonPigScript + "\n"+ "\"\"\")"
                + "\ninput = " + "'"+inputDir.toUri().getPath()+"'"
                + "\noutput = " + "'"+outputDir.toUri().getPath()+"'"
                + "\nQ = P.bind({'IN':input, 'OUT':output})"
                + "\nQ.runSingle()";

        writeStats = false;
        MainTestCase.execute(getTestUser(), this);
    }

    public void testPig_withNullExternalID() throws Exception {
        failOnException = false;
        String script = "A = load '$IN' using PigStorage(':');\n"
                + "store A into '$IN' USING PigStorage();";
        pigScript = script;
        writeStats = true;
        try {
            MainTestCase.execute(getTestUser(), this);
        }
        catch (Exception e) {
            //Ignore exception
        }
        String hadoopIdsFile = System.getProperty("oozie.action.externalChildIDs");
        assertFalse(new File(hadoopIdsFile).exists());

    }


}
