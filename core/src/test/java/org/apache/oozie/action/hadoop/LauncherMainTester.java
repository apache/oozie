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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.io.FileUtils.ONE_KB;
import static org.apache.commons.io.FileUtils.ONE_MB;

public class LauncherMainTester {

    public static final String JOB_ID_FILE_NAME = "jobID.txt";

    public static void main(String[] args) throws Throwable {
        if (args.length == 0) {
            System.out.println("Hello World!");
        }

        String testJavaOpts = System.getProperty("testJavaOpts");
        if (testJavaOpts != null && Boolean.parseBoolean(testJavaOpts)) {
            throw new RuntimeException("Failing on purpose");
        }
        String env = System.getenv("LAUNCHER_ENVIRON");
        if (env != null && env.equals("foo1")) {
            throw new RuntimeException("Failing on purpose");
        }

        final  String firstArgument = args.length > 0 ? args[0] : null;
        if (args.length == 1) {
            if ("throwable".equals(firstArgument)) {
                throw new Throwable("throwing throwable");
            }
            if ("exception".equals(firstArgument)) {
                throw new IOException("throwing exception");
            }
            if ("exit0".equals(firstArgument)) {
                System.exit(0);
            }
            if ("exit1".equals(firstArgument)) {
                System.exit(1);
            }
            if ("out".equals(firstArgument)) {
                File file = new File(System.getProperty("oozie.action.output.properties"));
                Properties props = new Properties();
                props.setProperty("a", "A");
                OutputStream os = new FileOutputStream(file);
                props.store(os, "");
                os.close();
                System.out.println(file.getAbsolutePath());
            }
            if ("id".equals(firstArgument)) {
                File file = new File(System.getProperty("oozie.action.newId"));
                Properties props = new Properties();
                props.setProperty("id", "IDSWAP");
                OutputStream os = new FileOutputStream(file);
                props.store(os, "");
                os.close();
                System.out.println(file.getAbsolutePath());
            }
            if ("securityManager".equals(firstArgument)) {
                SecurityManager sm = System.getSecurityManager();
                if (sm == null) {
                    throw new Throwable("no security manager");
                }
                // by using NULL as permission, if an underlaying SecurityManager is in place
                // a security exception will be thrown. As there is not underlaying SecurityManager
                // this tests that the delegation logic of the LauncherAMUtils SecurityManager is
                // correct for both checkPermission() signatures.
                sm.checkPermission(null);
                sm.checkPermission(null, sm.getSecurityContext());
            }
            if (firstArgument.startsWith("-Xmx")) {
                tryAllocate(firstArgument);
            }
        }
        if(args.length == 3) {
            if ("javamapreduce".equals(firstArgument)) {
                executeJavaMapReduce(args);
            }
        }
        checkAndSleep(args);
    }

    private static void executeJavaMapReduce(String[] args) throws IOException, InterruptedException {
        JobConf jConf = createSleepMapperReducerJobConf();
        final Path input = new Path(args[1]);
        FileInputFormat.setInputPaths(jConf, input);
        FileOutputFormat.setOutputPath(jConf, new Path(args[2]));
        writeToFile(input, jConf, "dummy\n", "data.txt");
        JobClient jc = new JobClient(jConf);
        System.out.println("Submitting MR job");
        RunningJob job = jc.submitJob(jConf);
        System.out.println("Submitted job " + job.getID().toString());
        writeToFile(input, jConf, job.getID().toString(), JOB_ID_FILE_NAME);
        job.waitForCompletion();
        jc.monitorAndPrintJob(jConf, job);
        if (job.getJobState() != JobStatus.SUCCEEDED) {
            System.err.println(job.getJobState() + " job state instead of" + JobStatus.SUCCEEDED);
            System.exit(-1);
        }
    }

    private static JobConf createSleepMapperReducerJobConf() {
        JobConf jConf = new JobConf(true);
        jConf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
        jConf.setMapperClass(SleepMapperReducerForTest.class);
        jConf.setReducerClass(SleepMapperReducerForTest.class);
        jConf.setOutputKeyClass(Text.class);
        jConf.setOutputValueClass(IntWritable.class);
        jConf.setInputFormat(TextInputFormat.class);
        jConf.setOutputFormat(TextOutputFormat.class);
        jConf.setNumReduceTasks(1);
        jConf.set(SleepMapperReducerForTest.SLEEP_TIME_MILLIS_KEY, "60000");
        return jConf;
    }

    private static void writeToFile(Path input, JobConf jConf, String content, String fileName) throws IOException {
        try (FileSystem fs = FileSystem.get(jConf);
              Writer w = new OutputStreamWriter(fs.create(new Path(input, fileName)))) {
            w.write(content);
        }
        System.out.println("Job Id written to file");
    }

    private static void tryAllocate(final String xmxParameter) {
        Preconditions.checkArgument(JavaActionExecutor.HEAP_MODIFIERS_PATTERN.matcher(xmxParameter).matches(),
                String.format("malformed heap modifier pattern [%s]", xmxParameter));

        final String xmxParameterKey = "-Xmx";
        final String configuredHeapMaxUnitAndUOM =
                xmxParameter.substring(xmxParameter.indexOf(xmxParameterKey) + xmxParameterKey.length());
        final long configuredHeapMaxMb = new BytesAndUOMConverter().toMegabytes(
                configuredHeapMaxUnitAndUOM);

        System.out.println(String.format("Trying to allocate in total [%s] megabytes", configuredHeapMaxMb));

        final List<ByteBuffer> megabytes = new ArrayList<>();
        for (int ixMB = 0; ixMB < configuredHeapMaxMb; ixMB++) {
            megabytes.add(ByteBuffer.allocate((int) ONE_MB));
            if (ixMB % (ONE_KB / 8) == 0) {
                System.out.println(String.format("Allocated [%s] megabytes", ixMB));
            }
        }

        System.out.println(String.format("All [%s] megabytes allocated successfully", configuredHeapMaxMb));
    }

    private static void checkAndSleep(String args[]) throws InterruptedException {
        if (args.length == 2 && args[0].equals("sleep")) {
            long sleepTime = Long.parseLong(args[1]);

            Thread.sleep(sleepTime);
        }
    }

}
