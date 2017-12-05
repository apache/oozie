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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

public class HdfsOperations {
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private final SequenceFileWriterFactory seqFileWriterFactory;

    public HdfsOperations(SequenceFileWriterFactory seqFileWriterFactory) {
        this.seqFileWriterFactory = Preconditions.checkNotNull(seqFileWriterFactory, "seqFileWriterFactory should not be null");
    }

    /**
     * Creates a Sequence file which contains the output from an action and uploads it to HDFS.
     * @param launcherJobConf the configuration
     * @param actionDir the target directory on HDFS
     * @param actionData the data to upload
     * @throws IOException in case of IO error
     * @throws InterruptedException in case of interruption
     */
    public void uploadActionDataToHDFS(final Configuration launcherJobConf, final Path actionDir,
                                       final Map<String, String> actionData) throws IOException, InterruptedException {
        Path finalPath = new Path(actionDir, LauncherAM.ACTION_DATA_SEQUENCE_FILE);
        // upload into sequence file
        System.out.println("Oozie Launcher, uploading action data to HDFS sequence file: " + finalPath.toUri());

        try (SequenceFile.Writer wr =
                     seqFileWriterFactory.createSequenceFileWriter(launcherJobConf, finalPath, Text.class, Text.class)) {

            if (wr != null) {
                for (Entry<String, String> entry : actionData.entrySet()) {
                    wr.append(new Text(entry.getKey()), new Text(entry.getValue()));
                }
            } else {
                throw new IOException("SequenceFile.Writer is null for " + finalPath);
            }
        }
    }

    public boolean fileExists(final Path path, final Configuration launcherJobConf) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(path.toUri(), launcherJobConf);
        return fs.exists(path);
    }

    public void writeStringToFile(final Path path, final Configuration conf, final String contents)
            throws IOException, InterruptedException {
        try (FileSystem fs = FileSystem.get(path.toUri(), conf);
             java.io.Writer writer = new OutputStreamWriter(fs.create(path), DEFAULT_CHARSET)) {
            writer.write(contents);
        }
    }

    public String readFileContents(final Path path, final Configuration conf) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();

        try (FileSystem fs = FileSystem.get(path.toUri(), conf);
             InputStream is = fs.open(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, DEFAULT_CHARSET))) {

            String contents;
            while ((contents = reader.readLine()) != null) {
                sb.append(contents);
            }
        }

        return sb.toString();
    }
}
