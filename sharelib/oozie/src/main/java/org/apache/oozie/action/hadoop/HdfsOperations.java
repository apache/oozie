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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

public class HdfsOperations {
    private final SequenceFileWriterFactory seqFileWriterFactory;
    private final UserGroupInformation ugi;

    public HdfsOperations(SequenceFileWriterFactory seqFileWriterFactory, UserGroupInformation ugi) {
        this.seqFileWriterFactory = Preconditions.checkNotNull(seqFileWriterFactory, "seqFileWriterFactory should not be null");
        this.ugi = Preconditions.checkNotNull(ugi, "ugi should not be null");
    }

    /**
     * Creates a Sequence file which contains the output from an action and uploads it to HDFS.
     */
    public void uploadActionDataToHDFS(final Configuration launcherJobConf, final Path actionDir, final Map<String, String> actionData) throws IOException {
        IOException ioe = ugi.doAs(new PrivilegedAction<IOException>() {
            @Override
            public IOException run() {
                Path finalPath = new Path(actionDir, LauncherAM.ACTION_DATA_SEQUENCE_FILE);
                // upload into sequence file
                System.out.println("Oozie Launcher, uploading action data to HDFS sequence file: "
                        + new Path(actionDir, LauncherAM.ACTION_DATA_SEQUENCE_FILE).toUri());

                SequenceFile.Writer wr = null;
                try {
                    wr = seqFileWriterFactory.createSequenceFileWriter(launcherJobConf, finalPath, Text.class, Text.class);

                    if (wr != null) {
                        Set<String> keys = actionData.keySet();
                        for (String propsKey : keys) {
                            wr.append(new Text(propsKey), new Text(actionData.get(propsKey)));
                        }
                    } else {
                        throw new IOException("SequenceFile.Writer is null for " + finalPath);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return e;
                } finally {
                    if (wr != null) {
                        try {
                            wr.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            return e;
                        }
                    }
                }

                return null;
            }
        });

        if (ioe != null) {
            throw ioe;
        }
    }
}
