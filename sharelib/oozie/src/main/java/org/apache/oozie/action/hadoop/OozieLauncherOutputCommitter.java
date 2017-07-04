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
import java.io.IOException;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class OozieLauncherOutputCommitter extends OutputCommitter {

    public OozieLauncherOutputCommitter() {
        File propConf = new File(LauncherAMUtils.PROPAGATION_CONF_XML);
        if (!propConf.exists()) {
            try {
                propConf.createNewFile();
            }
            catch (IOException e) {
                System.out.println("Failed to create " + LauncherAMUtils.PROPAGATION_CONF_XML);
                e.printStackTrace(System.err);
            }
        }
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
    }

    /**
     * Did this task write any files in the work directory?
     * @param taskContext the task's context
     */
    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
    }

    // If write Override annotation, it will fail on Apache Hadoop versions
    // which does not contain this method.
    @Deprecated
    public boolean isRecoverySupported() {
        return true;
    }

    public boolean isRecoverySupported(JobContext jobContext) throws IOException {
        return isRecoverySupported();
    }
}
