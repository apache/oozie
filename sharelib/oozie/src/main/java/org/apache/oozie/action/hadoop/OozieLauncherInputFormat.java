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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
/**
 * Dummy input format implementation of Oozie launcher jobs. It returns only one record.
 */
public class OozieLauncherInputFormat implements InputFormat<Object, Object> {

    boolean isReadingDone = false;

    public RecordReader<Object, Object> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2)
            throws IOException {
        return new RecordReader<Object, Object>() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public float getProgress() throws IOException {
                if (isReadingDone) {
                    return 1.0f;
                }
                else
                    return 0.0f;
            }

            @Override
            public Object createKey() {
                return new ObjectWritable();
            }

            @Override
            public Object createValue() {
                return new ObjectWritable();
            }

            @Override
            public long getPos() throws IOException {
                if (isReadingDone) {
                    return 1;
                }
                else {
                    return 0;
                }
            }

            @Override
            public boolean next(Object arg0, Object arg1) throws IOException {
                if (isReadingDone) {
                    return false;
                }
                else {
                    isReadingDone = true;
                    return true;
                }
            }

        };
    }

    @Override
    public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
        return new InputSplit[] { new EmptySplit() };
    }

    /**
     * Empty Split implementation.
     */
    public static class EmptySplit implements InputSplit {

        @Override
        public void write(DataOutput out) throws IOException {
        }

        @Override
        public void readFields(DataInput in) throws IOException {
        }

        @Override
        public long getLength() {
            return 0L;
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }
    }

}
