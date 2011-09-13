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

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;

import java.io.IOException;
import java.util.Iterator;

@SuppressWarnings("deprecation")
public class MapperReducerCredentialsForTest implements Mapper, Reducer {
    public static final String COUNTER_GROUP = "cg";
    public static final String COUNTER_OUTPUT_DATA = "cod";

    public static final String TEST_CRED = "testcred";
    String credential = null;

    public static void main(String[] args) {
        System.out.println("hello!");
    }

    public void configure(JobConf jobConf) {
        credential = jobConf.get(TEST_CRED);
    }

    public void close() throws IOException {
    }

    @SuppressWarnings("unchecked")
    public void map(Object key, Object value, OutputCollector collector, Reporter reporter) throws IOException {
        collector.collect(key, value);
        if (credential != null) {
            reporter.incrCounter(COUNTER_GROUP, COUNTER_OUTPUT_DATA, 1);
        }
    }

    @SuppressWarnings("unchecked")
    public void reduce(Object key, Iterator values, OutputCollector collector, Reporter reporter)
            throws IOException {
        while (values.hasNext()) {
            collector.collect(key, values.next());
        }
    }

    public static boolean hasCredentials(RunningJob runningJob) throws IOException {
        boolean output = false;
        Counters counters = runningJob.getCounters();
        if (counters != null) {
            Counters.Group group = counters.getGroup(COUNTER_GROUP);
            if (group != null) {
                output = group.getCounter(COUNTER_OUTPUT_DATA) > 0;
            }
        }
        return output;
    }
}
