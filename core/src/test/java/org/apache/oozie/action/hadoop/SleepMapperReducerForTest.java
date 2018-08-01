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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class SleepMapperReducerForTest implements Mapper<Object, Object, Object, Object>, Reducer<Object, Object, Object, Object> {
    private long sleepTimeMillis = 20_000l;
    public static final String SLEEP_TIME_MILLIS_KEY = "oozie.test.sleep.time.millis";

    public static void main(String[] args) {
        System.out.println("hello!");
    }

    public void close() throws IOException {
    }

    @Override
    public void map(Object key, Object value, OutputCollector<Object, Object> collector, Reporter reporter) throws IOException {
        sleepUninterrupted(sleepTimeMillis, "Mapper sleeping for " + sleepTimeMillis + " millis.", "Mapper woke up");
    }

    @Override
    public void reduce(Object key, Iterator<Object> values, OutputCollector<Object, Object> collector, Reporter reporter)
            throws IOException {
        sleepUninterrupted(sleepTimeMillis, "Reducer sleeping for " + sleepTimeMillis + " millis.", "Reducer woke up");
    }

    @Override
    public void configure(JobConf jobConf) {
        sleepTimeMillis = jobConf.getLong(SLEEP_TIME_MILLIS_KEY, sleepTimeMillis);
        System.out.println("Configuring MR to sleep for" + sleepTimeMillis + " millis.");

    }

    private void sleepUninterrupted(long millis, String preSleepMessage, String postSleepMessage) {
        try {
            System.out.println(preSleepMessage);
            Thread.sleep(millis);
            System.out.println(postSleepMessage);
        } catch (InterruptedException e) {
        }
    }
}

