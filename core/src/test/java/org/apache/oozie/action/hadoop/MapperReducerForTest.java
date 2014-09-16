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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MapperReducerForTest implements Mapper, Reducer {
    public static final String GROUP = "g";
    public static final String NAME = "c";
    /**
     * If specified in the job conf, the mapper will write out the job.xml file here.
     */
    public static final String JOB_XML_OUTPUT_LOCATION = "oozie.job.xml.output.location";

    public static void main(String[] args) {
        System.out.println("hello!");
    }

    @Override
    public void configure(JobConf jobConf) {
        try {
            String loc = jobConf.get(JOB_XML_OUTPUT_LOCATION);
            if (loc != null) {
                Path p = new Path(loc);
                FileSystem fs = p.getFileSystem(jobConf);
                if (!fs.exists(p)) {
                    FSDataOutputStream out = fs.create(p);
                    try {
                        jobConf.writeXml(out);
                    } finally {
                        out.close();
                    }
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void close() throws IOException {
    }

    @SuppressWarnings("unchecked")
    public void map(Object key, Object value, OutputCollector collector, Reporter reporter) throws IOException {
        collector.collect(key, value);
        reporter.incrCounter(GROUP, NAME, 5l);
    }

    @SuppressWarnings("unchecked")
    public void reduce(Object key, Iterator values, OutputCollector collector, Reporter reporter)
            throws IOException {
        while (values.hasNext()) {
            collector.collect(key, values.next());
        }
    }
}
