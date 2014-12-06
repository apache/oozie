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

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import org.apache.hadoop.io.Text;

/**
 * This is just like MapperReducerForTest except that this map function outputs the classpath as the value
 */
public class MapperReducerUberJarForTest implements Mapper, Reducer {
    public static final String GROUP = "g";
    public static final String NAME = "c";

    public static void main(String[] args) {
        System.out.println("hello!");
    }

    public void configure(JobConf jobConf) {
    }

    public void close() throws IOException {
    }

    @SuppressWarnings("unchecked")
    public void map(Object key, Object value, OutputCollector collector, Reporter reporter) throws IOException {
        StringBuilder sb = new StringBuilder();
        ClassLoader applicationClassLoader = this.getClass().getClassLoader();
        if (applicationClassLoader == null) {
            applicationClassLoader = ClassLoader.getSystemClassLoader();
        }
        URL[] urls = ((URLClassLoader) applicationClassLoader).getURLs();
        for (URL url : urls) {
            sb.append(url.toString()).append("@");
        }
        collector.collect(key, new Text(sb.toString()));
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
