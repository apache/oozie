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
package org.apache.oozie.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.action.hadoop.OozieActionConfigurator;
import org.apache.oozie.action.hadoop.OozieActionConfiguratorException;

public class SampleOozieActionConfigurator implements OozieActionConfigurator {

    @Override
    public void configure(JobConf actionConf) throws OozieActionConfiguratorException {
        if (actionConf.getUser() == null) {
            throw new OozieActionConfiguratorException("No user set");
        }
        if (actionConf.get("examples.root") == null) {
            throw new OozieActionConfiguratorException("examples.root not set");
        }
        if (actionConf.get("output.dir.name") == null) {
            throw new OozieActionConfiguratorException("output.dir.name not set");
        }

        actionConf.setMapperClass(SampleMapper.class);
        actionConf.setReducerClass(SampleReducer.class);
        actionConf.setNumMapTasks(1);
        FileInputFormat.setInputPaths(actionConf,
                new Path("/user/" + actionConf.getUser() + "/" + actionConf.get("examples.root") + "/input-data/text"));
        FileOutputFormat.setOutputPath(actionConf,
                new Path("/user/" + actionConf.getUser() + "/" + actionConf.get("examples.root") + "/output-data/"
                        + actionConf.get("output.dir.name")));
    }
}
