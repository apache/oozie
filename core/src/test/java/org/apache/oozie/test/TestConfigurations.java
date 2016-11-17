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

package org.apache.oozie.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.HCatURIHandler;
import org.apache.oozie.service.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

class TestConfigurations {

    Configuration createYarnConfig(final Configuration parentConfig) {
        final Configuration yarnConfig = new YarnConfiguration(parentConfig);

        yarnConfig.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
        yarnConfig.set(YarnConfiguration.RM_WEBAPP_ADDRESS, "localhost:0");

        return yarnConfig;
    }

    @SuppressWarnings("deprecation")
    JobConf createJobConfFromYarnCluster(final Configuration yarnConfiguration) {
        final JobConf jobConf = new JobConf();
        final JobConf jobConfYarn = new JobConf(yarnConfiguration);

        for (final Map.Entry<String, String> entry : jobConfYarn) {
            // MiniMRClientClusterFactory sets the job jar in Hadoop 2.0 causing tests to fail
            // TODO call conf.unset after moving completely to Hadoop 2.x
            if (!(entry.getKey().equals("mapreduce.job.jar") || entry.getKey().equals("mapred.jar"))) {
                jobConf.set(entry.getKey(), entry.getValue());
            }
        }

        return jobConf;
    }

    JobConf createPristineJobConf(final String jobTrackerUri, final String nameNodeUri) {
        final JobConf jobConf = new JobConf();

        jobConf.set("mapred.job.tracker", jobTrackerUri);
        jobConf.set("fs.default.name", nameNodeUri);

        return jobConf;
    }

    JobConf createDFSConfig(String oozieUser, String testGroup) throws UnknownHostException {
        final JobConf conf = new JobConf();
        conf.set("dfs.block.access.token.enable", "false");
        conf.set("dfs.permissions", "true");
        conf.set("hadoop.security.authentication", "simple");

        //Doing this because Hadoop 1.x does not support '*' if the value is '*,127.0.0.1'
        final StringBuilder sb = new StringBuilder();
        sb.append("127.0.0.1,localhost");
        for (final InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
            sb.append(",").append(i.getCanonicalHostName());
        }
        conf.set("hadoop.proxyuser." + oozieUser + ".hosts", sb.toString());

        conf.set("hadoop.proxyuser." + oozieUser + ".groups", testGroup);
        conf.set("mapred.tasktracker.map.tasks.maximum", "4");
        conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");

        conf.set("hadoop.tmp.dir", "target/test-data" + "/minicluster");

        // Scheduler properties required for YARN CapacityScheduler to work
        conf.set("yarn.scheduler.capacity.root.queues", "default");
        conf.set("yarn.scheduler.capacity.root.default.capacity", "100");
        // Required to prevent deadlocks with YARN CapacityScheduler
        conf.set("yarn.scheduler.capacity.maximum-am-resource-percent", "0.5");

        return conf;
    }

    void setConfigurationForHCatalog(final Services services) {
        final Configuration conf = services.getConf();

        conf.set(Services.CONF_SERVICE_EXT_CLASSES,
                JMSAccessorService.class.getName() + "," +
                        PartitionDependencyManagerService.class.getName() + "," +
                        HCatAccessorService.class.getName());
        conf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES,
                "default=java.naming.factory.initial#" + XTestCase.ACTIVE_MQ_CONN_FACTORY + ";" +
                        "java.naming.provider.url#" + XTestCase.LOCAL_ACTIVE_MQ_BROKER +
                        "connectionFactoryNames#" + "ConnectionFactory");
        conf.set(URIHandlerService.URI_HANDLERS,
                FSURIHandler.class.getName() + "," + HCatURIHandler.class.getName());
    }
}