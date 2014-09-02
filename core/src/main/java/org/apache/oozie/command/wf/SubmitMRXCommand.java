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

package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.WorkflowAppService;
import org.jdom.Element;
import org.jdom.Namespace;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.command.CommandException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SubmitMRXCommand extends SubmitHttpXCommand {
    private static final Set<String> SKIPPED_CONFS = new HashSet<String>();
    private static final Map<String, String> DEPRECATE_MAP = new HashMap<String, String>();

    public SubmitMRXCommand(Configuration conf) {
        super("submitMR", "submitMR", conf);
    }

    static {
        SKIPPED_CONFS.add(WorkflowAppService.HADOOP_USER);
        SKIPPED_CONFS.add(XOozieClient.JT);
        SKIPPED_CONFS.add(XOozieClient.NN);
        // a brillant mind made a change in Configuration that 'fs.default.name' key gets converted to 'fs.defaultFS'
        // in Hadoop 0.23, we need skip that one too, keeping the old one because of Hadoop 1
        SKIPPED_CONFS.add(XOozieClient.NN_2);

        DEPRECATE_MAP.put(XOozieClient.NN, XOozieClient.NN_2);
        DEPRECATE_MAP.put(XOozieClient.JT, XOozieClient.JT_2);
        DEPRECATE_MAP.put(WorkflowAppService.HADOOP_USER, "mapreduce.job.user.name");
    }

    @Override
    protected Namespace getSectionNamespace(){
        return Namespace.getNamespace("uri:oozie:workflow:0.2");
    }

    @Override
    protected String getWorkflowName(){
        return "mapreduce";
    }

    private Element generateConfigurationSection(Configuration conf, Namespace ns) {
        Element configuration = null;
        Iterator<Map.Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String name = entry.getKey();
            if (MANDATORY_OOZIE_CONFS.contains(name) || OPTIONAL_OOZIE_CONFS.contains(name)
                    || SKIPPED_CONFS.contains(name)
                    || DEPRECATE_MAP.containsValue(name)) {
                continue;
            }

            if (configuration == null) {
                configuration = new Element("configuration", ns);
            }

            String value = entry.getValue();
            Element property = new Element("property", ns);
            Element nameElement = new Element("name", ns);
            nameElement.addContent(name != null ? name : "");
            property.addContent(nameElement);
            Element valueElement = new Element("value", ns);
            valueElement.addContent(value != null ? value : "");
            property.addContent(valueElement);
            configuration.addContent(property);
        }

        return configuration;
    }

    @Override
    protected Element generateSection(Configuration conf, Namespace ns) {
        Element mapreduce = new Element("map-reduce", ns);
        Element jt = new Element("job-tracker", ns);
        String newJTVal = conf.get(DEPRECATE_MAP.get(XOozieClient.JT));
        jt.addContent(newJTVal != null ? newJTVal : (conf.get(XOozieClient.JT)));
        mapreduce.addContent(jt);
        Element nn = new Element("name-node", ns);
        String newNNVal = conf.get(DEPRECATE_MAP.get(XOozieClient.NN));
        nn.addContent(newNNVal != null ? newNNVal : (conf.get(XOozieClient.NN)));
        mapreduce.addContent(nn);

        if (conf.size() > MANDATORY_OOZIE_CONFS.size()) { // excluding JT, NN,
                                                          // LIBPATH
            // configuration section
            Element configuration = generateConfigurationSection(conf, ns);
            if (configuration != null) {
                mapreduce.addContent(configuration);
            }

            // file section
            addFileSection(mapreduce, conf, ns);

            // archive section
            addArchiveSection(mapreduce, conf, ns);
        }

        return mapreduce;
    }

    @Override
    protected void checkMandatoryConf(Configuration conf) {
        for (String key : MANDATORY_OOZIE_CONFS) {
            String value = conf.get(key);
            if(value == null) {
                if(DEPRECATE_MAP.containsKey(key)) {
                    if(conf.get(DEPRECATE_MAP.get(key)) == null) {
                        throw new RuntimeException(key + " or " + DEPRECATE_MAP.get(key) + " is not specified");
                    }
                }
                else {
                    throw new RuntimeException(key + " is not specified");
                }
            }
        }
    }

    @Override
    public String getEntityKey() {
        return null;
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() {

    }

    @Override
    protected void verifyPrecondition() throws CommandException {

    }
}
