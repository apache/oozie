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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.jdom.Namespace;
import org.jdom.Element;

public class SubmitSqoopXCommand extends SubmitHttpXCommand {
    public SubmitSqoopXCommand(Configuration conf) {
        super("submitSqoop", "submitSqoop", conf);
    }

    protected String getOptions(){
        return XOozieClient.SQOOP_OPTIONS;
    }

    @Override
    protected Namespace getSectionNamespace(){
        return Namespace.getNamespace("uri:oozie:sqoop-action:0.4");
    }

    @Override
    protected String getWorkflowName(){
        return "sqoop";
    }

    @Override
    protected Element generateSection(Configuration conf, Namespace ns) {
        String name = "sqoop";
        Element ele = new Element(name, ns);
        Element jt = new Element("job-tracker", ns);
        jt.addContent(conf.get(XOozieClient.JT));
        ele.addContent(jt);
        Element nn = new Element("name-node", ns);
        nn.addContent(conf.get(XOozieClient.NN));
        ele.addContent(nn);

        List<String> Dargs = new ArrayList<String>();
        String[] args = MapReduceMain.getStrings(conf, getOptions());
        for (String arg : args) {
            if (arg.startsWith("-D")) {
                Dargs.add(arg);
            }
        }

        // configuration section
        if (Dargs.size() > 0) {
            Element configuration = generateConfigurationSection(Dargs, ns);
            ele.addContent(configuration);
        }

        String[] sqoopArgs = conf.get(XOozieClient.SQOOP_COMMAND).split("\n");
        for (String arg : sqoopArgs) {
            Element eArg = new Element("arg", ns);
            eArg.addContent(arg);
            ele.addContent(eArg);
        }

        // file section
        addFileSection(ele, conf, ns);

        // archive section
        addArchiveSection(ele, conf, ns);

        return ele;
    }

    @Override
    protected void verifyPrecondition() throws CommandException {
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() {
    }

    @Override
    public String getEntityKey() {
        return null;
    }
}
