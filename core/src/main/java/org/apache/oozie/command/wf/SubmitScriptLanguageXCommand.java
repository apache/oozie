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
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.command.CommandException;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.ArrayList;
import java.util.List;

public abstract class SubmitScriptLanguageXCommand extends SubmitHttpXCommand {
    public SubmitScriptLanguageXCommand(String name, String type, Configuration conf) {
        super(name, type, conf);
    }

    @Override
    protected abstract String getWorkflowName();

    protected abstract String getOptions();

    protected abstract String getScriptParamters();

    @Override
    protected Namespace getSectionNamespace() {
        return Namespace.getNamespace("uri:oozie:workflow:0.2");
    }

    @Override
    protected Element generateSection(Configuration conf, Namespace ns) {
        String name = getWorkflowName();
        Element ele = new Element(name, ns);
        Element jt = new Element("job-tracker", ns);
        jt.addContent(conf.get(XOozieClient.JT));
        ele.addContent(jt);
        Element nn = new Element("name-node", ns);
        nn.addContent(conf.get(XOozieClient.NN));
        ele.addContent(nn);

        List<String> Dargs = new ArrayList<String>();
        List<String> otherArgs = new ArrayList<String>();
        String[] args = MapReduceMain.getStrings(conf, getOptions());
        for (String arg : args) {
            if (arg.startsWith("-D")) {
                Dargs.add(arg);
            }
            else {
                otherArgs.add(arg);
            }
        }
        String [] params = MapReduceMain.getStrings(conf, getScriptParamters());

        // configuration section
        if (Dargs.size() > 0) {
            Element configuration = generateConfigurationSection(Dargs, ns);
            ele.addContent(configuration);
        }

        Element script = new Element("script", ns);
        script.addContent("dummy." + name);
        ele.addContent(script);

        // parameter section
        for (String param : params) {
            Element parameter = new Element("param", ns);
            parameter.addContent(param);
            ele.addContent(parameter);
        }

        // argument section
        for (String arg : otherArgs) {
            Element argument = new Element("argument", ns);
            argument.addContent(arg);
            ele.addContent(argument);
        }

        // file section
        addFileSection(ele, conf, ns);

        // archive section
        addArchiveSection(ele, conf, ns);

        return ele;
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
