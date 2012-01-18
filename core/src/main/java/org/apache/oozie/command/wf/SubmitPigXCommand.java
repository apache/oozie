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
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;
import org.apache.oozie.action.hadoop.MapReduceMain;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.command.CommandException;

import java.util.ArrayList;
import java.util.List;

public class SubmitPigXCommand extends SubmitHttpXCommand {
    public SubmitPigXCommand(Configuration conf, String authToken) {
        super("submitPig", "submitPig", conf, authToken);
    }

    private Element generatePigSection(Configuration conf, Namespace ns) {
        Element pig = new Element("pig", ns);
        Element jt = new Element("job-tracker", ns);
        jt.addContent(conf.get(XOozieClient.JT));
        pig.addContent(jt);
        Element nn = new Element("name-node", ns);
        nn.addContent(conf.get(XOozieClient.NN));
        pig.addContent(nn);

        List<String> Dargs = new ArrayList<String>();
        List<String> otherArgs = new ArrayList<String>();
        String[] pigArgs = MapReduceMain.getStrings(conf, XOozieClient.PIG_OPTIONS);
        for (String arg : pigArgs) {
            if (arg.startsWith("-D")) {
                Dargs.add(arg);
            }
            else {
                otherArgs.add(arg);
            }
        }

        // configuration section
        if (Dargs.size() > 0) {
            Element configuration = generateConfigurationSection(Dargs, ns);
            pig.addContent(configuration);
        }

        Element script = new Element("script", ns);
        script.addContent("dummy.pig");
        pig.addContent(script);

        // argument section
        for (String arg : otherArgs) {
            Element argument = new Element("argument", ns);
            argument.addContent(arg);
            pig.addContent(argument);
        }

        // file section
        addFileSection(pig, conf, ns);

        // archive section
        addArchiveSection(pig, conf, ns);

        return pig;
    }

    private Element generateConfigurationSection(List<String> Dargs, Namespace ns) {
        Element configuration = new Element("configuration", ns);
        for (String arg : Dargs) {
            String name = null, value = null;
            int pos = arg.indexOf("=");
            if (pos == -1) { // "-D<name>" or "-D" only
                name = arg.substring(2, arg.length());
                value = "";
            }
            else { // "-D<name>=<value>"
                name = arg.substring(2, pos);
                value = arg.substring(pos + 1, arg.length());
            }

            Element property = new Element("property", ns);
            Element nameElement = new Element("name", ns);
            nameElement.addContent(name);
            property.addContent(nameElement);
            Element valueElement = new Element("value", ns);
            valueElement.addContent(value);
            property.addContent(valueElement);
            configuration.addContent(property);
        }

        return configuration;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.oozie.command.wf.SubmitHttpCommand#getWorkflowXml(org.apache
     * .hadoop.conf.Configuration)
     */
    @Override
    protected String getWorkflowXml(Configuration conf) {
        for (String key : MANDATORY_OOZIE_CONFS) {
            String value = conf.get(key);
            if (value == null) {
                throw new RuntimeException(key + " is not specified");
            }
        }

        Namespace ns = Namespace.getNamespace("uri:oozie:workflow:0.2");
        Element root = new Element("workflow-app", ns);
        root.setAttribute("name", "oozie-pig");

        Element start = new Element("start", ns);
        start.setAttribute("to", "pig1");
        root.addContent(start);

        Element action = new Element("action", ns);
        action.setAttribute("name", "pig1");

        Element pig = generatePigSection(conf, ns);
        action.addContent(pig);

        Element ok = new Element("ok", ns);
        ok.setAttribute("to", "end");
        action.addContent(ok);

        Element error = new Element("error", ns);
        error.setAttribute("to", "fail");
        action.addContent(error);

        root.addContent(action);

        Element kill = new Element("kill", ns);
        kill.setAttribute("name", "fail");
        Element message = new Element("message", ns);
        message.addContent("Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
        kill.addContent(message);
        root.addContent(kill);

        Element end = new Element("end", ns);
        end.setAttribute("name", "end");
        root.addContent(end);

        return XmlUtils.prettyPrint(root).toString();
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
