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
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.ArrayList;
import java.util.List;

public abstract class SubmitScriptLanguageXCommand extends SubmitHttpXCommand {
    public SubmitScriptLanguageXCommand(String name, String type, Configuration conf) {
        super(name, type, conf);
    }

    protected abstract String getLanguageName();

    protected abstract String getOptions();

    protected abstract String getScriptParamters();

    protected Namespace getSectionNamespace() {
        return Namespace.getNamespace("uri:oozie:workflow:0.2");
    }

    private Element generateSection(Configuration conf, Namespace ns) {
        String name = getLanguageName();
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
        root.setAttribute("name", "oozie-" + getLanguageName());

        Element start = new Element("start", ns);
        String name = getLanguageName();
        String nodeName = name + "1";
        start.setAttribute("to", nodeName);
        root.addContent(start);

        Element action = new Element("action", ns);
        action.setAttribute("name", nodeName);

        Element ele = generateSection(conf, getSectionNamespace());
        action.addContent(ele);

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
        message.addContent(name + " failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
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
