/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.action.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class SqoopActionExecutor extends JavaActionExecutor {


    public SqoopActionExecutor() {
        super("sqoop");
    }

    @Override
    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(HiveMain.class);
        classes.add(SqoopMain.class);
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, SqoopMain.class.getName());
    }

    @Override
    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);

        HiveActionExecutor hiveAE = new HiveActionExecutor();
        hiveAE.setupHiveDefault(conf, appPath, actionXml);

        return conf;
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();

        try {
            Element e = actionXml.getChild("configuration", ns);
            if (e != null) {
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                checkForDisallowedProps(inlineConf, "inline configuration");
                XConfiguration.copy(inlineConf, actionConf);
            }
        } catch (IOException ex) {
            throw convertException(ex);
        }

        String[] args;
        if (actionXml.getChild("command", ns) != null) {
            String command = actionXml.getChild("command", ns).getTextTrim();
            StringTokenizer st = new StringTokenizer(command, " ");
            List<String> l = new ArrayList<String>();
            while (st.hasMoreTokens()) {
                l.add(st.nextToken());
            }
            args = l.toArray(new String[l.size()]);
        }
        else {
            List<Element> eArgs = (List<Element>) actionXml.getChildren("arg", ns);
            args = new String[eArgs.size()];
            for (int i = 0; i < eArgs.size(); i++) {
                args[i] = eArgs.get(i).getTextTrim();
            }
        }

        SqoopMain.setSqoopCommand(actionConf, args);
        return actionConf;
    }

    @Override
    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return true;
    }


    /**
     * Return the sharelib postfix for the action.
     *
     * @param context executor context.
     * @param actionXml the action XML.
     * @return the action sharelib post fix, this implementation returns <code>hive</code>.
     */
    protected String getShareLibPostFix(Context context, Element actionXml) {
        return "sqoop";
    }

}
