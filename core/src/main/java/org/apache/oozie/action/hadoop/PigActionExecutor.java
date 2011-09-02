/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.JDOMException;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.List;

public class PigActionExecutor extends JavaActionExecutor {

    public PigActionExecutor() {
        super("pig");
    }

    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(PigMain.class);
        return classes;
    }

    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, PigMain.class.getName());
    }

    void injectActionCallback(Context context, Configuration launcherConf) {
    }

    @Override
    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);
        Namespace ns = actionXml.getNamespace();
        String script = actionXml.getChild("script", ns).getTextTrim();
        String pigName = new Path(script).getName();
        String pigScriptContent = context.getProtoActionConf().get(XOozieClient.PIG_SCRIPT);

        Path pigScriptFile = null;
        if (pigScriptContent != null) { // Create pig script on hdfs if this is
            // an http submission pig job;
            FSDataOutputStream dos = null;
            try {
                Path actionPath = context.getActionDir();
                pigScriptFile = new Path(actionPath, script);
                FileSystem fs = context.getAppFileSystem();
                dos = fs.create(pigScriptFile);
                dos.writeBytes(pigScriptContent);

                addToCache(conf, actionPath, script + "#" + pigName, false);
            }
            catch (Exception ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_OPERATION", XLog
                        .format("Not able to write pig script file {0} on hdfs", pigScriptFile), ex);
            }
            finally {
                try {
                    if (dos != null) {
                        dos.close();
                    }
                }
                catch (IOException ex) {
                    XLog.getLog(getClass()).error("Error: " + ex.getMessage());
                }
            }
        }
        else {
            addToCache(conf, appPath, script + "#" + pigName, false);
        }

        return conf;
    }

    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();

        String script = actionXml.getChild("script", ns).getTextTrim();
        String pigName = new Path(script).getName();

        List<Element> params = (List<Element>) actionXml.getChildren("param", ns);
        String[] strParams = new String[params.size()];
        for (int i = 0; i < params.size(); i++) {
            strParams[i] = params.get(i).getTextTrim();
        }
        String[] strArgs = null;
        List<Element> eArgs = actionXml.getChildren("argument", ns);
        if (eArgs != null && eArgs.size() > 0) {
            strArgs = new String[eArgs.size()];
            for (int i = 0; i < eArgs.size(); i++) {
                strArgs[i] = eArgs.get(i).getTextTrim();
            }
        }
        PigMain.setPigScript(actionConf, pigName, strParams, strArgs);
        return actionConf;
    }

    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return true;
    }

}
