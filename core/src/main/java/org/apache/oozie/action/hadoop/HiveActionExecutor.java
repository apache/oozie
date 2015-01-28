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

import static org.apache.oozie.action.hadoop.LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.service.HadoopAccessorException;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class HiveActionExecutor extends ScriptLanguageActionExecutor {

    private static final String HIVE_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.HiveMain";
    static final String HIVE_SCRIPT = "oozie.hive.script";
    static final String HIVE_PARAMS = "oozie.hive.params";
    static final String HIVE_ARGS = "oozie.hive.args";

    public HiveActionExecutor() {
        super("hive");
    }

    @Override
    public List<Class> getLauncherClasses() {
        List<Class> classes = new ArrayList<Class>();
        try {
            classes.add(Class.forName(HIVE_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
	}
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(CONF_OOZIE_ACTION_MAIN_CLASS, HIVE_MAIN_CLASS_NAME);
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml,
                                  Path appPath) throws ActionExecutorException {
        Configuration conf = super.setupActionConf(actionConf, context, actionXml, appPath);

        Namespace ns = actionXml.getNamespace();
        String script = actionXml.getChild("script", ns).getTextTrim();
        String scriptName = new Path(script).getName();
        String hiveScriptContent = context.getProtoActionConf().get(XOozieClient.HIVE_SCRIPT);

        if (hiveScriptContent == null){
            addToCache(conf, appPath, script + "#" + scriptName, false);
        }

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

        setHiveScript(conf, scriptName, strParams, strArgs);
        return conf;
    }

    public static void setHiveScript(Configuration conf, String script, String[] params, String[] args) {
        conf.set(HIVE_SCRIPT, script);
        MapReduceMain.setStrings(conf, HIVE_PARAMS, params);
        MapReduceMain.setStrings(conf, HIVE_ARGS, args);
    }

    @Override
    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return true;
    }

    @Override
    protected void getActionData(FileSystem actionFs, RunningJob runningJob, WorkflowAction action, Context context)
            throws HadoopAccessorException, JDOMException, IOException, URISyntaxException {
        super.getActionData(actionFs, runningJob, action, context);

        if (action.getData() != null) {
            // Load stored Hadoop jobs ids and promote them as external child
            // ids on job success
            Properties props = new Properties();
            props.load(new StringReader(action.getData()));
            context.setExternalChildIDs((String) props.get(LauncherMain.HADOOP_JOBS));
        }
    }

    /**
     * Return the sharelib name for the action.
     *
     * @return returns <code>hive</code>.
     * @param actionXml
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "hive";
    }

    protected String getScriptName() {
        return XOozieClient.HIVE_SCRIPT;
    }

    @Override
    public String[] getShareLibFilesForActionConf() {
        return new String[]{"hive-site.xml"};
    }

}
