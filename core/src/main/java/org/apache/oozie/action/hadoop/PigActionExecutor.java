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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.JDOMException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;

public class PigActionExecutor extends ScriptLanguageActionExecutor {

    private static final String PIG_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.PigMain";
    private static final String OOZIE_PIG_STATS = "org.apache.oozie.action.hadoop.OoziePigStats";
    static final String PIG_SCRIPT = "oozie.pig.script";
    static final String PIG_PARAMS = "oozie.pig.params";
    static final String PIG_ARGS = "oozie.pig.args";

    public PigActionExecutor() {
        super("pig");
    }

    @Override
    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        try {
            classes.add(Class.forName(PIG_MAIN_CLASS_NAME));
            classes.add(Class.forName(OOZIE_PIG_STATS));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }


    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, PIG_MAIN_CLASS_NAME);
    }

    @Override
    void injectActionCallback(Context context, Configuration launcherConf) {
    }

    @Override
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
        setPigScript(actionConf, pigName, strParams, strArgs);
        return actionConf;
    }

    public static void setPigScript(Configuration conf, String script, String[] params, String[] args) {
        conf.set(PIG_SCRIPT, script);
        MapReduceMain.setStrings(conf, PIG_PARAMS, params);
        MapReduceMain.setStrings(conf, PIG_ARGS, args);
    }


    @Override
    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return false;
    }

    /**
     * Get the stats and external child IDs for a pig job
     *
     * @param actionFs the FileSystem object
     * @param runningJob the runningJob
     * @param action the Workflow action
     * @param context executor context
     *
     */
    @Override
    protected void getActionData(FileSystem actionFs, RunningJob runningJob, WorkflowAction action, Context context) throws HadoopAccessorException, JDOMException, IOException, URISyntaxException{
        super.getActionData(actionFs, runningJob, action, context);
        String stats = getStats(context, actionFs);
        context.setExecutionStats(stats);
        String externalChildIDs = getExternalChildIDs(context, actionFs);
        context.setExternalChildIDs(externalChildIDs);
    }

    private String getStats(Context context, FileSystem actionFs) throws IOException, HadoopAccessorException,
            URISyntaxException {
        Path actionOutput = LauncherMapperHelper.getActionStatsDataPath(context.getActionDir());
        String stats = null;
        if (actionFs.exists(actionOutput)) {
            stats = getDataFromPath(actionOutput, actionFs);

        }
        return stats;
    }

    @Override
    protected void setActionCompletionData(Context context, FileSystem fs) throws HadoopAccessorException, IOException,
            URISyntaxException {
        String data = getExternalChildIDs(context, fs);
        context.setExternalChildIDs(data);
    }

    private String getExternalChildIDs(Context context, FileSystem actionFs) throws IOException,
            HadoopAccessorException, URISyntaxException {
        Path actionOutput = LauncherMapperHelper.getExternalChildIDsDataPath(context.getActionDir());
        String externalIDs = null;
        if (actionFs.exists(actionOutput)) {
            externalIDs = getDataFromPath(actionOutput, actionFs);
            XLog.getLog(getClass()).info(XLog.STD, "Hadoop Jobs launched : [{0}]", externalIDs);
        }
        return externalIDs;
    }

    private static String getDataFromPath(Path actionOutput, FileSystem actionFs) throws IOException{
        BufferedReader reader = null;
        String data = null;
        try {
            InputStream is = actionFs.open(actionOutput);
            reader = new BufferedReader(new InputStreamReader(is));
            data = IOUtils.getReaderAsString(reader, -1);

        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        return data;
    }

    /**
     * Return the sharelib postfix for the action.
     *
     * @return returns <code>pig</code>.
     * @param actionXml
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "pig";
    }

    protected String getScriptName() {
        return XOozieClient.PIG_SCRIPT;
    }

}
