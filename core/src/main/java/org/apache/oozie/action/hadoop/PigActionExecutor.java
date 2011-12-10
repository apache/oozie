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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.util.ClassUtils;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.JDOMException;
import org.mortbay.log.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.List;

public class PigActionExecutor extends JavaActionExecutor {

    public PigActionExecutor() {
        super("pig");
    }

    @Override
    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(PigMain.class);
        classes.add(OoziePigStats.class);
        return classes;
    }


    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, PigMain.class.getName());
    }

    @Override
    void injectActionCallback(Context context, Configuration launcherConf) {
    }

    @Override
    protected Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
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
        PigMain.setPigScript(actionConf, pigName, strParams, strArgs);
        return actionConf;
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
        Path actionOutput = LauncherMapper.getActionStatsDataPath(context.getActionDir());
        String stats = null;
        if (actionFs.exists(actionOutput)) {
            stats = getDataFromPath(actionOutput, actionFs);

        }
        return stats;
    }

    private String getExternalChildIDs(Context context, FileSystem actionFs) throws IOException,
            HadoopAccessorException, URISyntaxException {
        Path actionOutput = LauncherMapper.getExternalChildIDsDataPath(context.getActionDir());
        String externalIDs = null;
        if (actionFs.exists(actionOutput)) {
            externalIDs = getDataFromPath(actionOutput, actionFs);
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
     * @param context executor context.
     * @param actionXml the action XML.
     * @return the action sharelib post fix, this implementation returns <code>pig</code>.
     */
    @Override
    protected String getShareLibPostFix(Context context, Element actionXml) {
        return "pig";
    }

}
