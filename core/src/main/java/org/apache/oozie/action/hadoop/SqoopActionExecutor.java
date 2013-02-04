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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.XLog;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class SqoopActionExecutor extends JavaActionExecutor {

  public static final String OOZIE_ACTION_EXTERNAL_STATS_WRITE = "oozie.action.external.stats.write";

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

    /**
     * We will gather counters from all executed action Hadoop jobs (e.g. jobs
     * that moved data, not the launcher itself) and merge them together. There
     * will be only one job most of the time. The only exception is
     * import-all-table option that will execute one job per one exported table.
     *
     * @param context Action context
     * @param action Workflow action
     * @throws ActionExecutorException
     */
    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        super.end(context, action);
        JobClient jobClient = null;

        boolean exception = false;
        try {
            if (action.getStatus() == WorkflowAction.Status.OK) {
                Element actionXml = XmlUtils.parseXml(action.getConf());
                JobConf jobConf = createBaseHadoopConf(context, actionXml);
                jobClient = createJobClient(context, jobConf);

                // Cumulative counters for all Sqoop mapreduce jobs
                Counters counters = null;

                String externalIds = action.getExternalChildIDs();
                String []jobIds = externalIds.split(",");

                for(String jobId : jobIds) {
                    RunningJob runningJob = jobClient.getJob(JobID.forName(jobId));
                    if (runningJob == null) {
                      throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "SQOOP001",
                        "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", action
                        .getExternalId(), action.getId());
                    }

                    Counters taskCounters = runningJob.getCounters();
                    if(taskCounters != null) {
                        if(counters == null) {
                          counters = taskCounters;
                        } else {
                          counters.incrAllCounters(taskCounters);
                        }
                    } else {
                      XLog.getLog(getClass()).warn("Could not find Hadoop Counters for job: [{0}]", jobId);
                    }
                }

                if (counters != null) {
                    ActionStats stats = new MRStats(counters);
                    String statsJsonString = stats.toJSON();
                    context.setVar(MapReduceActionExecutor.HADOOP_COUNTERS, statsJsonString);

                    // If action stats write property is set to false by user or
                    // size of stats is greater than the maximum allowed size,
                    // do not store the action stats
                    if (Boolean.parseBoolean(evaluateConfigurationProperty(actionXml,
                            OOZIE_ACTION_EXTERNAL_STATS_WRITE, "true"))
                            && (statsJsonString.getBytes().length <= getMaxExternalStatsSize())) {
                        context.setExecutionStats(statsJsonString);
                        log.debug(
                          "Printing stats for sqoop action as a JSON string : [{0}]", statsJsonString);
                    }
                } else {
                    context.setVar(MapReduceActionExecutor.HADOOP_COUNTERS, "");
                    XLog.getLog(getClass()).warn("Can't find any associated Hadoop job counters");
                }
            }
        }
        catch (Exception ex) {
            exception = true;
            throw convertException(ex);
        }
        finally {
            if (jobClient != null) {
                try {
                    jobClient.close();
                }
                catch (Exception e) {
                    if (exception) {
                        log.error("JobClient error: ", e);
                    }
                    else {
                        throw convertException(e);
                    }
                }
            }
        }
    }

    // Return the value of the specified configuration property
    private String evaluateConfigurationProperty(Element actionConf, String key, String defaultValue)
            throws ActionExecutorException {
        try {
            if (actionConf != null) {
                Namespace ns = actionConf.getNamespace();
                Element e = actionConf.getChild("configuration", ns);

                if(e != null) {
                  String strConf = XmlUtils.prettyPrint(e).toString();
                  XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                  return inlineConf.get(key, defaultValue);
                }
            }
            return defaultValue;
        }
        catch (IOException ex) {
            throw convertException(ex);
        }
    }

    /**
     * Get the stats and external child IDs
     *
     * @param actionFs the FileSystem object
     * @param runningJob the runningJob
     * @param action the Workflow action
     * @param context executor context
     *
     */
    @Override
    protected void getActionData(FileSystem actionFs, RunningJob runningJob, WorkflowAction action, Context context)
            throws HadoopAccessorException, JDOMException, IOException, URISyntaxException{
        super.getActionData(actionFs, runningJob, action, context);

        // Load stored Hadoop jobs ids and promote them as external child ids
        action.getData();
        Properties props = new Properties();
        props.load(new StringReader(action.getData()));
        context.setExternalChildIDs((String)props.get(LauncherMain.HADOOP_JOBS));
    }

    @Override
    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return true;
    }


    /**
     * Return the sharelib name for the action.
     *
     * @return returns <code>sqoop</code>.
     * @param actionXml
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "sqoop";
    }

}
