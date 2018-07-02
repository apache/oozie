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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

public class SqoopActionExecutor extends JavaActionExecutor {

  public static final String OOZIE_ACTION_EXTERNAL_STATS_WRITE = "oozie.action.external.stats.write";
  private static final String SQOOP_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.SqoopMain";
  static final String SQOOP_ARGS = "oozie.sqoop.args";
  private static final String SQOOP = "sqoop";

    public SqoopActionExecutor() {
        super(SQOOP);
    }

    @Override
    public List<Class<?>> getLauncherClasses() {
        List<Class<?>> classes = new ArrayList<>();
        try {
            classes.add(Class.forName(SQOOP_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS, SQOOP_MAIN_CLASS_NAME);
    }

    @Override
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();

        try {
            Element e = actionXml.getChild("configuration", ns);
            if (e != null) {
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                XConfiguration.copy(inlineConf, actionConf);
                checkForDisallowedProps(inlineConf, "inline configuration");
            }
        } catch (IOException ex) {
            throw convertException(ex);
        }

        final List<String> argList = new ArrayList<>();
        // Build a list of arguments from either a tokenized <command> string or a list of <arg>
        if (actionXml.getChild("command", ns) != null) {
            String command = actionXml.getChild("command", ns).getTextTrim();
            StringTokenizer st = new StringTokenizer(command, " ");
            while (st.hasMoreTokens()) {
                argList.add(st.nextToken());
            }
        }
        else {
            @SuppressWarnings("unchecked")
            List<Element> eArgs = (List<Element>) actionXml.getChildren("arg", ns);
            for (Element elem : eArgs) {
                argList.add(elem.getTextTrim());
            }
        }
        // If the command is given accidentally as "sqoop import --option"
        // instead of "import --option" we can make a user's life easier
        // by removing away the unnecessary "sqoop" token.
        // However, we do not do this if the command looks like
        // "sqoop --option", as that's entirely invalid.
        if (argList.size() > 1 &&
                argList.get(0).equalsIgnoreCase(SQOOP) &&
                !argList.get(1).startsWith("-")) {
            XLog.getLog(getClass()).info(
                    "Found a redundant 'sqoop' prefixing the command. Removing it.");
            argList.remove(0);
        }

        setSqoopCommand(actionConf, argList.toArray(new String[argList.size()]));
        return actionConf;
    }

    private void setSqoopCommand(Configuration conf, String[] args) {
        ActionUtils.setStrings(conf, SQOOP_ARGS, args);
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
                Configuration jobConf = createBaseHadoopConf(context, actionXml);
                jobClient = createJobClient(context, jobConf);

                // Cumulative counters for all Sqoop mapreduce jobs
                Counters counters = null;

                // Sqoop do not have to create mapreduce job each time
                String externalIds = action.getExternalChildIDs();
                if (externalIds != null && !externalIds.trim().isEmpty()) {
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
                            && (statsJsonString.getBytes(Charsets.UTF_8).length <= getMaxExternalStatsSize())) {
                        context.setExecutionStats(statsJsonString);
                        LOG.debug(
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
                        LOG.error("JobClient error: ", e);
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
