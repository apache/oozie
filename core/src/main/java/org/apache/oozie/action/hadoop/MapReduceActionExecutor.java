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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;
import org.json.simple.JSONObject;

public class MapReduceActionExecutor extends JavaActionExecutor {

    public static final String HADOOP_COUNTERS = "hadoop.counters";
    private XLog log = XLog.getLog(getClass());

    public MapReduceActionExecutor() {
        super("map-reduce");
    }

    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(StreamingMain.class);
        classes.add(PipesMain.class);
        return classes;
    }

    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        String mainClass;
        Namespace ns = actionXml.getNamespace();
        if (actionXml.getChild("streaming", ns) != null) {
            mainClass = launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, StreamingMain.class.getName());
        }
        else {
            if (actionXml.getChild("pipes", ns) != null) {
                mainClass = launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, PipesMain.class.getName());
            }
            else {
                mainClass = launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, MapReduceMain.class.getName());
            }
        }
        return mainClass;
    }

    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath) throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath);
        conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", true);
        return conf;
    }

    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        Namespace ns = actionXml.getNamespace();
        if (actionXml.getChild("streaming", ns) != null) {
            Element streamingXml = actionXml.getChild("streaming", ns);
            String mapper = streamingXml.getChildTextTrim("mapper", ns);
            String reducer = streamingXml.getChildTextTrim("reducer", ns);
            String recordReader = streamingXml.getChildTextTrim("record-reader", ns);
            List<Element> list = (List<Element>) streamingXml.getChildren("record-reader-mapping", ns);
            String[] recordReaderMapping = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                recordReaderMapping[i] = list.get(i).getTextTrim();
            }
            list = (List<Element>) streamingXml.getChildren("env", ns);
            String[] env = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                env[i] = list.get(i).getTextTrim();
            }
            StreamingMain.setStreaming(actionConf, mapper, reducer, recordReader, recordReaderMapping, env);
        }
        else {
            if (actionXml.getChild("pipes", ns) != null) {
                Element pipesXml = actionXml.getChild("pipes", ns);
                String map = pipesXml.getChildTextTrim("map", ns);
                String reduce = pipesXml.getChildTextTrim("reduce", ns);
                String inputFormat = pipesXml.getChildTextTrim("inputformat", ns);
                String partitioner = pipesXml.getChildTextTrim("partitioner", ns);
                String writer = pipesXml.getChildTextTrim("writer", ns);
                String program = pipesXml.getChildTextTrim("program", ns);
                PipesMain.setPipes(actionConf, map, reduce, inputFormat, partitioner, writer, program);
            }
        }
        actionConf = super.setupActionConf(actionConf, context, actionXml, appPath);
        return actionConf;
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        super.end(context, action);
        JobClient jobClient = null;
        boolean exception = false;
        try {
            if (action.getStatus() == WorkflowAction.Status.OK) {
                Element actionXml = XmlUtils.parseXml(action.getConf());
                Configuration conf = createBaseHadoopConf(context, actionXml);
                JobConf jobConf = new JobConf();
                XConfiguration.copy(conf, jobConf);
                jobClient = createJobClient(context, jobConf);
                RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalId()));
                if (runningJob == null) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "MR002",
                                                      "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", action
                            .getExternalId(), action.getId());
                }

                // TODO this has to be done in a better way
                if (!runningJob.getJobName().startsWith("oozie:action:")) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "MR001",
                                                      "ID swap should have happened in launcher job [{0}]", action.getExternalId());
                }
                Counters counters = runningJob.getCounters();
                if (counters != null) {
                    JSONObject json = counterstoJson(counters);
                    context.setVar(HADOOP_COUNTERS, json.toJSONString());
                }
                else {

                    context.setVar(HADOOP_COUNTERS, "");

                    XLog.getLog(getClass()).warn("Could not find Hadoop Counters for: [{0}]", action.getExternalId());
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

    @SuppressWarnings("unchecked")
    private JSONObject counterstoJson(Counters counters) {

        if (counters == null) {
            return null;
        }

        JSONObject groups = new JSONObject();
        for (String gName : counters.getGroupNames()) {
            JSONObject group = new JSONObject();
            for (Counters.Counter counter : counters.getGroup(gName)) {
                String cName = counter.getName();
                Long cValue = counter.getCounter();
                group.put(cName, cValue);
            }
            groups.put(gName, group);
        }
        return groups;
    }

}
