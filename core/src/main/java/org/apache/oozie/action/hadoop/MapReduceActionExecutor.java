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
import java.util.List;

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
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;
import org.json.simple.JSONObject;

public class MapReduceActionExecutor extends JavaActionExecutor {

    public static final String OOZIE_ACTION_EXTERNAL_STATS_WRITE = "oozie.action.external.stats.write";
    public static final String HADOOP_COUNTERS = "hadoop.counters";
    public static final String OOZIE_MAPREDUCE_UBER_JAR = "oozie.mapreduce.uber.jar";
    public static final String OOZIE_MAPREDUCE_UBER_JAR_ENABLE = "oozie.action.mapreduce.uber.jar.enable";
    private XLog log = XLog.getLog(getClass());

    public MapReduceActionExecutor() {
        super("map-reduce");
    }

    @Override
    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(StreamingMain.class);
        classes.add(PipesMain.class);
        return classes;
    }

    @Override
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

    @Override
    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context) throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);
        conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", true);
        return conf;
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        boolean regularMR = false;
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
                PipesMain.setPipes(actionConf, map, reduce, inputFormat, partitioner, writer, program, appPath);
            }
            else {
                regularMR = true;
            }
        }
        actionConf = super.setupActionConf(actionConf, context, actionXml, appPath);

        // For "regular" (not streaming or pipes) MR jobs
        if (regularMR) {
            // Resolve uber jar path (has to be done after super because oozie.mapreduce.uber.jar is under <configuration>)
            String uberJar = actionConf.get(OOZIE_MAPREDUCE_UBER_JAR);
            if (uberJar != null) {
                if (!Services.get().getConf().getBoolean(OOZIE_MAPREDUCE_UBER_JAR_ENABLE, false)) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "MR003",
                            "{0} property is not allowed.  Set {1} to true in oozie-site to enable.", OOZIE_MAPREDUCE_UBER_JAR,
                            OOZIE_MAPREDUCE_UBER_JAR_ENABLE);
                }
                String nameNode = actionXml.getChildTextTrim("name-node", ns);
                if (nameNode != null) {
                    Path uberJarPath = new Path(uberJar);
                    if (uberJarPath.toUri().getScheme() == null || uberJarPath.toUri().getAuthority() == null) {
                        if (uberJarPath.isAbsolute()) {     // absolute path without namenode --> prepend namenode
                            Path nameNodePath = new Path(nameNode);
                            String nameNodeSchemeAuthority = nameNodePath.toUri().getScheme()
                                    + "://" + nameNodePath.toUri().getAuthority();
                            actionConf.set(OOZIE_MAPREDUCE_UBER_JAR, new Path(nameNodeSchemeAuthority + uberJarPath).toString());
                        }
                        else {                              // relative path --> prepend app path
                            actionConf.set(OOZIE_MAPREDUCE_UBER_JAR, new Path(appPath, uberJarPath).toString());
                        }
                    }
                }
            }
        }
        else {
            if (actionConf.get(OOZIE_MAPREDUCE_UBER_JAR) != null) {
                log.warn("The " + OOZIE_MAPREDUCE_UBER_JAR + " property is only applicable for MapReduce (not streaming nor pipes)"
                        + " workflows, ignoring");
                actionConf.set(OOZIE_MAPREDUCE_UBER_JAR, "");
            }
        }

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
                JobConf jobConf = createBaseHadoopConf(context, actionXml);
                jobClient = createJobClient(context, jobConf);
                RunningJob runningJob = jobClient.getJob(JobID.forName(action.getExternalId()));
                if (runningJob == null) {
                    throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "MR002",
                                                      "Unknown hadoop job [{0}] associated with action [{1}].  Failing this action!", action
                            .getExternalId(), action.getId());
                }

                Counters counters = runningJob.getCounters();
                if (counters != null) {
                    ActionStats stats = new MRStats(counters);
                    String statsJsonString = stats.toJSON();
                    context.setVar(HADOOP_COUNTERS, statsJsonString);

                    // If action stats write property is set to false by user or
                    // size of stats is greater than the maximum allowed size,
                    // do not store the action stats
                    if (Boolean.parseBoolean(evaluateConfigurationProperty(actionXml,
                            OOZIE_ACTION_EXTERNAL_STATS_WRITE, "false"))
                            && (statsJsonString.getBytes().length <= getMaxExternalStatsSize())) {
                        context.setExecutionStats(statsJsonString);
                        log.debug(
                                "Printing stats for Map-Reduce action as a JSON string : [{0}]", statsJsonString);
                    }
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

    // Return the value of the specified configuration property
    private String evaluateConfigurationProperty(Element actionConf, String key, String defaultValue) throws ActionExecutorException {
        try {
            if (actionConf != null) {
                Namespace ns = actionConf.getNamespace();
                Element e = actionConf.getChild("configuration", ns);
                String strConf = XmlUtils.prettyPrint(e).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                return inlineConf.get(key, defaultValue);
            }
            return "";
        }
        catch (IOException ex) {
            throw convertException(ex);
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

    /**
     * Return the sharelib name for the action.
     *
     * @return returns <code>streaming</code> if mapreduce-streaming action, <code>NULL</code> otherwise.
     * @param actionXml
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        Namespace ns = actionXml.getNamespace();
        return (actionXml.getChild("streaming", ns) != null) ? "mapreduce-streaming" : null;
    }

    @Override
    JobConf createLauncherConf(FileSystem actionFs, Context context, WorkflowAction action, Element actionXml,
            Configuration actionConf) throws ActionExecutorException {
        // If the user is using a regular MapReduce job and specified an uber jar, we need to also set it for the launcher;
        // so we override createLauncherConf to call super and then to set the uber jar if specified. At this point, checking that
        // uber jars are enabled and resolving the uber jar path is already done by setupActionConf() when it parsed the actionConf
        // argument and we can just look up the uber jar in the actionConf argument.
        JobConf launcherJobConf = super.createLauncherConf(actionFs, context, action, actionXml, actionConf);
        Namespace ns = actionXml.getNamespace();
        if (actionXml.getChild("streaming", ns) == null && actionXml.getChild("pipes", ns) == null) {
            // Set for uber jar
            String uberJar = actionConf.get(MapReduceActionExecutor.OOZIE_MAPREDUCE_UBER_JAR);
            if (uberJar != null && uberJar.trim().length() > 0) {
                launcherJobConf.setJar(uberJar);
            }
        }
        return launcherJobConf;
    }
}
