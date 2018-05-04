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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.util.StringUtils;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

import java.util.ArrayList;
import java.util.List;

public class DistcpActionExecutor extends JavaActionExecutor{
    public static final String CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS = "org.apache.oozie.action.hadoop.DistcpMain";
    private static final String DISTCP_MAIN_CLASS_NAME = "org.apache.hadoop.tools.DistCp";
    public static final String CLASS_NAMES = "oozie.actions.main.classnames";
    private static final XLog LOG = XLog.getLog(DistcpActionExecutor.class);
    public static final String DISTCP_TYPE = "distcp";

    /**
     * Comma separated list of NameNode hosts to obtain delegation token(s) for.
     */
    private static final String OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS = "oozie.launcher.mapreduce.job.hdfs-servers";

    /**
     * Comma separated list to instruct ResourceManagers on either cluster to skip delegation token renewal for NameNode hosts.
     */
    private static final String OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS_TOKEN_RENEWAL_EXCLUDE =
            "oozie.launcher.mapreduce.job.hdfs-servers.token-renewal.exclude";
    private static final String JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE = "mapreduce.job.hdfs-servers.token-renewal.exclude";


    public DistcpActionExecutor() {
        super("distcp");
    }

    @Override
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        actionConf = super.setupActionConf(actionConf, context, actionXml, appPath);
        actionConf.set(JavaMain.JAVA_MAIN_CLASS, DISTCP_MAIN_CLASS_NAME);
        return actionConf;
    }

    @Override
    public List<Class<?>> getLauncherClasses() {
        List<Class<?>> classes = new ArrayList<>();
        try {
            classes.add(Class.forName(CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    /**
     * This function returns the Action classes names from the configuration
     *
     * @param type This is type of the action classes
     * @return Name of the class from the configuration
     */
    public static String getClassNamebyType(String type){
        String classname = null;
        for (String function : ConfigurationService.getStrings(CLASS_NAMES)) {
            function = StringUtils.trim(function);
            LOG.debug("class for Distcp Action: " + function);
            String[] str = function.split("=");
            if (str.length > 0) {
                if(type.equalsIgnoreCase(str[0])){
                    classname = new String(str[1]);
                }
            }
        }
        return classname;
    }

    /**
     * Return the sharelib name for the action.
     *
     * @return returns <code>distcp</code>.
     * @param actionXml
     */
    @Override
    protected String getDefaultShareLibName(Element actionXml) {
        return "distcp";
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS, CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS);
    }

    /**
     * Extracts information required for DistCp action between secure clusters (in the same or distinct Kerberos realms)
     *
     * @param jobconf workflow action configuration
     */
    @Override
    protected void setActionTokenProperties(final Configuration jobconf) {
        final String hdfsServers = jobconf.get(OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS);
        if (hdfsServers != null) {
            jobconf.set(MRJobConfig.JOB_NAMENODES, hdfsServers);
            final String tokenRenewalExclude = jobconf.get(OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS_TOKEN_RENEWAL_EXCLUDE);
            if (tokenRenewalExclude != null) {
                jobconf.set(JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE, tokenRenewalExclude);
            }
        }
    }
}
