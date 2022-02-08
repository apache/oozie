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
import org.apache.oozie.util.XLog;
import org.jdom2.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.oozie.action.hadoop.CredentialsProviderFactory.FS;
import static org.apache.oozie.action.hadoop.CredentialsProviderFactory.NAMENODE_FS;
import static org.apache.oozie.action.hadoop.FileSystemCredentials.FILESYSTEM_PATH;

public class DistcpActionExecutor extends JavaActionExecutor {
    private static final String CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS = "org.apache.oozie.action.hadoop.DistcpMain";
    private static final String DISTCP_MAIN_CLASS_NAME = "org.apache.hadoop.tools.DistCp";
    public static final String CLASS_NAMES = "oozie.actions.main.classnames";
    private static final XLog LOG = XLog.getLog(DistcpActionExecutor.class);

    /**
     * Comma separated list of NameNode hosts to obtain delegation token(s) for.
     */
    static final String OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS = "oozie.launcher.mapreduce.job.hdfs-servers";

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
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
        }
        return classes;
    }

    /**
     * Return the sharelib name for the action.
     *
     * @return returns <code>distcp</code>.
     * @param actionXml action xml element
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
     * @param actionConf workflow action configuration
     * @param credPropertiesMap Map of defined workflow credentials
     */
    @Override
    void addNameNodeCredentials(Configuration actionConf, Map<String, CredentialsProperties> credPropertiesMap) {
        String hdfsServers = actionConf.get(OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS);
        if (hdfsServers != null) {
            LOG.info("Overriding {0} default value from action config with {1}",
                    MRJobConfig.JOB_NAMENODES, hdfsServers);
            actionConf.set(MRJobConfig.JOB_NAMENODES, hdfsServers);
            final String tokenRenewalExclude = actionConf.get(OOZIE_LAUNCHER_MAPREDUCE_JOB_HDFS_SERVERS_TOKEN_RENEWAL_EXCLUDE);
            if (tokenRenewalExclude != null) {
                actionConf.set(JOB_NAMENODES_TOKEN_RENEWAL_EXCLUDE, tokenRenewalExclude);
            }
            CredentialsProperties fsCredentialProperties = new CredentialsProperties(NAMENODE_FS, FS);
            fsCredentialProperties.getProperties().put(FILESYSTEM_PATH, hdfsServers);
            credPropertiesMap.put(NAMENODE_FS, fsCredentialProperties);
        } else {
            super.addNameNodeCredentials(actionConf, credPropertiesMap);
        }
    }
}
