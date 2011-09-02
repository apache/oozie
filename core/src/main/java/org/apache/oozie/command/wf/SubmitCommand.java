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
package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.service.WorkflowAppService;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.DagXLogInfoService;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.command.Command;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.WorkflowLib;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;

import java.util.Date;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.io.IOException;

public class SubmitCommand extends Command<String> {
    public static final String CONFIG_DEFAULT = "config-default.xml";

    private Configuration conf;
    private String authToken;

    public SubmitCommand(Configuration conf, String authToken) {
        super("submit", "submit", 0, XLog.STD);
        this.conf = ParamChecker.notNull(conf, "conf");
        this.authToken = ParamChecker.notEmpty(authToken, "authToken");
    }

    private static final String HADOOP_UGI = "hadoop.job.ugi";
    private static final String HADOOP_USER = "user.name";
    private static final Set<String> DISALLOWED_PROPERTIES = new HashSet<String>();

    static {
        DISALLOWED_PROPERTIES.add(HADOOP_USER);
        DISALLOWED_PROPERTIES.add(HADOOP_UGI);
        DISALLOWED_PROPERTIES.add(WorkflowAppService.HADOOP_JT_KERBEROS_NAME);
        DISALLOWED_PROPERTIES.add(WorkflowAppService.HADOOP_NN_KERBEROS_NAME);
    }

    public static void validateDefaultConfiguration(Configuration conf) throws CommandException {
        for (String prop : DISALLOWED_PROPERTIES) {
            if (conf.get(prop) != null) {
                throw new CommandException(ErrorCode.E0804, prop);
            }
        }
    }

    @Override
    protected String call(WorkflowStore store) throws StoreException, CommandException {
        incrJobCounter(1);
        WorkflowAppService wps = Services.get().get(WorkflowAppService.class);
        try {
            XLog.Info.get().setParameter(DagXLogInfoService.TOKEN, conf.get(OozieClient.LOG_TOKEN));
            WorkflowApp app = wps.parseDef(conf, authToken);
            XConfiguration protoActionConf = wps.createProtoActionConf(conf, authToken);
            WorkflowLib workflowLib = Services.get().get(WorkflowStoreService.class).getWorkflowLibWithNoDB();

            Path configDefault = new Path(conf.get(OozieClient.APP_PATH), CONFIG_DEFAULT);

            String user = conf.get(OozieClient.USER_NAME);
            String group = conf.get(OozieClient.GROUP_NAME);
            FileSystem fs = Services.get().get(HadoopAccessorService.class).
                    createFileSystem(user, group, configDefault.toUri(), new Configuration());

            if (fs.exists(configDefault)) {
                try {
                    Configuration defaultConf = new XConfiguration(fs.open(configDefault));
                    validateDefaultConfiguration(defaultConf);
                    XConfiguration.injectDefaults(defaultConf, conf);
                }
                catch (IOException ex) {
                    throw new IOException("default configuration file, " + ex.getMessage(), ex);
                }
            }

            // Resolving all variables in the job properties.
            // This ensures the Hadoop Configuration semantics is preserved.
            XConfiguration resolvedVarsConf = new XConfiguration();
            for (Map.Entry<String, String> entry  : conf) {
                resolvedVarsConf.set(entry.getKey(), conf.get(entry.getKey()));
            }
            conf = resolvedVarsConf;            

            WorkflowInstance wfInstance;
            try {
                wfInstance = workflowLib.createInstance(app, conf);
            }
            catch (WorkflowException e) {
                throw new StoreException(e);
            }
            WorkflowJobBean workflow = new WorkflowJobBean();
            workflow.setId(wfInstance.getId());
            workflow.setAppName(app.getName());
            workflow.setAppPath(conf.get(OozieClient.APP_PATH));
            workflow.setConf(XmlUtils.prettyPrint(conf).toString());
            workflow.setProtoActionConf(protoActionConf.toXmlString());
            workflow.setCreatedTime(new Date());
            workflow.setLogToken(conf.get(OozieClient.LOG_TOKEN, ""));
            workflow.setStatus(WorkflowJob.Status.PREP);
            workflow.setRun(0);
            workflow.setUser(conf.get(OozieClient.USER_NAME));
            workflow.setGroup(conf.get(OozieClient.GROUP_NAME));
            workflow.setAuthToken(authToken);
            workflow.setWorkflowInstance(wfInstance);
            workflow.setExternalId(conf.get(OozieClient.EXTERNAL_ID));

            setLogInfo(workflow);

            store.insertWorkflow(workflow);
            return workflow.getId();
        }
        catch (WorkflowException ex) {
            throw new CommandException(ex);
        }
        catch (IOException ex) {
            throw new CommandException(ErrorCode.E0803, ex);
        }
    }

}
