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
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorService;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.ArrayList;
import java.util.List;

import static org.apache.oozie.action.hadoop.LauncherAMUtils.CONF_OOZIE_ACTION_MAIN_CLASS;

public class HiveActionExecutor extends ScriptLanguageActionExecutor {

    private static final String HIVE_MAIN_CLASS_NAME = "org.apache.oozie.action.hadoop.HiveMain";
    static final String HIVE_QUERY = "oozie.hive.query";
    static final String HIVE_SCRIPT = "oozie.hive.script";
    static final String HIVE_PARAMS = "oozie.hive.params";
    static final String HIVE_ARGS = "oozie.hive.args";

    private boolean addScriptToCache;

    public HiveActionExecutor() {
        super("hive");
        this.addScriptToCache = false;
    }

    @Override
    public List<Class<?>> getLauncherClasses() {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        try {
            classes.add(Class.forName(HIVE_MAIN_CLASS_NAME));
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found", e);
	}
        return classes;
    }

    @Override
    protected boolean shouldAddScriptToCache() {
        return this.addScriptToCache;
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
        Element scriptElement = actionXml.getChild("script", ns);
        Element queryElement = actionXml.getChild("query", ns);
        if (scriptElement != null){
            String script = scriptElement.getTextTrim();
            String scriptName = new Path(script).getName();
            this.addScriptToCache = true;
            conf.set(HIVE_SCRIPT, scriptName);
        } else if (queryElement != null) {
            // Unable to use getTextTrim due to https://issues.apache.org/jira/browse/HIVE-8182
            String query = queryElement.getText();
            conf.set(HIVE_QUERY, query);
        } else {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "INVALID_ARGUMENTS",
                "Hive action requires one of <script> or <query> to be set. Neither were found.");
        }

        List<Element> params = (List<Element>) actionXml.getChildren("param", ns);
        String[] strParams = new String[params.size()];
        for (int i = 0; i < params.size(); i++) {
            strParams[i] = params.get(i).getTextTrim();
        }
        ActionUtils.setStrings(conf, HIVE_PARAMS, strParams);

        String[] strArgs = null;
        List<Element> eArgs = actionXml.getChildren("argument", ns);
        if (eArgs != null && eArgs.size() > 0) {
            strArgs = new String[eArgs.size()];
            for (int i = 0; i < eArgs.size(); i++) {
                strArgs[i] = eArgs.get(i).getTextTrim();
            }
        }
        ActionUtils.setStrings(conf, HIVE_ARGS, strArgs);
        return conf;
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

    @Override
    protected Configuration loadHadoopDefaultResources(Context context, Element actionXml) {
        boolean loadDefaultResources = ConfigurationService
                .getBoolean(HadoopAccessorService.ACTION_CONFS_LOAD_DEFAULT_RESOURCES);
        Configuration conf = super.createBaseHadoopConf(context, actionXml, loadDefaultResources);
        return conf;
    }
}
