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

import static org.apache.oozie.action.hadoop.LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS;

import java.io.StringReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

public class HiveActionExecutor extends JavaActionExecutor {
    private static final String OOZIE_HIVE_DEFAULTS = "oozie.hive.defaults";

    public HiveActionExecutor() {
        super("hive");
    }

    @Override
    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(HiveMain.class);
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(CONF_OOZIE_ACTION_MAIN_CLASS, HiveMain.class.getName());
    }

    public Configuration setupHiveDefault(Configuration conf, Path appPath, Element actionXml)
            throws ActionExecutorException {
        try {
            //Setting up hive-default.xml file if specified by the Hive action
            Element actionConf = actionXml.getChild("configuration", actionXml.getNamespace());
            if (actionConf != null) {
                String strConf = XmlUtils.prettyPrint(actionConf).toString();
                XConfiguration inlineConf = new XConfiguration(new StringReader(strConf));
                if (inlineConf.get(OOZIE_HIVE_DEFAULTS) != null) {
                    Path hiveDefaults = new Path(inlineConf.get(OOZIE_HIVE_DEFAULTS));
                    // hive-default.xml will be softlinked to the working dir which is in the launcher CP.
                    // the softlink is done as 'oozie-user-hive-default.xml' and Oozie HiveMain class will
                    // check if the Hive being used has a hive-default.xml or not, if not it will rename
                    // it as hive-default.xml before invoking hive
                    addToCache(conf, appPath, hiveDefaults + "#" + HiveMain.USER_HIVE_DEFAULT_FILE, false);
                }
            }
            return conf;
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    protected Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        try {
            super.setupLauncherConf(conf, actionXml, appPath, context);
            Namespace ns = actionXml.getNamespace();

            setupHiveDefault(conf, appPath, actionXml);

            String script = actionXml.getChild("script", ns).getTextTrim();
            String scriptName = new Path(script).getName();
            addToCache(conf, appPath, script + "#" + scriptName, false);
            return conf;
        }
        catch (Exception ex) {
            throw convertException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml,
                                  Path appPath) throws ActionExecutorException {
        Configuration conf = super.setupActionConf(actionConf, context, actionXml, appPath);

        Namespace ns = actionXml.getNamespace();
        String script = actionXml.getChild("script", ns).getTextTrim();
        String scriptName = new Path(script).getName();
        addToCache(conf, appPath, script + "#" + scriptName, false);

        List<Element> params = (List<Element>) actionXml.getChildren("param", ns);
        String[] strParams = new String[params.size()];
        for (int i = 0; i < params.size(); i++) {
            strParams[i] = params.get(i).getTextTrim();
        }

        HiveMain.setHiveScript(conf, scriptName, strParams);
        return conf;
    }

    @Override
    protected boolean getCaptureOutput(WorkflowAction action) throws JDOMException {
        return true;
    }

    /**
     * Return the sharelib postfix for the action.
     *
     * @param context executor context.
     * @param actionXml the action XML.
     * @return the action sharelib post fix, this implementation returns <code>hive</code>.
     */
    protected String getShareLibPostFix(Context context, Element actionXml) {
        return "hive";
    }

}
