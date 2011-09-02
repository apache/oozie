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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.jdom.Element;
import org.jdom.Namespace;

import java.util.List;

public class PigActionExecutor extends JavaActionExecutor {

    public PigActionExecutor() {
        super("pig");
    }

    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        classes.add(LauncherMain.class);
        classes.add(MapReduceMain.class);
        classes.add(PigMain.class);
        return classes;
    }

    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, PigMain.class.getName());
    }

    void injectActionCallback(Context context, Configuration launcherConf) {
    }

    Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();
        String script = actionXml.getChild("script", ns).getTextTrim();
        String pigName = new Path(script).getName();
        addToCache(conf, appPath, script + "#" + pigName, false);
        return conf;
    }

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

        PigMain.setPigScript(actionConf, pigName, strParams);
        return actionConf;
    }

}
