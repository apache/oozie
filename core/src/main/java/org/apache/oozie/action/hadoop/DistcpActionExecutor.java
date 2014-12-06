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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

public class DistcpActionExecutor extends JavaActionExecutor{
    public static final String CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS = "org.apache.oozie.action.hadoop.DistcpMain";
    private static final String DISTCP_MAIN_CLASS_NAME = "org.apache.hadoop.tools.DistCp";
    public static final String CLASS_NAMES = "oozie.actions.main.classnames";
    private static final XLog LOG = XLog.getLog(DistcpActionExecutor.class);
    public static final String DISTCP_TYPE = "distcp";

    public DistcpActionExecutor() {
        super("distcp");
    }

    @Override
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        actionConf = super.setupActionConf(actionConf, context, actionXml, appPath);
        String classNameDistcp = CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS;
        String name = getClassNamebyType(DISTCP_TYPE);
        if(name != null){
            classNameDistcp = name;
        }
        actionConf.set(JavaMain.JAVA_MAIN_CLASS, DISTCP_MAIN_CLASS_NAME);
        return actionConf;
    }

    @Override
    public List<Class> getLauncherClasses() {
        List<Class> classes = new ArrayList<Class>();
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
            function = DistcpActionExecutor.Trim(function);
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
     * To trim string
     *
     * @param str
     * @return trim string
     */
    public static String Trim(String str) {
        if (str != null) {
            str = str.replaceAll("\\n", "");
            str = str.replaceAll("\\t", "");
            str = str.trim();
        }
        return str;
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
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, CONF_OOZIE_DISTCP_ACTION_MAIN_CLASS);
    }

}
