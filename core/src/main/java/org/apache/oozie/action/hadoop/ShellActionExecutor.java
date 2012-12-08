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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.jdom.Element;
import org.jdom.Namespace;

public class ShellActionExecutor extends JavaActionExecutor {

    /**
     * Config property name to set the child environment
     */
    public String OOZIE_LAUNCHER_CHILD_ENV = "mapred.child.env";

    public ShellActionExecutor() {
        super("shell");
    }

    @Override
    protected List<Class> getLauncherClasses() {
        List<Class> classes = super.getLauncherClasses();
        // Base class of ShellMain dedicated for 'shell' action
        classes.add(LauncherMain.class);
        // Some utility methods used in ShelltMain
        classes.add(MapReduceMain.class);
        // Specific to Shell action
        classes.add(ShellMain.class);
        // ShellMain's inner class
        classes.add(ShellMain.OutputWriteThread.class);
        return classes;
    }

    @Override
    protected String getLauncherMain(Configuration launcherConf, Element actionXml) {
        return launcherConf.get(LauncherMapper.CONF_OOZIE_ACTION_MAIN_CLASS, ShellMain.class.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    Configuration setupActionConf(Configuration actionConf, Context context, Element actionXml, Path appPath)
            throws ActionExecutorException {
        super.setupActionConf(actionConf, context, actionXml, appPath);
        Namespace ns = actionXml.getNamespace();

        String exec = actionXml.getChild("exec", ns).getTextTrim();
        String execName = new Path(exec).getName();
        actionConf.set(ShellMain.CONF_OOZIE_SHELL_EXEC, execName);

        // Setting Shell command's arguments
        setListInConf("argument", actionXml, actionConf, ShellMain.CONF_OOZIE_SHELL_ARGS, false);
        // Setting Shell command's environment variable key=value
        setListInConf("env-var", actionXml, actionConf, ShellMain.CONF_OOZIE_SHELL_ENVS, true);

        // Setting capture output flag
        actionConf.setBoolean(ShellMain.CONF_OOZIE_SHELL_CAPTURE_OUTPUT,
                actionXml.getChild("capture-output", ns) != null);

        return actionConf;
    }

    /**
     * This method read a list of tag from an XML element and set the
     * Configuration accordingly
     *
     * @param tag
     * @param actionXml
     * @param actionConf
     * @param key
     * @param checkKeyValue
     * @throws ActionExecutorException
     */
    protected void setListInConf(String tag, Element actionXml, Configuration actionConf, String key,
            boolean checkKeyValue) throws ActionExecutorException {
        String[] strTagValue = null;
        Namespace ns = actionXml.getNamespace();
        List<Element> eTags = actionXml.getChildren(tag, ns);
        if (eTags != null && eTags.size() > 0) {
            strTagValue = new String[eTags.size()];
            for (int i = 0; i < eTags.size(); i++) {
                strTagValue[i] = eTags.get(i).getTextTrim();
                if (checkKeyValue) {
                    checkPair(strTagValue[i]);
                }
            }
        }
        MapReduceMain.setStrings(actionConf, key, strTagValue);
    }

    /**
     * Check if the key=value pair is appropriately formatted
     * @param pair
     * @throws ActionExecutorException
     */
    private void checkPair(String pair) throws ActionExecutorException {
        String[] varValue = pair.split("=");
        if (varValue == null || varValue.length <= 1) {
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED, "JA010",
                    "Wrong ENV format [{0}] in <env-var> , key=value format expected ", pair);
        }
    }

    @Override
    protected Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);
        conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", true);
        addDefaultChildEnv(conf);
        return conf;
    }

    /**
     * This method sets the PATH to current working directory for the launched
     * map task from where shell command will run.
     *
     * @param conf
     */
    protected void addDefaultChildEnv(Configuration conf) {
        String envValues = "PATH=.:$PATH";
        updateProperty(conf, OOZIE_LAUNCHER_CHILD_ENV, envValues);
    }

    /**
     * Utility method to append the new value to any property.
     *
     * @param conf
     * @param propertyName
     * @param appendValue
     */
    private void updateProperty(Configuration conf, String propertyName, String appendValue) {
        if (conf != null) {
            String val = conf.get(propertyName, "");
            if (val.length() > 0) {
                val += ",";
            }
            val += appendValue;
            conf.set(propertyName, val);
            log.debug("action conf is updated with default value for property " + propertyName + ", old value :"
                    + conf.get(propertyName, "") + ", new value :" + val);
        }
    }

}
