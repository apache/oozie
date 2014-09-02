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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.util.XLog;
import org.jdom.Element;
import org.jdom.Namespace;

import java.io.IOException;
import java.util.List;

public abstract class ScriptLanguageActionExecutor extends JavaActionExecutor {

    public ScriptLanguageActionExecutor(String type) {
        super(type);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<Class> getLauncherClasses() {
        return null;
    }

    @Override
    protected Configuration setupLauncherConf(Configuration conf, Element actionXml, Path appPath, Context context)
            throws ActionExecutorException {
        super.setupLauncherConf(conf, actionXml, appPath, context);
        Namespace ns = actionXml.getNamespace();
        String script = actionXml.getChild("script", ns).getTextTrim();
        String name = new Path(script).getName();
        String scriptContent = context.getProtoActionConf().get(this.getScriptName());

        Path scriptFile = null;
        if (scriptContent != null) { // Create script on filesystem if this is
            // an http submission job;
            FSDataOutputStream dos = null;
            try {
                Path actionPath = context.getActionDir();
                scriptFile = new Path(actionPath, script);
                FileSystem fs = context.getAppFileSystem();
                dos = fs.create(scriptFile);
                dos.writeBytes(scriptContent);

                addToCache(conf, actionPath, script + "#" + name, false);
            }
            catch (Exception ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.ERROR, "FAILED_OPERATION", XLog
                        .format("Not able to write script file {0} on hdfs", scriptFile), ex);
            }
            finally {
                try {
                    if (dos != null) {
                        dos.close();
                    }
                }
                catch (IOException ex) {
                    XLog.getLog(getClass()).error("Error: " + ex.getMessage());
                }
            }
        }
        else {
            addToCache(conf, appPath, script + "#" + name, false);
        }

        return conf;
    }

    protected abstract String getScriptName();
}
