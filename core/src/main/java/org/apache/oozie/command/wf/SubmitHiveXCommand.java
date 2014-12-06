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

package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.XOozieClient;
import org.jdom.Namespace;

public class SubmitHiveXCommand extends SubmitScriptLanguageXCommand {
    public SubmitHiveXCommand(Configuration conf) {
        super("submitHive", "submitHive", conf);
    }

    @Override
    protected String getWorkflowName(){
        return "hive";
    }

    @Override
    protected String getOptions(){
        return XOozieClient.HIVE_OPTIONS;
    }

    @Override
    protected String getScriptParamters() {
        return XOozieClient.HIVE_SCRIPT_PARAMS;
    }

    @Override
    protected Namespace getSectionNamespace(){
        return Namespace.getNamespace("uri:oozie:hive-action:0.5");
    }
}
