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

/**
 * Created with IntelliJ IDEA.
 * User: bzhang
 * Date: 12/26/12
 * Time: 2:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class SubmitHiveXCommand extends SubmitScriptLanguageXCommand {
    public SubmitHiveXCommand(Configuration conf, String authToken) {
        super("submitHive", "submitHive", conf, authToken);
    }

    protected String getLanguageName(){
        return "hive";
    }

    protected String getOptions(){
        return XOozieClient.HIVE_OPTIONS;
    }

    protected Namespace getSectionNamespace(){
        return Namespace.getNamespace("uri:oozie:hive-action:0.2");
    }
}
