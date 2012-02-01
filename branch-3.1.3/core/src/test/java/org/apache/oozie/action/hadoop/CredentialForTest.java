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

import java.util.Map.Entry;

import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.action.hadoop.Credentials;
import org.apache.oozie.action.hadoop.CredentialsProperties;



@SuppressWarnings("deprecation")
public class CredentialForTest extends Credentials {

    @Override
    public void addtoJobConf(JobConf jobconf, CredentialsProperties props, Context context) throws Exception {

        String paramA = null;
        String paramB = null;
        for (Entry<String, String>  parameter : props.getProperties().entrySet()) {
            String name = parameter.getKey();
            if (name.equals("aa")) {
                paramA = parameter.getValue();
            }
            else  if (name.equals("bb")) {
                paramB = parameter.getValue();
            }
        }

        if (paramA == null || paramB == null) {
            throw new CredentialException(ErrorCode.E0510, "required parameters is null.");
        }

        jobconf.set(props.getName(), "testcert");
    }

}
