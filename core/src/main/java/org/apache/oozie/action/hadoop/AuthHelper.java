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

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

//TODO this class can go away once we use only 20.100+
public class AuthHelper {
    private static Class<? extends AuthHelper> KLASS = null;

    @SuppressWarnings("unchecked")
    public synchronized static AuthHelper get() {
        if (KLASS == null) {
            try {
                KLASS = (Class<? extends AuthHelper>)
                        Class.forName("org.apache.oozie.action.hadoop.kerberos.KerberosAuthHelper");
            }
            catch (ClassNotFoundException ex) {
                KLASS = AuthHelper.class;
            }
        }
        return ReflectionUtils.newInstance(KLASS, null);
    }

    public void set(JobClient jobClient, JobConf jobConf) throws IOException, InterruptedException {
    }

}
