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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.test.XFsTestCase;

import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

public abstract class MainTestCase extends XFsTestCase implements Callable<Void> {

    public static void execute(String user, final Callable<Void> callable) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                callable.call();
                return null;
            }
        });
    }

    public void testMain() throws Exception {
        execute(getTestUser(), this);
    }

}
