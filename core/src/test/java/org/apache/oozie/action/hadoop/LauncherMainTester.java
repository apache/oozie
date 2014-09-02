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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class LauncherMainTester {

    public static void main(String[] args) throws Throwable {
        if (args.length == 0) {
            System.out.println("Hello World!");
        }
        if (args.length == 1) {
            if (args[0].equals("throwable")) {
                throw new Throwable("throwing throwable");
            }
            if (args[0].equals("exception")) {
                throw new IOException("throwing exception");
            }
            if (args[0].equals("exit0")) {
                System.exit(0);
            }
            if (args[0].equals("exit1")) {
                System.exit(1);
            }
            if (args[0].equals("out")) {
                File file = new File(System.getProperty("oozie.action.output.properties"));
                Properties props = new Properties();
                props.setProperty("a", "A");
                OutputStream os = new FileOutputStream(file);
                props.store(os, "");
                os.close();
                System.out.println(file.getAbsolutePath());
            }
            if (args[0].equals("id")) {
                File file = new File(System.getProperty("oozie.action.newId"));
                Properties props = new Properties();
                props.setProperty("id", "IDSWAP");
                OutputStream os = new FileOutputStream(file);
                props.store(os, "");
                os.close();
                System.out.println(file.getAbsolutePath());
            }
            if (args[0].equals("securityManager")) {
                SecurityManager sm = System.getSecurityManager();
                if (sm == null) {
                    throw new Throwable("no security manager");
                }
                // by using NULL as permission, if an underlaying SecurityManager is in place
                // a security exception will be thrown. As there is not underlaying SecurityManager
                // this tests that the delegation logic of the LauncherMapper SecurityManager is
                // correct for both checkPermission() signatures.
                sm.checkPermission(null);
                sm.checkPermission(null, sm.getSecurityContext());
            }
        }
    }

}
