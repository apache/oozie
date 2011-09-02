/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

public class LauncherMainTester {

    public static void main(String[] args) throws Throwable {
        if (args.length == 0) {
            System.out.println("Hello World!");
        }
        if (args.length == 1) {
            if (args[0].equals("ex")) {
                throw new Throwable("throwing exception");
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
                File file = new File(System.getProperty("oozie.action.newId.properties"));
                Properties props = new Properties();
                props.setProperty("id", "IDSWAP");
                OutputStream os = new FileOutputStream(file);
                props.store(os, "");
                os.close();
                System.out.println(file.getAbsolutePath());
            }
        }
    }

}
