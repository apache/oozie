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

public class LauncherAMTestMainClass {
    public static final String SECURITY_EXCEPTION = "security";
    public static final String LAUNCHER_EXCEPTION = "launcher";
    public static final String JAVA_EXCEPTION = "java";
    public static final String THROWABLE = "throwable";

    public static final String JAVA_EXCEPTION_MESSAGE = "Java Exception";
    public static final String SECURITY_EXCEPTION_MESSAGE = "Security Exception";
    public static final String THROWABLE_MESSAGE = "Throwable";
    public static final int LAUNCHER_ERROR_CODE = 1234;

    public static void main(String args[]) throws Throwable {
        System.out.println("Invocation of TestMain");

        if (args != null && args.length == 1) {
            switch (args[0]){
                case JAVA_EXCEPTION:
                    throw new JavaMain.JavaMainException(new RuntimeException(JAVA_EXCEPTION_MESSAGE));
                case LAUNCHER_EXCEPTION:
                    throw new LauncherMainException(LAUNCHER_ERROR_CODE);
                case SECURITY_EXCEPTION:
                    throw new SecurityException(SECURITY_EXCEPTION_MESSAGE);
                case THROWABLE:
                    throw new Throwable(THROWABLE_MESSAGE);
            }
        }
    }
}
