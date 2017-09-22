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

package org.apache.oozie.tools;

import java.lang.reflect.Method;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Utility class which can disable Erasure Coding for a given path.
 *
 * Note that Erasure Coding was introduced in Hadoop 3, so in order for this class
 * to be compilable, reflection is used. Later, when we drop support for Hadoop 2.x,
 * this can be rewritten.
 */
public final class ECPolicyDisabler {
    private static final String GETREPLICATIONPOLICY_METHOD = "getReplicationPolicy";
    private static final String ERASURECODING_POLICIES_CLASS = "org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies";
    private static final String GETNAME_METHOD = "getName";
    private static final String SETERASURECODINGPOLICY_METHOD = "setErasureCodingPolicy";
    private static final String GETERASURECODINGPOLICY_METHOD = "getErasureCodingPolicy";

    public static void tryDisableECPolicyForPath(FileSystem fs, Path path) {
        if (fs instanceof DistributedFileSystem && supportsErasureCoding()) {
            System.out.println("Found Hadoop that supports Erasure Coding. Trying to disable Erasure Coding for path: "+ path);
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            final Object replicationPolicy = getReplicationPolicy();
            Method getErasureCodingPolicyMethod = getMethod(dfs, GETERASURECODINGPOLICY_METHOD);
            final Object currentECPolicy = invokeMethod(getErasureCodingPolicyMethod, dfs, path);

            if (currentECPolicy != replicationPolicy) {
                Method setECPolicyMethod = getMethod(dfs, SETERASURECODINGPOLICY_METHOD);
                Method policyGetNameMethod = getMethod(replicationPolicy, GETNAME_METHOD);

                String name = (String) invokeMethod(policyGetNameMethod, replicationPolicy);

                invokeMethod(setECPolicyMethod, dfs, path, name);
                System.out.println("Done");
            } else {
                System.out.println("Current policy is already replication");
            }
        } else {
            System.out.println("Found Hadoop that does not support Erasure Coding. Not taking any action.");
        }
    }

    private static boolean supportsErasureCoding() {
        try {
            getECPoliciesClass();
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static Object getReplicationPolicy() {
        try {
            Class<?> c = getECPoliciesClass();
            Method m = c.getMethod(GETREPLICATIONPOLICY_METHOD);
            return m.invoke(null);
        } catch (Exception e) {
            System.err.println("Error accessing method with reflection");
            throw new RuntimeException(e);
        }
    }

    private static Class<?> getECPoliciesClass() throws ClassNotFoundException {
        return Class.forName(ERASURECODING_POLICIES_CLASS);
    }

    private static Method getMethod(Object object, String methodName) {
        Method[] methods = object.getClass().getMethods();
        Method method = null;
        for (Method m : methods) {
            if (m.getName().equals(methodName)) {
                method = m;
                break;
            }
        }

        if (method == null) {
            throw new RuntimeException("Method " + methodName + "() not found");
        }

        return method;
    }

    private static Object invokeMethod(Method m, Object instance, Object... args) {
        try {
            return m.invoke(instance, args);
        } catch (Exception e) {
            System.err.println("Error invoking method with reflection");
            throw new RuntimeException(e);
        }
    }
}
