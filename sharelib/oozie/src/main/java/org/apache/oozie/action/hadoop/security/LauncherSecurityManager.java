/*
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

package org.apache.oozie.action.hadoop.security;

import java.security.Permission;
import java.security.Policy;

public class LauncherSecurityManager extends SecurityManager {
    private boolean exitInvoked;
    private int exitCode;
    private SecurityManager originalSecurityManager;
    private Policy originalPolicy;

    public LauncherSecurityManager() {
        exitInvoked = false;
        exitCode = 0;
        originalSecurityManager = System.getSecurityManager();
        originalPolicy = Policy.getPolicy();
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (originalSecurityManager != null) {
            // check everything with the original SecurityManager
            originalSecurityManager.checkPermission(perm, context);
        }
    }

    @Override
    public void checkPermission(Permission perm) {
        if (originalSecurityManager != null) {
            // check everything with the original SecurityManager
            originalSecurityManager.checkPermission(perm);
        }
    }

    @Override
    public void checkExit(int status) throws SecurityException {
        exitInvoked = true;
        exitCode = status;
        throw new SecurityException("Intercepted System.exit(" + status + ")");
    }

    public boolean getExitInvoked() {
        return exitInvoked;
    }

    public int getExitCode() {
        return exitCode;
    }

    public void enable() {
        if (System.getSecurityManager() != this) {
            Policy.setPolicy(new AllowAllPolicy());
            System.setSecurityManager(this);
        }
    }

    public void disable() {
        if (System.getSecurityManager() == this) {
            System.setSecurityManager(originalSecurityManager);
            Policy.setPolicy(originalPolicy);
        }
    }

    public void reset() {
        exitInvoked = false;
        exitCode = 0;
    }
}
