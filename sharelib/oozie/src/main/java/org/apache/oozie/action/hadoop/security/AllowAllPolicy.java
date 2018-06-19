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

import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * A security policy that grants AllPermissions.
 * <p>
 * LauncherAM uses Security Manager to handle exits.  With Derby version 10.12.1.1 and above, if a
 * security manager is configured, embedded Derby requires usederbyinternals permission, and
 * that is checked directly using AccessController.checkPermission.  This class will be used to
 * setup a security policy to grant AllPermissions to prevent failures related to Derby and other
 * similar dependencies in the future.
 * </p>
 */
class AllowAllPolicy extends Policy {

    private PermissionCollection perms;

    public AllowAllPolicy() {
        super();
        perms = new AllPermissionCollection();
        perms.add(new AllPermission());
    }

    @Override
    public PermissionCollection getPermissions(CodeSource codesource) {
        return perms;
    }

    static class AllPermissionCollection extends PermissionCollection {

        List<Permission> perms = new ArrayList<>();

        public void add(Permission p) {
            perms.add(p);
        }

        public boolean implies(Permission p) {
            return true;
        }

        public Enumeration<Permission> elements() {
            return Collections.enumeration(perms);
        }

        public boolean isReadOnly() {
            return false;
        }
    }
}