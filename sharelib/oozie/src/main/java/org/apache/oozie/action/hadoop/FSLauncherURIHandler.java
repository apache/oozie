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

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class FSLauncherURIHandler implements LauncherURIHandler {

    @Override
    public boolean create(URI uri, Configuration conf) throws LauncherException {
        boolean status = false;
        try {
            FileSystem fs = FileSystem.get(uri, conf);
            Path path = getNormalizedPath(uri);
            if (!fs.exists(path)) {
                status = fs.mkdirs(path);
                if (status) {
                    System.out.println("Creating directory at " + path + " succeeded.");
                }
                else {
                    System.out.println("Creating directory at " + path + " failed.");
                }
            }
        }
        catch (IOException e) {
            throw new LauncherException("Creating directory at " + uri + " failed.", e);
        }
        return status;
    }

    @Override
    public boolean delete(URI uri, Configuration conf) throws LauncherException {
        boolean status = false;
        try {
            FileSystem fs = FileSystem.get(uri, conf);
            Path[] pathArr = FileUtil.stat2Paths(fs.globStatus(getNormalizedPath(uri)));
            if (pathArr != null && pathArr.length > 0) {
                int fsGlobMax = conf.getInt(LauncherMapper.CONF_OOZIE_ACTION_FS_GLOB_MAX, 1000);
                if (pathArr.length > fsGlobMax) {
                    throw new LauncherException("exceeds max number (" + fsGlobMax
                            + ") of files/dirs to delete in <prepare>");
                }
                for (Path path : pathArr) {
                    if (fs.exists(path)) {
                        status = fs.delete(path, true);
                        if (status) {
                            System.out.println("Deletion of path " + path + " succeeded.");
                        }
                        else {
                            System.out.println("Deletion of path " + path + " failed.");
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new LauncherException("Deletion of path " + uri + " failed.", e);
        }
        return status;
    }

    private Path getNormalizedPath(URI uri) {
        // Normalizes uri path replacing // with / in the path which users specify by mistake
        return new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
    }

    @Override
    public List<Class<?>> getClassesForLauncher() {
        return null;
    }

}
