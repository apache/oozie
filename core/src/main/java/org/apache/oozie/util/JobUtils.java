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
package org.apache.oozie.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

/**
 * Job utilities.
 */
public class JobUtils {
    /**
     * Normalize appPath in job conf with the provided user/group - If it's not
     * jobs via proxy submission, after normalization appPath always points to
     * job's Xml definition file.
     * <p/>
     * 
     * @param user user
     * @param group group
     * @param conf job configuration.
     * @throws IOException thrown if normalization can not be done properly.
     */
    public static void normalizeAppPath(String user, String group, Configuration conf) throws IOException {
        if (user == null) {
            throw new IllegalArgumentException("user cannot be null");
        }
        
        if (group == null) {
            throw new IllegalArgumentException("group cannot be null");
        }
        
        if (conf.get(XOozieClient.IS_PROXY_SUBMISSION) != null) { // do nothing for proxy submission job;
            return;
        }

        String wfPathStr = conf.get(OozieClient.APP_PATH);
        String coordPathStr = conf.get(OozieClient.COORDINATOR_APP_PATH);
        String appPathStr = wfPathStr != null ? wfPathStr : coordPathStr;

        FileSystem fs = null;
        try {
            fs = Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, new Path(appPathStr).toUri(), conf);
        }
        catch (HadoopAccessorException ex) {
            throw new IOException(ex.getMessage());
        }

        Path appPath = new Path(appPathStr);
        String normalizedAppPathStr = appPathStr;
        if (!fs.exists(appPath)) {
            throw new IOException("Error: " + appPathStr + " does not exist");
        }

        FileStatus fileStatus = fs.getFileStatus(appPath);
        Path appXml = appPath;
        // Normalize appPath here - it will always point to a workflow/coordinator xml definition file;
        if (fileStatus.isDir()) {
            appXml = new Path(appPath, (wfPathStr != null)? "workflow.xml" : "coordinator.xml");
            normalizedAppPathStr = appXml.toString();
        }

        if (wfPathStr != null) {
            conf.set(OozieClient.APP_PATH, normalizedAppPathStr);
        }
        else if (coordPathStr != null) {
            conf.set(OozieClient.COORDINATOR_APP_PATH, normalizedAppPathStr);
        }
    }
}