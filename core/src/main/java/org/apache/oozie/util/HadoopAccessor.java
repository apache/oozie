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
package org.apache.oozie.util;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Creates JobClient and FileSystem instances injecting the provided user/group. <p/> Subclasses should overwrite the
 * create methods to exchange user credentials.
 */
public class HadoopAccessor {
    private String user;
    private String group;

    /**
     * Create an initialized instance.
     */
    public HadoopAccessor() {
    }

    /**
     * Sets the user and group to inject when creating an JobConf or FileSystem instance.
     *
     * @param user user id.
     * @param group group id.
     */
    public void setUGI(String user, String group) {
        this.user = notEmpty(user, "user");
        this.group = notEmpty(group, "group");
    }

    /**
     * Return a JobClient created with the provided user/group.
     *
     * @param conf JobConf with all necessary information to create the JobClient.
     * @return JobClient created with the provided user/group.
     * @throws IOException if the client could not be created.
     */
    public JobClient createJobClient(JobConf conf) throws IOException {
        notEmpty(user, "user");
        notEmpty(group, "group");
        conf = new JobConf(conf);
        injectUGI(conf);
        return new JobClient(conf);
    }

    /**
     * Return a FileSystem created with the provided user/group.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws IOException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(Configuration conf) throws IOException {
        notEmpty(user, "user");
        notEmpty(group, "group");
        conf = new Configuration(conf);
        injectUGI(conf);
        return FileSystem.get(conf);
    }

    /**
     * Return a FileSystem created with the provided user/group for the specified URI.
     *
     * @param uri file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws IOException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(URI uri, Configuration conf) throws IOException {
        notEmpty(user, "user");
        notEmpty(group, "group");
        conf = new Configuration(conf);
        injectUGI(conf);
        return FileSystem.get(uri, conf);
    }

    protected String notEmpty(String str, String name) {
        if (str == null) {
            throw new IllegalStateException(name + " cannot be null");
        }
        if (str.length() == 0) {
            throw new IllegalStateException(name + " cannot be empty");
        }
        return str;
    }

    protected void injectUGI(Configuration conf) {
        conf.set("user.name", user);
        conf.set("hadoop.job.ugi", user + "," + group);
    }

}
