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
package org.apache.oozie.service;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/**
 * The HadoopAccessorService returns HadoopAccessor instances configured to work on behalf of a user-group.
 * <p/>
 * The default accessor used is the base accessor which just injects the UGI into the configuration instance
 * used to create/obtain JobClient and ileSystem instances.
 * <p/>
 * The HadoopAccess class to use can be configured in the <code>oozie-site.xml</code> using the
 * <code>oozie.service.HadoopAccessorService.accessor.class</code> property.
 */
public class HadoopAccessorService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HadoopAccessorService.";


    public void init(Services services) throws ServiceException {
        init(services.getConf());
    }

    public void init(Configuration serviceConf) throws ServiceException {
    }

    public void destroy() {
    }

    public Class<? extends Service> getInterface() {
        return HadoopAccessorService.class;
    }

    /**
     * Return a JobClient created with the provided user/group.
     *
     * @param conf JobConf with all necessary information to create the JobClient.
     * @return JobClient created with the provided user/group.
     * @throws IOException if the client could not be created.
     */
    public JobClient createJobClient(String user, String group, JobConf conf) throws IOException {
        conf = createConfiguration(user, group, conf);
        return new JobClient(conf);
    }

    /**
     * Return a FileSystem created with the provided user/group.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws IOException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, Configuration conf) throws IOException {
        conf = createConfiguration(user, group, conf);
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
    public FileSystem createFileSystem(String user, String group, URI uri, Configuration conf) throws IOException {
        conf = createConfiguration(user, group, conf);
        return FileSystem.get(uri, conf);
    }


    @SuppressWarnings("unchecked")
    private <C extends Configuration> C createConfiguration(String user, String group, C conf) {
        ParamChecker.notEmpty(user, "user");
        ParamChecker.notEmpty(group, "group");
        C fsConf = (C) ((conf instanceof JobConf) ? new JobConf() : new Configuration());
        XConfiguration.copy(conf, fsConf);
        fsConf.set("user.name", user);
        fsConf.set("hadoop.job.ugi", user + "," + group);
        return fsConf;
    }

    /**
     * Add a file to the ClassPath via the DistributedCache.
     */
    public void addFileToClassPath(String user, String group, final Path file, final Configuration conf)
            throws IOException {
        Configuration defaultConf = createConfiguration(user, group, conf);
        DistributedCache.addFileToClassPath(file, defaultConf);
        DistributedCache.addFileToClassPath(file, conf);
    }

}
