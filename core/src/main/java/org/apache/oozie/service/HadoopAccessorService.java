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
package org.apache.oozie.service;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.HashSet;

/**
 * The HadoopAccessorService returns HadoopAccessor instances configured to work on behalf of a user-group. <p/> The
 * default accessor used is the base accessor which just injects the UGI into the configuration instance used to
 * create/obtain JobClient and ileSystem instances. <p/> The HadoopAccess class to use can be configured in the
 * <code>oozie-site.xml</code> using the <code>oozie.service.HadoopAccessorService.accessor.class</code> property.
 */
public class HadoopAccessorService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "HadoopAccessorService.";
    public static final String JOB_TRACKER_WHITELIST = CONF_PREFIX + "jobTracker.whitelist";
    public static final String NAME_NODE_WHITELIST = CONF_PREFIX + "nameNode.whitelist";

    private Set<String> jobTrackerWhitelist = new HashSet<String>();
    private Set<String> nameNodeWhitelist = new HashSet<String>();

    public void init(Services services) throws ServiceException {
        for (String name : services.getConf().getStringCollection(JOB_TRACKER_WHITELIST)) {
            String tmp = name.toLowerCase().trim();
            if (tmp.length() == 0) {
                continue;
            }
            jobTrackerWhitelist.add(tmp);
        }
        XLog.getLog(getClass()).info(
                "JOB_TRACKER_WHITELIST :" + services.getConf().getStringCollection(JOB_TRACKER_WHITELIST)
                        + ", Total entries :" + jobTrackerWhitelist.size());
        for (String name : services.getConf().getStringCollection(NAME_NODE_WHITELIST)) {
            String tmp = name.toLowerCase().trim();
            if (tmp.length() == 0) {
                continue;
            }
            nameNodeWhitelist.add(tmp);
        }
        XLog.getLog(getClass()).info(
                "NAME_NODE_WHITELIST :" + services.getConf().getStringCollection(NAME_NODE_WHITELIST)
                        + ", Total entries :" + nameNodeWhitelist.size());
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
     * @param conf JobConf with all necessary information to create the
     *        JobClient.
     * @return JobClient created with the provided user/group.
     * @throws HadoopAccessorException if the client could not be created.
     */
    public JobClient createJobClient(String user, String group, JobConf conf) throws HadoopAccessorException {
        validateJobTracker(conf.get("mapred.job.tracker"));
        conf = createConfiguration(user, group, conf);
        try {
            return new JobClient(conf);
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    /**
     * Return a FileSystem created with the provided user/group.
     * 
     * @param conf Configuration with all necessary information to create the
     *        FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws HadoopAccessorException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, Configuration conf) throws HadoopAccessorException {
        try {
            validateNameNode(new URI(conf.get("fs.default.name")).getAuthority());
            conf = createConfiguration(user, group, conf);
            return FileSystem.get(conf);
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
        catch (URISyntaxException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    /**
     * Return a FileSystem created with the provided user/group for the
     * specified URI.
     * 
     * @param uri file system URI.
     * @param conf Configuration with all necessary information to create the
     *        FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws HadoopAccessorException if the filesystem could not be created.
     */
    public FileSystem createFileSystem(String user, String group, URI uri, Configuration conf)
            throws HadoopAccessorException {
        validateNameNode(uri.getAuthority());
        conf = createConfiguration(user, group, conf);
        try {
            return FileSystem.get(uri, conf);
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    /**
     * Validate Job tracker
     * @param jobTrackerUri
     * @throws HadoopAccessorException
     */
    protected void validateJobTracker(String jobTrackerUri) throws HadoopAccessorException {
        validate(jobTrackerUri, jobTrackerWhitelist, ErrorCode.E0900);
    }

    /**
     * Validate Namenode list
     * @param nameNodeUri
     * @throws HadoopAccessorException
     */
    protected void validateNameNode(String nameNodeUri) throws HadoopAccessorException {
        validate(nameNodeUri, nameNodeWhitelist, ErrorCode.E0901);
    }

    private void validate(String uri, Set<String> whitelist, ErrorCode error) throws HadoopAccessorException {
        if (uri != null) {
            uri = uri.toLowerCase().trim();
            if (whitelist.size() > 0 && !whitelist.contains(uri)) {
                throw new HadoopAccessorException(error, uri);
            }
        }
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
