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
package org.apache.oozie.dependency;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.coord.CoordUtils;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIAccessorException;
import org.apache.oozie.util.XLog;
import org.jdom.Element;

public class FSURIHandler extends URIHandler {

    private static XLog LOG = XLog.getLog(FSURIHandler.class);
    private HadoopAccessorService service;

    public FSURIHandler() {
        service = Services.get().get(HadoopAccessorService.class);
    }

    @Override
    public void init(Configuration conf) {

    }

    @Override
    public Set<String> getSupportedSchemes() {
        return service.getSupportedSchemes();
    }

    @Override
    public DependencyType getDependencyType(URI uri) throws URIAccessorException {
        return DependencyType.PULL;
    }

    @Override
    public void registerForNotification(URI uri, String actionID) throws URIAccessorException {
        throw new UnsupportedOperationException("Notifications are not supported for " + uri.getScheme());
    }

    @Override
    public URIContext getURIContext(URI uri, Configuration conf, String user) throws URIAccessorException {
        FileSystem fs = getFileSystem(uri, conf, user);
        return new FSURIContext(conf, user, fs);
    }

    @Override
    public boolean create(URI uri, Configuration conf, String user) throws URIAccessorException {
        FileSystem fs = getFileSystem(uri, conf, user);
        return create(fs, uri);
    }

    @Override
    public boolean exists(URI uri, URIContext uriContext) throws URIAccessorException {
        try {
            FileSystem fs = ((FSURIContext) uriContext).getFileSystem();
            return fs.exists(new Path(uri));
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    @Override
    public boolean exists(URI uri, Configuration conf, String user) throws URIAccessorException {
        try {
            FileSystem fs = getFileSystem(uri, conf, user);
            return fs.exists(new Path(uri));
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    @Override
    public boolean delete(URI uri, Configuration conf, String user) throws URIAccessorException {
        FileSystem fs = getFileSystem(uri, conf, user);
        return delete(fs, uri);
    }

    @Override
    public String getURIWithDoneFlag(String uri, Element doneFlagElement) throws URIAccessorException {
        String doneFlag = CoordUtils.getDoneFlag(doneFlagElement);
        if (doneFlag.length() > 0) {
            uri += "/" + doneFlag;
        }
        return uri;
    }

    @Override
    public String getURIWithDoneFlag(String uri, String doneFlag) throws URIAccessorException {
        if (doneFlag.length() > 0) {
            uri += "/" + doneFlag;
        }
        return uri;
    }

    @Override
    public void validate(String uri) throws URIAccessorException {
    }

    @Override
    public void destroy() {

    }

    private FileSystem getFileSystem(URI uri, Configuration conf, String user) throws HadoopAccessorException {
        Configuration fsConf = service.createJobConf(uri.getAuthority());
        return service.createFileSystem(user, uri, fsConf);
    }

    private boolean create(FileSystem fs, URI uri) throws URIAccessorException {
        Path path = new Path(uri);
        try {
            if (!fs.exists(path)) {
                boolean status = fs.mkdirs(path);
                if (status) {
                    LOG.info("Creating directory at {0} succeeded.", path);
                }
                else {
                    LOG.info("Creating directory at {0} failed.", path);
                }
                return status;
            }
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
        return false;
    }

    private boolean delete(FileSystem fs, URI uri) throws URIAccessorException {
        Path path = new Path(uri);
        try {
            if (fs.exists(path)) {
                boolean status = fs.delete(path, true);
                if (status) {
                    LOG.info("Deletion of path {0} succeeded.", path);
                }
                else {
                    LOG.info("Deletion of path {0} failed.", path);
                }
                return status;
            }
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
        return false;
    }

}
