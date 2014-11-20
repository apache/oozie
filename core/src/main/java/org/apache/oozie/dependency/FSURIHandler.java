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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.hadoop.FSLauncherURIHandler;
import org.apache.oozie.action.hadoop.LauncherURIHandler;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.Services;

public class FSURIHandler implements URIHandler {

    private HadoopAccessorService service;
    private Set<String> supportedSchemes;
    private List<Class<?>> classesToShip;

    @Override
    public void init(Configuration conf) {
        service = Services.get().get(HadoopAccessorService.class);
        supportedSchemes = service.getSupportedSchemes();
        classesToShip = new FSLauncherURIHandler().getClassesForLauncher();
    }

    @Override
    public Set<String> getSupportedSchemes() {
        return supportedSchemes;
    }

    @Override
    public Class<? extends LauncherURIHandler> getLauncherURIHandlerClass() {
        return FSLauncherURIHandler.class;
    }

    @Override
    public List<Class<?>> getClassesForLauncher() {
        return classesToShip;
    }

    @Override
    public DependencyType getDependencyType(URI uri) throws URIHandlerException {
        return DependencyType.PULL;
    }

    @Override
    public void registerForNotification(URI uri, Configuration conf, String user, String actionID)
            throws URIHandlerException {
        throw new UnsupportedOperationException("Notifications are not supported for " + uri.getScheme());
    }

    @Override
    public boolean unregisterFromNotification(URI uri, String actionID) {
        throw new UnsupportedOperationException("Notifications are not supported for " + uri.getScheme());
    }

    @Override
    public Context getContext(URI uri, Configuration conf, String user, boolean readOnly) throws URIHandlerException {
        FileSystem fs = getFileSystem(uri, conf, user);
        return new FSContext(conf, user, fs);
    }

    @Override
    public boolean exists(URI uri, Context context) throws URIHandlerException {
        try {
            FileSystem fs = ((FSContext) context).getFileSystem();
            return fs.exists(getNormalizedPath(uri));
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    @Override
    public boolean exists(URI uri, Configuration conf, String user) throws URIHandlerException {
        try {
            FileSystem fs = getFileSystem(uri, conf, user);
            return fs.exists(getNormalizedPath(uri));
        }
        catch (IOException e) {
            throw new HadoopAccessorException(ErrorCode.E0902, e);
        }
    }

    @Override
    public String getURIWithDoneFlag(String uri, String doneFlag) throws URIHandlerException {
        if (doneFlag.length() > 0) {
            uri += "/" + doneFlag;
        }
        return uri;
    }

    @Override
    public void validate(String uri) throws URIHandlerException {
    }

    @Override
    public void destroy() {

    }

    @Override
    public void delete(URI uri, Context context) throws URIHandlerException {
        FileSystem fs = ((FSContext) context).getFileSystem();
        Path path = new Path(uri);
        try {
            if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                    throw new URIHandlerException(ErrorCode.E0907, path.toString());
                }
            }
        }
        catch (IOException e) {
            throw new URIHandlerException(ErrorCode.E0907, path.toString());
        }
    }

    @Override
    public void delete(URI uri, Configuration conf, String user) throws URIHandlerException {
        Path path = new Path(uri);
        FileSystem fs = getFileSystem(uri, conf, user);
        try{
            if (fs.exists(path)) {
                if (!fs.delete(path, true)) {
                    throw new URIHandlerException(ErrorCode.E0907, path.toString());
                }
            }
        } catch (IOException e){
            throw new URIHandlerException(ErrorCode.E0907, path.toString());
        }
    }

    private Path getNormalizedPath(URI uri) {
        // Normalizes uri path replacing // with / in the path which users specify by mistake
        return new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
    }

    private FileSystem getFileSystem(URI uri, Configuration conf, String user) throws HadoopAccessorException {
        if (user == null) {
            throw new HadoopAccessorException(ErrorCode.E0902, "user has to be specified to access FileSystem");
        }
        Configuration fsConf = service.createJobConf(uri.getAuthority());
        return service.createFileSystem(user, uri, fsConf);
    }

    static class FSContext extends Context {

        private FileSystem fs;

        /**
         * Create a FSContext that can be used to access a filesystem URI
         *
         * @param conf Configuration to access the URI
         * @param user name of the user the URI should be accessed as
         * @param fs FileSystem to access
         */
        public FSContext(Configuration conf, String user, FileSystem fs) {
            super(conf, user);
            this.fs = fs;
        }

        /**
         * Get the FileSystem to access the URI
         * @return FileSystem to access the URI
         */
        public FileSystem getFileSystem() {
            return fs;
        }
    }

}
