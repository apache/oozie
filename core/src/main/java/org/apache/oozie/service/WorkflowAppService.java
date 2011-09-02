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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.HadoopAccessor;
import org.apache.oozie.ErrorCode;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service that provides application workflow definition reading from the path
 * and creation of the proto configuration.
 */
public abstract class WorkflowAppService implements Service {

    public static final String APP_LIB_JAR_PATH_LIST = "oozie.wf.application.jar.paths";

    public static final String APP_LIB_SO_PATH_LIST = "oozie.wf.applcation.so.paths";

    public static final String HADOOP_UGI = "hadoop.job.ugi";

    public static final String HADOOP_USER = "user.name";

    /**
     * Initialize the workflow application service.
     *
     * @param services services instance.
     */
    public void init(Services services) {
    }

    /**
     * Destroy the workflow application service.
     */
    public void destroy() {
    }

    /**
     * Return the public interface for workflow application service.
     *
     * @return {@link WorkflowAppService}.
     */
    public Class<? extends Service> getInterface() {
        return WorkflowAppService.class;
    }

    /**
     * Read workflow definition.
     *
     * @param appPath  application path.
     * @param user     user name.
     * @param group    group name.
     * @param autToken authentication token.
     * @return workflow definition.
     * @throws WorkflowException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath, String user, String group, String autToken)
            throws WorkflowException {
        try {
            URI uri = new URI(appPath);
            HadoopAccessor ha = Services.get().get(HadoopAccessorService.class).get(user, group);
            FileSystem fs = ha.createFileSystem(uri, new Configuration());                        
            Reader reader = new InputStreamReader(fs.open(new Path(uri.getPath(), "workflow.xml")));
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();

        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, appPath, ex.getMessage(), ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
    }

    /**
     * Create proto configuration.
     * <p/>
     * The proto configuration includes the user,group and the paths which need
     * to be added to distributed cache. These paths include .jar,.so and the
     * resource file paths.
     *
     * @param jobConf   job configuration.
     * @param authToken authentication token.
     * @return proto configuration.
     * @throws WorkflowException thrown if the proto action configuration could not be created.
     */
    public XConfiguration createProtoActionConf(Configuration jobConf, String authToken) throws WorkflowException {
        XConfiguration conf = new XConfiguration();
        try {
            String user = jobConf.get(OozieClient.USER_NAME);
            String group = jobConf.get(OozieClient.GROUP_NAME);
            String hadoopUgi = user + "," + group;

            URI uri = new URI(jobConf.get(OozieClient.APP_PATH));

            HadoopAccessor ha = Services.get().get(HadoopAccessorService.class).get(user, group);
            FileSystem fs = ha.createFileSystem(uri, new Configuration());

            conf.set(OozieClient.USER_NAME, user);
            conf.set(OozieClient.GROUP_NAME, group);
            conf.set(HADOOP_UGI, hadoopUgi);

            Path appPath = new Path(uri.getPath());
            List<String> jarFilePaths = getLibPaths(fs, appPath, ".jar");
            List<String> soFilepaths = getLibPaths(fs, appPath, ".so");
            conf.setStrings(APP_LIB_JAR_PATH_LIST, jarFilePaths.toArray(new String[jarFilePaths.size()]));
            conf.setStrings(APP_LIB_SO_PATH_LIST, soFilepaths.toArray(new String[soFilepaths.size()]));
            return conf;

        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0712, jobConf.get(OozieClient.APP_PATH),
                                        ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, jobConf.get(OozieClient.APP_PATH),
                                        ex.getMessage(), ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0712, jobConf.get(OozieClient.APP_PATH),
                                        ex.getMessage(), ex);
        }
    }

    /**
     * Parse workflow definition.
     *
     * @param jobConf   job configuration.
     * @param authToken authentication token.
     * @return workflow application.
     * @throws WorkflowException thrown if the workflow application could not be parsed.
     */
    public abstract WorkflowApp parseDef(Configuration jobConf, String authToken) throws WorkflowException;

    /**
     * Get library paths for a given extension.
     *
     * @param fs        file system object.
     * @param appPath   hdfs application path.
     * @param extension to be listed.
     * @return list of paths.
     * @throws IOException thrown if the lib paths could not be obtained.
     */
    private List<String> getLibPaths(FileSystem fs, Path appPath, String extension) throws IOException {
        List<String> libPaths = new ArrayList<String>();
        FileStatus[] files =
                fs.listStatus(new Path(appPath + "/lib"), new LibPathFilter(appPath.toUri().getPath(), extension));
        for (FileStatus file : files) {
            libPaths.add((String) file.getPath().toUri().getPath().trim());
        }
        return libPaths;
    }

    /**
     * Filter for listing library paths.
     */
    private class LibPathFilter implements PathFilter {
        private String extension;
        private String libPath;

        /**
         * Creates library paths filter.
         *
         * @param appPath   workflow application path.
         * @param extension file extension to be listed.
         */
        public LibPathFilter(String appPath, String extension) {
            this.extension = extension;
            this.libPath = appPath + "/lib";
        }

        /**
         * Check the library path.
         *
         * @param path path to be checked
         * @return true if path has the extension and start with application
         *         /lib directory else false
         */
        @Override
        public boolean accept(Path path) {
            return path.getName().trim().endsWith(extension) && path.toUri().getPath().startsWith(libPath);
        }
    }

}