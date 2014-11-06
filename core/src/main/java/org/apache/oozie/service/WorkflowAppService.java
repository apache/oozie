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

package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.workflow.WorkflowApp;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.util.IOUtils;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service that provides application workflow definition reading from the path and creation of the proto configuration.
 */
public abstract class WorkflowAppService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "WorkflowAppService.";

    public static final String SYSTEM_LIB_PATH = CONF_PREFIX + "system.libpath";

    public static final String APP_LIB_PATH_LIST = "oozie.wf.application.lib";

    public static final String HADOOP_USER = "user.name";

    public static final String CONFG_MAX_WF_LENGTH = CONF_PREFIX + "WorkflowDefinitionMaxLength";

    public static final String OOZIE_SUBWORKFLOW_CLASSPATH_INHERITANCE = "oozie.subworkflow.classpath.inheritance";

    public static final String OOZIE_WF_SUBWORKFLOW_CLASSPATH_INHERITANCE = "oozie.wf.subworkflow.classpath.inheritance";

    private Path systemLibPath;
    private long maxWFLength;
    private boolean oozieSubWfCPInheritance;

    /**
     * Initialize the workflow application service.
     *
     * @param services services instance.
     */
    public void init(Services services) {
        Configuration conf = services.getConf();

        String path = ConfigurationService.get(conf, SYSTEM_LIB_PATH);
        if (path.trim().length() > 0) {
            systemLibPath = new Path(path.trim());
        }

        maxWFLength = conf.getInt(CONFG_MAX_WF_LENGTH, 100000);

        oozieSubWfCPInheritance = conf.getBoolean(OOZIE_SUBWORKFLOW_CLASSPATH_INHERITANCE, false);
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
     *
     * @param appPath application path.
     * @param user user name.
     * @return workflow definition.
     * @throws WorkflowException thrown if the definition could not be read.
     */
    protected String readDefinition(String appPath, String user, Configuration conf)
            throws WorkflowException {
        try {
            URI uri = new URI(appPath);
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            JobConf jobConf = has.createJobConf(uri.getAuthority());
            FileSystem fs = has.createFileSystem(user, uri, jobConf);

            // app path could be a directory
            Path path = new Path(uri.getPath());
            if (!fs.isFile(path)) {
                path = new Path(path, "workflow.xml");
            }

            FileStatus fsStatus = fs.getFileStatus(path);
            if (fsStatus.getLen() > this.maxWFLength) {
                throw new WorkflowException(ErrorCode.E0736, fsStatus.getLen(), this.maxWFLength);
            }

            Reader reader = new InputStreamReader(fs.open(path));
            StringWriter writer = new StringWriter();
            IOUtils.copyCharStream(reader, writer);
            return writer.toString();

        }
        catch (WorkflowException wfe) {
            throw wfe;
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, appPath, ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new WorkflowException(ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0710, ex.getMessage(), ex);
        }
    }
    /**
     * Create proto configuration. <p/> The proto configuration includes the user,group and the paths which need to be
     * added to distributed cache. These paths include .jar,.so and the resource file paths.
     *
     * @param jobConf job configuration.
     * @param isWorkflowJob indicates if the job is a workflow job or not.
     * @return proto configuration.
     * @throws WorkflowException thrown if the proto action configuration could not be created.
     */
    public XConfiguration createProtoActionConf(Configuration jobConf, boolean isWorkflowJob)
            throws WorkflowException {
        try {
            HadoopAccessorService has = Services.get().get(HadoopAccessorService.class);
            URI uri = new URI(jobConf.get(OozieClient.APP_PATH));

            Configuration conf = has.createJobConf(uri.getAuthority());
            XConfiguration protoConf = new XConfiguration();


            String user = jobConf.get(OozieClient.USER_NAME);
            conf.set(OozieClient.USER_NAME, user);
            protoConf.set(OozieClient.USER_NAME, user);

            FileSystem fs = has.createFileSystem(user, uri, conf);

            Path appPath = new Path(uri);
            XLog.getLog(getClass()).debug("jobConf.libPath = " + jobConf.get(OozieClient.LIBPATH));
            XLog.getLog(getClass()).debug("jobConf.appPath = " + appPath);

            Collection<String> filePaths;
            if (isWorkflowJob) {
                // app path could be a directory
                Path path = new Path(uri.getPath());
                if (!fs.isFile(path)) {
                    filePaths = getLibFiles(fs, new Path(appPath + "/lib"));
                } else {
                    filePaths = getLibFiles(fs, new Path(appPath.getParent(), "lib"));
                }
            }
            else {
                filePaths = new LinkedHashSet<String>();
            }

            String[] libPaths = jobConf.getStrings(OozieClient.LIBPATH);
            if (libPaths != null && libPaths.length > 0) {
                for (int i = 0; i < libPaths.length; i++) {
                    if (libPaths[i].trim().length() > 0) {
                        Path libPath = new Path(libPaths[i].trim());
                        Collection<String> libFilePaths = getLibFiles(fs, libPath);
                        filePaths.addAll(libFilePaths);
                    }
                }
            }

            // Check if a subworkflow should inherit the libs from the parent WF
            // OOZIE_WF_SUBWORKFLOW_CLASSPATH_INHERITANCE has priority over OOZIE_SUBWORKFLOW_CLASSPATH_INHERITANCE from oozie-site
            // If OOZIE_WF_SUBWORKFLOW_CLASSPATH_INHERITANCE isn't specified, we use OOZIE_SUBWORKFLOW_CLASSPATH_INHERITANCE
            if (jobConf.getBoolean(OOZIE_WF_SUBWORKFLOW_CLASSPATH_INHERITANCE, oozieSubWfCPInheritance)) {
                // Keep any libs from a parent workflow that might already be in APP_LIB_PATH_LIST and also remove duplicates
                String[] parentFilePaths = jobConf.getStrings(APP_LIB_PATH_LIST);
                if (parentFilePaths != null && parentFilePaths.length > 0) {
                    String[] filePathsNames = filePaths.toArray(new String[filePaths.size()]);
                    for (int i = 0; i < filePathsNames.length; i++) {
                        Path p = new Path(filePathsNames[i]);
                        filePathsNames[i] = p.getName();
                    }
                    Arrays.sort(filePathsNames);
                    List<String> nonDuplicateParentFilePaths = new ArrayList<String>();
                    for (String parentFilePath : parentFilePaths) {
                        Path p = new Path(parentFilePath);
                        if (Arrays.binarySearch(filePathsNames, p.getName()) < 0) {
                            nonDuplicateParentFilePaths.add(parentFilePath);
                        }
                    }
                    filePaths.addAll(nonDuplicateParentFilePaths);
                }
            }

            protoConf.setStrings(APP_LIB_PATH_LIST, filePaths.toArray(new String[filePaths.size()]));

            //Add all properties start with 'oozie.'
            for (Map.Entry<String, String> entry : jobConf) {
                if (entry.getKey().startsWith("oozie.")) {
                    String name = entry.getKey();
                    String value = entry.getValue();
                    // if property already exists, should not overwrite
                    if(protoConf.get(name) == null) {
                        protoConf.set(name, value);
                    }
                }
            }
            return protoConf;
        }
        catch (IOException ex) {
            throw new WorkflowException(ErrorCode.E0712, jobConf.get(OozieClient.APP_PATH), ex.getMessage(), ex);
        }
        catch (URISyntaxException ex) {
            throw new WorkflowException(ErrorCode.E0711, jobConf.get(OozieClient.APP_PATH), ex.getMessage(), ex);
        }
        catch (HadoopAccessorException ex) {
            throw new WorkflowException(ex);
        }
        catch (Exception ex) {
            throw new WorkflowException(ErrorCode.E0712, jobConf.get(OozieClient.APP_PATH),
                                        ex.getMessage(), ex);
        }
    }

    /**
     * Parse workflow definition.
     *
     * @param jobConf
     * @return
     * @throws WorkflowException
     */
    public abstract WorkflowApp parseDef(Configuration jobConf) throws WorkflowException;

    /**
     * Parse workflow definition along with config-default.xml config
     *
     * @param jobConf job configuration
     * @param configDefault config from config-default.xml
     * @return workflow application thrown if the workflow application could not
     *         be parsed
     * @throws WorkflowException
     */
    public abstract WorkflowApp parseDef(Configuration jobConf, Configuration configDefault) throws WorkflowException;

    /**
     * Parse workflow definition.
     * @param wfXml workflow.
     * @param jobConf job configuration
     * @return workflow application.
     * @throws WorkflowException thrown if the workflow application could not be parsed.
     */
    public abstract WorkflowApp parseDef(String wfXml, Configuration jobConf) throws WorkflowException;

    /**
     * Get all library paths.
     *
     * @param fs file system object.
     * @param libPath hdfs library path.
     * @return list of paths.
     * @throws IOException thrown if the lib paths could not be obtained.
     */
    private Collection<String> getLibFiles(FileSystem fs, Path libPath) throws IOException {
        Set<String> libPaths = new LinkedHashSet<String>();
        if (fs.exists(libPath)) {
            FileStatus[] files = fs.listStatus(libPath, new NoPathFilter());

            for (FileStatus file : files) {
                libPaths.add(file.getPath().toUri().toString());
            }
        }
        else {
            XLog.getLog(getClass()).warn("libpath [{0}] does not exist", libPath);
        }
        return libPaths;
    }

    /*
     * Filter class doing no filtering.
     * We dont need define this class, but seems fs.listStatus() is not working properly without this.
     * So providing this dummy no filtering Filter class.
     */
    private class NoPathFilter implements PathFilter {
        @Override
        public boolean accept(Path path) {
            return true;
        }
    }

    /**
     * Returns Oozie system libpath.
     *
     * @return Oozie system libpath (sharelib) in HDFS if present, otherwise it returns <code>NULL</code>.
     */
    public Path getSystemLibPath() {
        return systemLibPath;
    }
}
