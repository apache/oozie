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

package org.apache.oozie.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.Properties;

import com.google.common.base.Charsets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FilenameUtils;
import org.apache.oozie.cli.OozieCLI;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class XOozieClient extends OozieClient {
    public static final String RM = "yarn.resourcemanager.address";
    public static final String NN = "fs.default.name";
    public static final String NN_2 = "fs.defaultFS";

    public static final String PIG_SCRIPT = "oozie.pig.script";

    public static final String PIG_OPTIONS = "oozie.pig.options";

    public static final String PIG_SCRIPT_PARAMS = "oozie.pig.script.params";

    public static final String HIVE_SCRIPT = "oozie.hive.script";

    public static final String HIVE_OPTIONS = "oozie.hive.options";

    public static final String HIVE_SCRIPT_PARAMS = "oozie.hive.script.params";

    public static final String SQOOP_COMMAND = "oozie.sqoop.command";

    public static final String SQOOP_OPTIONS = "oozie.sqoop.options";

    public static final String FILES = "oozie.files";

    public static final String ARCHIVES = "oozie.archives";

    public static final String IS_PROXY_SUBMISSION = "oozie.proxysubmission";

    protected XOozieClient() {
    }

    /**
     * Create an eXtended Workflow client instance.
     *
     * @param oozieUrl URL of the Oozie instance it will interact with.
     */
    public XOozieClient(String oozieUrl) {
        super(oozieUrl);
    }

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "FilenameUtils is used to filter user input. JDK8+ is used.")
    private String readScript(String script) throws IOException {
        if (!new File(script).exists()) {
            throw new IOException("Error: script file [" + script + "] does not exist");
        }

        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(
                    FilenameUtils.getFullPath(script) + FilenameUtils.getName(script)), Charsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        }
        finally {
            try {
                br.close();
            }
            catch (IOException ex) {
                System.err.println("Error: " + ex.getMessage());
            }
        }
    }

    private String serializeSqoopCommand(String[] command) {
        StringBuilder sb = new StringBuilder();
        for (String arg : command) {
            sb.append(arg).append("\n");
        }
        return sb.toString();
    }

    static void setStrings(Properties conf, String key, String[] values) {
        if (values != null) {
            conf.setProperty(key + ".size", (new Integer(values.length)).toString());
            for (int i = 0; i < values.length; i++) {
                conf.setProperty(key + "." + i, values[i]);
            }
        }
    }

    private void validateHttpSubmitConf(Properties conf) {
        String RM = conf.getProperty(XOozieClient.RM);
        if (RM == null) {
            throw new RuntimeException("Resource manager is not specified in conf");
        }

        String NN = conf.getProperty(XOozieClient.NN);
        String NN_2 = conf.getProperty(XOozieClient.NN_2);
        if (NN == null) {
            if(NN_2 == null) {
                throw new RuntimeException("namenode is not specified in conf");
            } else {
                NN = NN_2;
            }
        }

        String libPath = conf.getProperty(LIBPATH);
        if (libPath == null) {
            throw new RuntimeException("libpath is not specified in conf");
        }
        if (!libPath.contains(":/")) {
            String newLibPath;
            if (libPath.startsWith("/")) {
                if(NN.endsWith("/")) {
                    newLibPath = NN + libPath.substring(1);
                } else {
                    newLibPath = NN + libPath;
                }
            } else {
                throw new RuntimeException("libpath should be absolute");
            }
            conf.setProperty(LIBPATH, newLibPath);
        }

        conf.setProperty(IS_PROXY_SUBMISSION, "true");
    }

    /**
     * Submit a Pig job via HTTP.
     *
     * @param conf job configuration.
     * @param pigScriptFile pig script file.
     * @param pigArgs pig arguments string.
     * @return the job Id.
     * @throws java.io.IOException thrown if there is a problem with pig script file.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    @Deprecated
    public String submitPig(Properties conf, String pigScriptFile, String[] pigArgs) throws IOException, OozieClientException {
        return submitScriptLanguage(conf, pigScriptFile, pigArgs, OozieCLI.PIG_CMD);
    }

    /**
     * Submit a Pig or Hive job via HTTP.
     *
     * @param conf job configuration.
     * @param scriptFile  script file.
     * @param args  arguments string.
     * @param jobType job type.
     * @return the job Id.
     * @throws java.io.IOException thrown if there is a problem with script file.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    public String submitScriptLanguage(Properties conf, String scriptFile, String[] args, String jobType)
            throws IOException, OozieClientException {
        return submitScriptLanguage(conf, scriptFile, args, null, jobType);
    }

    /**
     * Submit a Pig or Hive job via HTTP.
     *
     * @param conf job configuration.
     * @param scriptFile  script file.
     * @param args  arguments string.
     * @param params parameters string.
     * @param jobType job type.
     * @return the job Id.
     * @throws java.io.IOException thrown if there is a problem with file.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    public String submitScriptLanguage(Properties conf, String scriptFile, String[] args, String[] params,
        String jobType)
            throws IOException, OozieClientException {
        OozieClient.notNull(conf, "conf");
        OozieClient.notNull(scriptFile, "scriptFile");
        validateHttpSubmitConf(conf);

        String script;
        String options;
        String scriptParams;

        switch (jobType) {
            case OozieCLI.HIVE_CMD:
                script = XOozieClient.HIVE_SCRIPT;
                options = XOozieClient.HIVE_OPTIONS;
                scriptParams = XOozieClient.HIVE_SCRIPT_PARAMS;
                break;
            case OozieCLI.PIG_CMD:
                script = XOozieClient.PIG_SCRIPT;
                options = XOozieClient.PIG_OPTIONS;
                scriptParams = XOozieClient.PIG_SCRIPT_PARAMS;
                break;
            default:
                throw new IllegalArgumentException("jobType must be either pig or hive");
        }

        conf.setProperty(script, readScript(scriptFile));
        setStrings(conf, options, args);
        setStrings(conf, scriptParams, params);

        return (new HttpJobSubmit(conf, jobType)).call();
    }

    /**
     * Submit a Sqoop job via HTTP.
     *
     * @param conf job configuration.
     * @param command sqoop command to run.
     * @param args  arguments string.
     * @return the job Id.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    public String submitSqoop(Properties conf, String[] command, String[] args)
            throws OozieClientException {
        OozieClient.notNull(conf, "conf");
        OozieClient.notNull(command, "command");
        validateHttpSubmitConf(conf);

        conf.setProperty(XOozieClient.SQOOP_COMMAND, serializeSqoopCommand(command));
        setStrings(conf, XOozieClient.SQOOP_OPTIONS, args);

        return (new HttpJobSubmit(conf, OozieCLI.SQOOP_CMD)).call();
    }

    /**
     * Submit a Map/Reduce job via HTTP.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws OozieClientException thrown if the job could not be submitted.
     */
    public String submitMapReduce(Properties conf) throws OozieClientException {
        OozieClient.notNull(conf, "conf");
        validateHttpSubmitConf(conf);

        return (new HttpJobSubmit(conf, "mapreduce")).call();
    }

    private class HttpJobSubmit extends ClientCallable<String> {
        private Properties conf;

        HttpJobSubmit(Properties conf, String jobType) {
            super("POST", RestConstants.JOBS, "", prepareParams(RestConstants.JOBTYPE_PARAM, jobType));
            this.conf = notNull(conf, "conf");
        }

        @Override
        protected String call(HttpURLConnection conn) throws IOException, OozieClientException {
            conn.setRequestProperty("content-type", RestConstants.XML_CONTENT_TYPE);
            writeToXml(conf, conn.getOutputStream());
            if (conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
                JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream(), Charsets.UTF_8));
                return (String) json.get(JsonTags.JOB_ID);
            }
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                handleError(conn);
            }
            return null;
        }
    }

    /**
     * set LIBPATH for HTTP submission job.
     *
     * @param conf Configuration object.
     * @param pathStr lib HDFS path.
     */
    public void setLib(Properties conf, String pathStr) {
        conf.setProperty(LIBPATH, pathStr);
    }

    /**
     * The equivalent to &lt;file&gt; tag in oozie's workflow xml.
     *
     * @param conf Configuration object.
     * @param file file HDFS path. A "#..." symbolic string can be appended to the path to specify symbolic link name.
     *             For example, "/user/oozie/parameter_file#myparams". If no "#..." is specified, file name will be used as
     *             symbolic link name.
     */
    public void addFile(Properties conf, String file) {
        OozieClient.notEmpty(file, "file");
        String files = conf.getProperty(FILES);
        conf.setProperty(FILES, files == null ? file : files + "," + file);
    }

    /**
     * The equivalent to &lt;archive&gt; tag in oozie's workflow xml.
     *
     * @param conf Configuration object.
     * @param file file HDFS path. A "#..." symbolic string can be appended to the path to specify symbolic link name.
     *             For example, "/user/oozie/udf1.jar#my.jar". If no "#..." is specified, file name will be used as
     *             symbolic link name.
     */
    public void addArchive(Properties conf, String file) {
        OozieClient.notEmpty(file, "file");
        String files = conf.getProperty(ARCHIVES);
        conf.setProperty(ARCHIVES, files == null ? file : files + "," + file);
    }
}
